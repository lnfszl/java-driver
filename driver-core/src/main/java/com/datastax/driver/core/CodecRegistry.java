/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Set;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.UncheckedExecutionException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.TypeCodec.*;
import com.datastax.driver.core.exceptions.CodecNotFoundException;

/**
 * A registry for {@link TypeCodec}s.
 */
@SuppressWarnings("unchecked")
public class CodecRegistry {

    private static final ImmutableSet<TypeCodec<?>> PRIMITIVE_CODECS = ImmutableSet.of(
        BlobCodec.instance,
        BooleanCodec.instance,
        IntCodec.instance,
        BigintCodec.instance,
        CounterCodec.instance,
        DoubleCodec.instance,
        FloatCodec.instance,
        BigIntegerCodec.instance,
        DecimalCodec.instance,
        TextCodec.instance,
        VarcharCodec.instance,
        AsciiCodec.instance,
        TimestampCodec.instance,
        UUIDCodec.instance,
        TimeUUIDCodec.instance,
        InetCodec.instance
    );

    /**
     * The set of all codecs to handle
     * the default mappings for
     * - Native types
     * - Lists of native types
     * - Sets of native types
     * - Maps of native types
     * - Custom CQL types
     *
     * Note that ther is no support for custom CQL types
     * in the default set of codecs.
     * Codecs for custom CQL types MUST be
     * registered manually.
     */
    private static final ImmutableSet<TypeCodec<?>> DEFAULT_CODECS;

    static {
        ImmutableSet.Builder<TypeCodec<?>> builder = new ImmutableSet.Builder<TypeCodec<?>>();
        builder.addAll(PRIMITIVE_CODECS);
        for (TypeCodec<?> primitiveCodec1 : PRIMITIVE_CODECS) {
            builder.add(new ListCodec(primitiveCodec1));
            builder.add(new SetCodec(primitiveCodec1));
            for (TypeCodec<?> primitiveCodec2 : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec1, primitiveCodec2));
            }
        }
        DEFAULT_CODECS = builder.build();
    }

    public static final CodecRegistry DEFAULT_INSTANCE = new CodecRegistry(DEFAULT_CODECS);

    public static class Builder {

        ImmutableSet.Builder<TypeCodec<?>> builder = new ImmutableSet.Builder<TypeCodec<?>>();

        /**
         * Add all 238 default TypeCodecs to the registry.
         *
         * @return this
         */
        public Builder withDefaultCodecs() {
            builder.addAll(DEFAULT_CODECS);
            return this;
        }

        /**
         * TypeCodec instances that have a Java type already
         * present in the set of codec will override
         * the previous one.
         *
         * @param codecs The codecs to add to the registry
         * @return this
         */
        public Builder withCodecs(TypeCodec<?>... codecs) {
            for (TypeCodec<?> codec : codecs) {
                builder.add(codec);
            }
            return this;
        }


        /**
         * Adds required codecs to handle the given custom CQL type.
         *
         * @param customType The custom CQL type to add to the registry
         * @return this
         */
        public Builder withCustomType(DataType customType) {
            CustomCodec customCodec = new CustomCodec(customType);
            builder.add(customCodec);
            builder.add(new ListCodec<ByteBuffer>(customCodec));
            builder.add(new SetCodec<ByteBuffer>(customCodec));
            for (TypeCodec<?> primitiveCodec : PRIMITIVE_CODECS) {
                builder.add(new MapCodec(primitiveCodec, customCodec));
                builder.add(new MapCodec(customCodec, primitiveCodec));
            }
            return this;
        }
        public CodecRegistry build() {
            return new CodecRegistry(builder.build());
        }
    }

    public static CodecRegistry.Builder builder() {
        return new Builder();
    }

    private static final class Key {

        private final TypeToken<?> javaType;

        private final DataType cqlType;

        private Key(TypeToken<?> javaType, DataType cqlType) {
            this.javaType = javaType;
            this.cqlType = cqlType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Key))
                return false;
            Key key = (Key)o;
            if (javaType != null ? !javaType.equals(key.javaType) : key.javaType != null)
                return false;
            return !(cqlType != null ? !cqlType.equals(key.cqlType) : key.cqlType != null);
        }

        @Override
        public int hashCode() {
            int result = javaType != null ? javaType.hashCode() : 0;
            result = 31 * result + (cqlType != null ? cqlType.hashCode() : 0);
            return result;
        }
    }

    private final ImmutableSet<TypeCodec<?>> codecs;

    private final LoadingCache<Key, TypeCodec<?>> cache;

    private CodecRegistry(ImmutableSet<TypeCodec<?>> codecs) {
        this.codecs = codecs;
        this.cache = CacheBuilder.newBuilder()
            .initialCapacity(400)
            .build(
                new CacheLoader<Key, TypeCodec<?>>() {
                    public TypeCodec<?> load(Key key) {
                        return lookupCodec(key);
                    }
                });
    }

    /**
     * Called
     * - When binding collection values, and the Java type of the collection element is not known
     * - When the Java type is irrelevant, such as in toString() methods
     *
     * @param cqlType The CQL type the codec should deserialize from and serialize to
     * @param <T> The Java type the codec can serialize from and deserialize to
     * @return A suitable codec
     * @throws CodecNotFoundException if a suitable codec cannot be found
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType) throws CodecNotFoundException {
        checkNotNull(cqlType);
        return codecFor(cqlType, (TypeToken)null);
    }

    /**
     * Called from GettableData instances when
     * calling methods getX, where both CQL type and Java type
     * are imposed by the method contract.
     * <p>
     * One out of two arguments can be {@code null}, but not both.
     * When one argument is {@code null}, it is assumed that its meaning is "ANY",
     * e.g. <code>codecFor(null, String.class)</code> would
     * return the first codec that deserializes from any CQL type
     * to a Java String.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec's Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even
     * if another codec as a better or exact match for it</em>.
     *
     * @param cqlType The CQL type the codec should deserialize from and serialize to
     * @param javaType The Java class the codec can serialize from and deserialize to
     * @param <T> The Java type the codec can serialize from and deserialize to
     * @return A suitable codec
     * @throws CodecNotFoundException if a suitable codec cannot be found
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, Class<T> javaType) throws CodecNotFoundException {
        return codecFor(cqlType, TypeToken.of(javaType));
    }

    /**
     * Called from GettableData instances when
     * calling methods getX, where both CQL type and Java type
     * are imposed by the method contract, and the Java
     * type is a parameterized type.
     * <p>
     * One out of two arguments can be {@code null}, but not both.
     * When one argument is {@code null}, it is assumed that its meaning is "ANY",
     * e.g. <code>codecFor(null, TypeToken.of(String.class))</code> would
     * return the first codec that deserializes from any CQL type
     * to a Java String.
     * <p>
     * Note that type inheritance needs special care.
     * If a codec's Java type that is assignable to the
     * given Java type, that codec may be returned if it is found first
     * in the registry, <em>even
     * if another codec as a better or exact match for it</em>.
     *
     * @param cqlType The CQL type the codec should deserialize from and serialize to.
     * @param javaType The {@link TypeToken} encapsulating the Java type the codec can serialize from and deserialize to.
     * @param <T> The Java type the codec can serialize from and deserialize to.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(DataType cqlType, TypeToken<T> javaType) throws CodecNotFoundException {
        checkArgument(cqlType != null || javaType != null);
        Key key = new Key(javaType, cqlType);
        try {
            return (TypeCodec<T>)cache.getUnchecked(key);
        } catch (UncheckedExecutionException e) {
            throw (CodecNotFoundException) e.getCause();
        }
    }

    /**
     * Called when binding a value whose CQL type and Java type are both unknown.
     * Happens specially when creating SimpleStatement instances,
     * when binding BoundStatement values, and in the QueryBuilder.
     * <p>
     * Note that, due to type erasure, this method works on a best-effort basis
     * and might not return the most appropriate codec,
     * specially for generic types such as collections.
     *
     * @param value The value we are looking a codec for; must not be {@code null}.
     * @param <T> The Java type the codec can serialize from and deserialize to.
     * @return A suitable codec.
     * @throws CodecNotFoundException if a suitable codec cannot be found.
     */
    public <T> TypeCodec<T> codecFor(T value) {
        checkNotNull(value);
        for (TypeCodec<?> codec : codecs) {
            if (codec.canSerialize(value)) {
                return (TypeCodec<T>)codec;
            }
        }
        throw new CodecNotFoundException(
            String.format("Codec not found for requested value %s",
                value), null, TypeToken.of(value.getClass()));
    }

    private <T> TypeCodec<T> lookupCodec(Key key) {
        for (TypeCodec<?> codec : codecs) {
            if (codec.canSerDe(key.cqlType, key.javaType)) {
                return (TypeCodec<T>)codec;
            }
        }
        throw new CodecNotFoundException(
            String.format("Codec not found for requested pair: Java type %s <-> CQL type %s",
                key.javaType == null ? "ANY" : key.javaType,
                key.cqlType == null ? "ANY" : key.cqlType),
            key.cqlType, key.javaType);
    }

}
