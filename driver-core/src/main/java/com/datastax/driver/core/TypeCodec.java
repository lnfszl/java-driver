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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.datastax.driver.core.DataType.*;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

/**
 * A Codec that can deserialize one or more CQL types into one Java Type,
 * and vice versa.
 * <p>
 * Codecs have main methods:
 *
 * <ol>
 *     <li>{@link #serialize(Object)}: used to serialize from the Java type handled by the codec to a
 *     {@link ByteBuffer} instance with the expected raw format of the corresponding CQL type;</li>
 *     <li>{@link #deserialize(ByteBuffer)}}: used to deserialize a {@link ByteBuffer} instance
 *     with the expected raw format of the corresponding CQL type to the Java type handled by the codec</li>
 *     <li>{@link #format(Object)}: returns CQL String representation of the Java type handled by the codec,
 *     according to the syntax of the CQL type handled by the codec;</li>
 *     <li>{@link #parse(String)}; parses a CQL String into the Java type handled by the codec.</li>
 * </ol>
 *
 * Codecs also have inspection methods:
 *
 * <ol>
 *     <li>{@link #serialize(Object)}: used to serialize from the Java type handled by the codec to a
 *     {@link ByteBuffer} instance with the expected raw format of the corresponding CQL type;</li>
 *     <li>{@link #deserialize(ByteBuffer)}}: used to deserialize a {@link ByteBuffer} instance
 *     with the expected raw format of the corresponding CQL type to the Java type handled by the codec</li>
 *     <li>{@link #format(Object)}: returns CQL String representation of the Java type handled by the codec,
 *     according to the syntax of the CQL type handled by the codec;</li>
 *     <li>{@link #parse(String)}; parses a CQL String into the Java type handled by the codec.</li>
 * </ol>
 *
 * @param <T>
 */
public abstract class TypeCodec<T> {

    protected final TypeToken<T> javaType;
    
    protected final DataType cqlType;

    /**
     * This constructor can only be used for non parameterized types.
     * For parameterized ones, pelase use {@link #TypeCodec(DataType, TypeToken)} instead.
     *
     * @param javaClass The Java class this codec serializes from and deserializes to.
     */
    protected TypeCodec(DataType cqlType, Class<T> javaClass) {
        this(cqlType, TypeToken.of(javaClass));
    }

    protected TypeCodec(DataType cqlType, TypeToken<T> javaType) {
        this.cqlType = cqlType;
        this.javaType = javaType;
    }

    /**
     * Return the Javaa type that this codec deserializes to and serializes from.
     * @return The Java type this codec deserializes to and serializes from.
     */
    public TypeToken<T> getJavaType() {
        return javaType;
    }

    /**
     * Serialize the given value according to the CQL type
     * handled by this codec.
     * @param value An instance of T
     * @return A {@link ByteBuffer} instance containing the serialized form of T
     */
    public abstract ByteBuffer serialize(T value);

    /**
     * Serialize the given value according to the CQL type
     * handled by this codec.
     * @param bytes A {@link ByteBuffer} instance containing the serialized form of T
     * @return An instance of T
     */
    public abstract T deserialize(ByteBuffer bytes);

    /**
     * Parse the given CQL string into an instance of the Java type
     * handled by this codec.
     * @param value CQL string
     * @return An instance of T
     */
    public abstract T parse(String value);

    /**
     * Format the given value as a valid CQL string according
     * to the CQL type handled by this codec.
     * @param value An instance of T
     * @return CQL string
     */
    public abstract String format(T value);

    /**
     * Return true if this codec is capable of deserializing
     * the given {@code cqlType} into the given {@code javaType},
     * and vice versa (bidirecitional serialization).
     * <p>
     * One out of two arguments can be {@code null}, but not both.
     * When one argument is {@code null}, it is assumed that its meaning
     * is "ANY", e.g. <code>canSerDe(null, TypeToken.of(String.class))</code>
     * would return {@code true} if the codec deserializes from any CQL type
     * to a Java String.
     *
     * @param cqlType The CQL type this codec should deserialize from and serialize to, or {@code null} if it is irrelevant
     * @param javaType The Java type this codec should serialize from and deserialize to, or {@code null} if it is irrelevant
     * @return {@code true} if the codec is capable of deserializing
     * the given {@code cqlType} into the given {@code javaType},
     * and vice versa, and {@code false} otherwise.
     */
    public final boolean canSerDe(DataType cqlType, TypeToken javaType) {
        return (cqlType == null || canDeserialize(cqlType)) && (javaType == null || canSerialize(javaType));
    }

    /**
     * Return {@code true} if this codec is capable of serializing
     * the given {@code javaType}.
     *
     * @param javaType The Java type this codec should serialize from and deserialize to; cannot be {@code null}.
     * @return {@code true} if the codec is capable of serializing
     * the given {@code javaType}, and {@code false} otherwise.
     */
    public final boolean canSerialize(TypeToken javaType) {
        checkNotNull(javaType);
        return this.javaType.isAssignableFrom(javaType);
    }

    /**
     * Return {@code true} if this codec is capable of deserializing
     * the given {@code cqlType}.
     *
     * @param cqlType The CQL type this codec should deserialize from and serialize to; cannot be {@code null}.
     * @return {@code true} if the codec is capable of deserializing
     * the given {@code cqlType}, and {@code false} otherwise.
     */
    protected final boolean canDeserialize(DataType cqlType) {
        checkNotNull(cqlType);
        return this.cqlType.equals(cqlType);
    }

    /**
     * Return {@code true} if this codec is capable of serializing
     * the given object. Note that the object's Java type is inferred
     * from the object' runtime (raw) type, contrary
     * to {@link #canSerialize(TypeToken)} which is capable of
     * handling generic types.
     * <p>
     * This method is intended mostly to be used by the QueryBuilder
     * when no type information is available when the codec is used.
     * <p>
     * Implementation notes:
     *
     * <ol>
     * <li>The implementation provided here can only handle non-parameterized types;
     * codecs handling parameterized types, such as collection types, must override
     * this method and perform some sort of "manual"
     * inspection of the actual type parameters, because
     * this information is lost at runtime by type erasure and cannot
     * be retrieved from a simple Object parameter.</li>
     * </ol>
     *
     * @param value The Java type this codec should serialize from and deserialize to; cannot be {@code null}.
     * @return {@code true} if the codec is capable of serializing
     * the given {@code javaType}, and {@code false} otherwise.
     */
    public boolean canSerialize(Object value) {
        checkNotNull(value);
        return canSerialize(TypeToken.of(value.getClass()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        TypeCodec<?> typeCodec = (TypeCodec<?>)o;

        if (!javaType.equals(typeCodec.javaType))
            return false;
        return cqlType.equals(typeCodec.cqlType);

    }

    @Override
    public int hashCode() {
        int result = javaType.hashCode();
        result = 31 * result + cqlType.hashCode();
        return result;
    }

    public static abstract class StringCodec extends TypeCodec<String> {
        
        private final Charset charset;

        public StringCodec(DataType cqlType, Charset charset) {
            super(cqlType, String.class);
            this.charset = charset;
        }

        @Override
        public String parse(String value) {
            return value;
        }

        @Override
        public String format(String value) {
            return '\'' + replace(value, '\'', "''") + '\'';
        }

        // Simple method to replace a single character. String.replace is a bit too
        // inefficient (see JAVA-67)
        public static String replace(String text, char search, String replacement) {
            if (text == null || text.isEmpty())
                return text;

            int nbMatch = 0;
            int start = -1;
            do {
                start = text.indexOf(search, start+1);
                if (start != -1)
                    ++nbMatch;
            } while (start != -1);

            if (nbMatch == 0)
                return text;

            int newLength = text.length() + nbMatch * (replacement.length() - 1);
            char[] result = new char[newLength];
            int newIdx = 0;
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c == search) {
                    for (int r = 0; r < replacement.length(); r++)
                        result[newIdx++] = replacement.charAt(r);
                } else {
                    result[newIdx++] = c;
                }
            }
            return new String(result);
        }

        @Override
        public ByteBuffer serialize(String value) {
            return ByteBuffer.wrap(value.getBytes(charset));
        }

        @Override
        public String deserialize(ByteBuffer bytes) {
            return new String(Bytes.getArray(bytes), charset);
        }
    }

    public static class TextCodec extends StringCodec {

        public static final TextCodec instance = new TextCodec();

        private TextCodec() {
            super(text(), Charset.forName("UTF-8"));
        }

    }

    public static class VarcharCodec extends StringCodec {

        public static final VarcharCodec instance = new VarcharCodec();

        private VarcharCodec() {
            super(varchar(), Charset.forName("UTF-8"));
        }

    }

    public static class AsciiCodec extends StringCodec {

        public static final AsciiCodec instance = new AsciiCodec();

        private AsciiCodec() {
            super(ascii(), Charset.forName("US-ASCII"));
        }

    }

    public abstract static class LongCodec extends TypeCodec<Long> {

        public LongCodec(DataType cqlType) {
            super(cqlType,  Long.class);
        }

        @Override
        public Long parse(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits long value from \"%s\"", value));
            }
        }

        @Override
        public String format(Long value) {
            return Long.toString(value);
        }

        @Override
        public ByteBuffer serialize(Long value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(long value) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(0, value);
            return bb;
        }

        @Override
        public Long deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public long deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 8)
                throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getLong(bytes.position());
        }
    }


    public static class BigintCodec extends LongCodec {

        public static final BigintCodec instance = new BigintCodec();

        private BigintCodec() {
            super(bigint());
        }

    }

    public static class CounterCodec extends LongCodec {

        public static final CounterCodec instance = new CounterCodec();

        private CounterCodec() {
            super(counter());
        }

    }

    public static class BlobCodec extends TypeCodec<ByteBuffer> {

        public static final BlobCodec instance = new BlobCodec();

        private BlobCodec() {
            super(blob(), ByteBuffer.class);
        }

        @Override
        public ByteBuffer parse(String value) {
            return Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value) {
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes) {
            return bytes.duplicate();
        }
    }

    public static class CustomCodec extends TypeCodec<ByteBuffer> {

        public CustomCodec(DataType custom) {
            super(custom, ByteBuffer.class);
            assert custom.getName() == Name.CUSTOM;
        }

        @Override
        public ByteBuffer parse(String value) {
            return Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value) {
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes) {
            return bytes.duplicate();
        }
    }

    public static class BooleanCodec extends TypeCodec<Boolean> {

        private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[] {1});
        private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[] {0});

        public static final BooleanCodec instance = new BooleanCodec();

        private BooleanCodec() {
            super(cboolean(), Boolean.class);
        }

        @Override
        public Boolean parse(String value) {
            if (value.equalsIgnoreCase(Boolean.FALSE.toString()))
                return false;
            if (value.equalsIgnoreCase(Boolean.TRUE.toString()))
                return true;

            throw new InvalidTypeException(String.format("Cannot parse boolean value from \"%s\"", value));
        }

        @Override
        public String format(Boolean value) {
            return value ? "true" : "false";
        }

        @Override
        public ByteBuffer serialize(Boolean value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(boolean value) {
            return value ? TRUE.duplicate() : FALSE.duplicate();
        }

        @Override
        public Boolean deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public boolean deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 1)
                throw new InvalidTypeException("Invalid boolean value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position()) != 0;
        }
    }

    public static class DecimalCodec extends TypeCodec<BigDecimal> {

        public static final DecimalCodec instance = new DecimalCodec();

        private DecimalCodec() {
            super(decimal(), BigDecimal.class);
        }

        @Override
        public BigDecimal parse(String value) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse decimal value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigDecimal value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigDecimal value) {
            BigInteger bi = value.unscaledValue();
            int scale = value.scale();
            byte[] bibytes = bi.toByteArray();

            ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
            bytes.putInt(scale);
            bytes.put(bibytes);
            bytes.rewind();
            return bytes;
        }

        @Override
        public BigDecimal deserialize(ByteBuffer bytes) {
            if (bytes.remaining() < 4)
                throw new InvalidTypeException("Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());

            bytes = bytes.duplicate();
            int scale = bytes.getInt();
            byte[] bibytes = new byte[bytes.remaining()];
            bytes.get(bibytes);

            BigInteger bi = new BigInteger(bibytes);
            return new BigDecimal(bi, scale);
        }
    }

    public static class DoubleCodec extends TypeCodec<Double> {

        public static final DoubleCodec instance = new DoubleCodec();

        private DoubleCodec() {
            super(cdouble(), Double.class);
        }

        @Override
        public Double parse(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits double value from \"%s\"", value));
            }
        }

        @Override
        public String format(Double value) {
            return Double.toString(value);
        }

        @Override
        public ByteBuffer serialize(Double value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(double value) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putDouble(0, value);
            return bb;
        }

        @Override
        public Double deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public double deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 8)
                throw new InvalidTypeException("Invalid 64-bits double value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getDouble(bytes.position());
        }
    }

    public static class FloatCodec extends TypeCodec<Float> {

        public static final FloatCodec instance = new FloatCodec();

        private FloatCodec() {
            super(cfloat(), Float.class);
        }

        @Override
        public Float parse(String value) {
            try {
                return Float.parseFloat(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits float value from \"%s\"", value));
            }
        }

        @Override
        public String format(Float value) {
            return Float.toString(value);
        }

        @Override
        public ByteBuffer serialize(Float value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(float value) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putFloat(0, value);
            return bb;
        }

        @Override
        public Float deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public float deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 4)
                throw new InvalidTypeException("Invalid 32-bits float value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getFloat(bytes.position());
        }
    }

    public static class InetCodec extends TypeCodec<InetAddress> {

        public static final InetCodec instance = new InetCodec();

        private InetCodec() {
            super(inet(), InetAddress.class);
        }

        @Override
        public InetAddress parse(String value) {
            try {
                return InetAddress.getByName(value);
            } catch (Exception e) {
                throw new InvalidTypeException(String.format("Cannot parse inet value from \"%s\"", value));
            }
        }

        @Override
        public String format(InetAddress value) {
            return "'" + value.getHostAddress() + "'";
        }

        @Override
        public ByteBuffer serialize(InetAddress value) {
            return ByteBuffer.wrap(value.getAddress());
        }

        @Override
        public InetAddress deserialize(ByteBuffer bytes) {
            try {
                return InetAddress.getByAddress(Bytes.getArray(bytes));
            } catch (UnknownHostException e) {
                throw new InvalidTypeException("Invalid bytes for inet value, got " + bytes.remaining() + " bytes");
            }
        }
    }

    public static class IntCodec extends TypeCodec<Integer> {

        public static final IntCodec instance = new IntCodec();

        private IntCodec() {
            super(cint(), Integer.class);
        }

        @Override
        public Integer parse(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Integer value) {
            return Integer.toString(value);
        }

        @Override
        public ByteBuffer serialize(Integer value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(int value) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(0, value);
            return bb;
        }

        @Override
        public Integer deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public int deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 4)
                throw new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getInt(bytes.position());
        }
    }

    public static class TimestampCodec extends TypeCodec<Date> {

        private static final String[] iso8601Patterns = new String[] {
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mmZ",
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mmZ",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd",
            "yyyy-MM-ddZ"
        };

        public static final TimestampCodec instance = new TimestampCodec();
        private static final Pattern IS_LONG_PATTERN = Pattern.compile("^-?\\d+$");

        private TimestampCodec() {
            super(timestamp(), Date.class);
        }

        /*
         * Copied and adapted from apache commons DateUtils.parseStrictly method (that is used Cassandra side
         * to parse date strings). It is copied here so as to not create a dependency on apache commons "just
         * for this".
         */
        private static Date parseDate(String str, final String[] parsePatterns) throws ParseException {
            SimpleDateFormat parser = new SimpleDateFormat();
            parser.setLenient(false);

            ParsePosition pos = new ParsePosition(0);
            for (String parsePattern : parsePatterns) {
                String pattern = parsePattern;

                parser.applyPattern(pattern);
                pos.setIndex(0);

                String str2 = str;
                Date date = parser.parse(str2, pos);
                if (date != null && pos.getIndex() == str2.length()) {
                    return date;
                }
            }
            throw new ParseException("Unable to parse the date: " + str, -1);
        }

        @Override
        public Date parse(String value) {
            if (IS_LONG_PATTERN.matcher(value).matches()) {
                try {
                    return new Date(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
                }
            }

            try {
                return parseDate(value, iso8601Patterns);
            } catch (ParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
        }

        @Override
        public String format(Date value) {
            return Long.toString(value.getTime());
        }

        @Override
        public ByteBuffer serialize(Date value) {
            return BigintCodec.instance.serializeNoBoxing(value.getTime());
        }

        @Override
        public Date deserialize(ByteBuffer bytes) {
            return new Date(BigintCodec.instance.deserializeNoBoxing(bytes));
        }
    }

    public static abstract class AbstractUUIDCodec extends TypeCodec<UUID> {

        private AbstractUUIDCodec(DataType cqlType) {
            super(cqlType, UUID.class);
        }

        @Override
        public UUID parse(String value) {
            try {
                return UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse UUID value from \"%s\"", value));
            }
        }

        @Override
        public String format(UUID value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(UUID value) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(0, value.getMostSignificantBits());
            bb.putLong(8, value.getLeastSignificantBits());
            return bb;
        }

        @Override
        public UUID deserialize(ByteBuffer bytes) {
            return new UUID(bytes.getLong(bytes.position() + 0), bytes.getLong(bytes.position() + 8));
        }
    }

    public static class UUIDCodec extends AbstractUUIDCodec {

        public static final UUIDCodec instance = new UUIDCodec();

        private UUIDCodec() {
            super(uuid());
        }

    }

    public static class TimeUUIDCodec extends AbstractUUIDCodec {

        public static final TimeUUIDCodec instance = new TimeUUIDCodec();

        private TimeUUIDCodec() {
            super(timeuuid());
        }

    }

    public static class BigIntegerCodec extends TypeCodec<BigInteger> {

        public static final BigIntegerCodec instance = new BigIntegerCodec();

        private BigIntegerCodec() {
            super(varint(), BigInteger.class);
        }

        @Override
        public BigInteger parse(String value) {
            try {
                return new BigInteger(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse varint value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigInteger value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigInteger value) {
            return ByteBuffer.wrap(value.toByteArray());
        }

        @Override
        public BigInteger deserialize(ByteBuffer bytes) {
            return new BigInteger(Bytes.getArray(bytes));
        }
    }

    public static class ListCodec<T> extends TypeCodec<List<T>> {

        private final TypeCodec<T> eltCodec;

        public ListCodec(TypeCodec<T> eltCodec) {
            super(list(eltCodec.cqlType), new TypeToken<List<T>>() {
            }.where(new TypeParameter<T>() {
            }, eltCodec.getJavaType()));
            this.eltCodec = eltCodec;
        }

        @Override
        public boolean canSerialize(Object value) {
            if(value instanceof List) {
                // runtime type ok, now check element type
                List<?> list = (List<?>)value;
                if(list.isEmpty())
                    return true;
                Object elt = list.get(0);
                return eltCodec.canSerialize(elt);
            }
            return false;
        }

        @Override
        public List<T> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(List<T> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = 0; i < value.size(); i++) {
                if (i != 0)
                    sb.append(",");
                sb.append(eltCodec.format(value.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(List<T> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            int size = 0;
            for (T elt : value) {
                ByteBuffer bb;
                try {
                    bb = eltCodec.serialize(elt);
                } catch (ClassCastException e) {
                    // With collections, the element type has not been checked, so it can throw
                    throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
                }
                bbs.add(bb);
                size += 2 + bb.remaining();
            }
            return CodecUtils.pack(bbs, value.size(), size);
        }

        @Override
        public List<T> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.getUnsignedShort(input);
                List<T> l = new ArrayList<T>(n);
                for (int i = 0; i < n; i++) {
                    int s = CodecUtils.getUnsignedShort(input);
                    byte[] data = new byte[s];
                    input.get(data);
                    ByteBuffer databb = ByteBuffer.wrap(data);
                    l.add(eltCodec.deserialize(databb));
                }
                return l;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize list");
            }
        }
    }

    public static class SetCodec<T> extends TypeCodec<Set<T>> {

        private final TypeCodec<T> eltCodec;

        public SetCodec(TypeCodec<T> eltCodec) {
            super(set(eltCodec.cqlType), new TypeToken<Set<T>>() {
            }.where(new TypeParameter<T>() {
            }, eltCodec.getJavaType()));
            this.eltCodec = eltCodec;
        }

        @Override
        public boolean canSerialize(Object value) {
            if(value instanceof Set) {
                // runtime type ok, now check element type
                Set<?> set = (Set<?>)value;
                if(set.isEmpty())
                    return true;
                Object elt = set.iterator().next();
                return eltCodec.canSerialize(elt);
            }
            return false;
        }

        @Override
        public Set<T> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(Set<T> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            int i = 0;
            for (T v : value) {
                if (i++ != 0)
                    sb.append(",");
                sb.append(eltCodec.format(v));
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(Set<T> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            int size = 0;
            for (T elt : value) {
                ByteBuffer bb;
                try {
                    bb = eltCodec.serialize(elt);
                } catch (ClassCastException e) {
                    // With collections, the element type has not been checked, so it can throw
                    throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
                }
                bbs.add(bb);
                size += 2 + bb.remaining();
            }
            return CodecUtils.pack(bbs, value.size(), size);
        }

        @Override
        public Set<T> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.getUnsignedShort(input);
                Set<T> l = new LinkedHashSet<T>(n);
                for (int i = 0; i < n; i++) {
                    int s = CodecUtils.getUnsignedShort(input);
                    byte[] data = new byte[s];
                    input.get(data);
                    ByteBuffer databb = ByteBuffer.wrap(data);
                    l.add(eltCodec.deserialize(databb));
                }
                return l;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize a set");
            }
        }
    }

    public static class MapCodec<K, V> extends TypeCodec<Map<K, V>> {

        private final TypeCodec<K> keyCodec;
        private final TypeCodec<V> valueCodec;

        public MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
            super(map(keyCodec.cqlType, valueCodec.cqlType),
                new TypeToken<Map<K, V>>(){}
                .where(new TypeParameter<K>(){}, keyCodec.getJavaType())
                .where(new TypeParameter<V>(){}, valueCodec.getJavaType())
            );
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
        }

        @Override
        public boolean canSerialize(Object value) {
            if(value instanceof Map) {
                // runtime type ok, now check key and value types
                Map<?,?> map = (Map<?,?>)value;
                if(map.isEmpty())
                    return true;
                Map.Entry<?, ?> entry = map.entrySet().iterator().next();
                return keyCodec.canSerialize(entry.getKey()) && valueCodec.canSerialize(entry.getValue());
            }
            return false;
        }

        @Override
        public Map<K, V> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String format(Map<K, V> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            int i = 0;
            for (Map.Entry<K, V> e : value.entrySet()) {
                if (i++ != 0)
                    sb.append(",");
                sb.append(keyCodec.format(e.getKey()));
                sb.append(":");
                sb.append(valueCodec.format(e.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(Map<K, V> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
            int size = 0;
            for (Map.Entry<K, V> entry : value.entrySet()) {
                ByteBuffer bbk;
                try {
                    bbk = keyCodec.serialize(entry.getKey());
                } catch (ClassCastException e) {
                    // With collections, the element type has not been checked, so it can throw
                    throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
                }
                ByteBuffer bbv;
                try {
                    bbv = valueCodec.serialize(entry.getValue());
                } catch (ClassCastException e) {
                    // With collections, the element type has not been checked, so it can throw
                    throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
                }
                bbs.add(bbk);
                bbs.add(bbv);
                size += 4 + bbk.remaining() + bbv.remaining();
            }
            return CodecUtils.pack(bbs, value.size(), size);
        }

        @Override
        public Map<K, V> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = CodecUtils.getUnsignedShort(input);
                Map<K, V> m = new LinkedHashMap<K, V>(n);
                for (int i = 0; i < n; i++) {
                    int sk = CodecUtils.getUnsignedShort(input);
                    byte[] datak = new byte[sk];
                    input.get(datak);
                    ByteBuffer kbb = ByteBuffer.wrap(datak);

                    int sv = CodecUtils.getUnsignedShort(input);
                    byte[] datav = new byte[sv];
                    input.get(datav);
                    ByteBuffer vbb = ByteBuffer.wrap(datav);

                    m.put(keyCodec.deserialize(kbb), valueCodec.deserialize(vbb));
                }
                return m;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize a map");
            }
        }
    }
}
