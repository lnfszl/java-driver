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
import java.util.List;

import com.google.common.reflect.TypeToken;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CodecUtils {

    public static ByteBuffer[] convert(List<Object> values, CodecRegistry codecRegistry) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.size()];
        for (int i = 0; i < values.size(); i++) {
            try {
                Object value = values.get(i);
                if (value == null) {
                    serializedValues[i] = null;
                } else {
                    if (value instanceof Token)
                        value = ((Token)value).getValue();
                    TypeCodec<Object> codec = codecRegistry.codecFor(value);
                    serializedValues[i] = codec.serialize(value);
                }
            } catch (IllegalArgumentException e) {
                // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                throw new IllegalArgumentException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, values.get(i).getClass()));
            }
        }
        return serializedValues;
    }

    // Utility method for collections
    public static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size) {
        if (elements > 65535)
            throw new IllegalArgumentException("Native protocol version 2 supports up to 65535 elements in any collection - but collection contains " + elements + " elements");
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers) {
            int elemSize = bb.remaining();
            if (elemSize > 65535)
                throw new IllegalArgumentException("Native protocol version 2 supports only elements with size up to 65535 bytes - but element size is " + elemSize + " bytes");
            result.putShort((short)elemSize);
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }

    // Utility method for collections
    public static int getUnsignedShort(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }
}
