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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.text;

public class TypeCodecTest {

    public static final DataType CUSTOM_FOO = DataType.custom("com.example.FooBar");

    private CodecRegistry codecRegistry = CodecRegistry.builder()
        .withDefaultCodecs()
        .withCustomType(CUSTOM_FOO).build();

    @Test(groups = "unit")
    public void testCustomList() throws Exception {
        DataType cqlType = DataType.list(CUSTOM_FOO);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomSet() throws Exception {
        DataType cqlType = DataType.set(CUSTOM_FOO);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomKeyMap() throws Exception {
        DataType cqlType = DataType.map(CUSTOM_FOO, text());
        TypeCodec<Map<?,?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit")
    public void testCustomValueMap() throws Exception {
        DataType cqlType = DataType.map(text(), CUSTOM_FOO);
        TypeCodec<Map<?,?>> codec = codecRegistry.codecFor(cqlType);
        assertThat(codec).isNotNull().canDeserialize(cqlType);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.cint());
        List<Integer> list = Collections.nCopies(65536, 1);
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list);
    }

    @Test(groups = "unit", expectedExceptions = { IllegalArgumentException.class })
    public void collectionElementTooLargeTest() throws Exception {
        DataType cqlType = DataType.list(DataType.text());
        List<String> list = Lists.newArrayList(Strings.repeat("a", 65536));
        TypeCodec<List<?>> codec = codecRegistry.codecFor(cqlType);
        codec.serialize(list);
    }
}
