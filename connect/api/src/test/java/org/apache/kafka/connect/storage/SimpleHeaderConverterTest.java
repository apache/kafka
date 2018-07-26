/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SimpleHeaderConverterTest {

    private static final String TOPIC = "topic";
    private static final String HEADER = "header";

    private static final Map<String, String> STRING_MAP = new LinkedHashMap<>();
    private static final Schema STRING_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).schema();

    private static final Map<String, Short> STRING_SHORT_MAP = new LinkedHashMap<>();
    private static final Schema STRING_SHORT_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT16_SCHEMA).schema();

    private static final Map<String, Integer> STRING_INT_MAP = new LinkedHashMap<>();
    private static final Schema STRING_INT_MAP_SCHEMA = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).schema();

    private static final List<Integer> INT_LIST = new ArrayList<>();
    private static final Schema INT_LIST_SCHEMA = SchemaBuilder.array(Schema.INT32_SCHEMA).schema();

    private static final List<String> STRING_LIST = new ArrayList<>();
    private static final Schema STRING_LIST_SCHEMA = SchemaBuilder.array(Schema.STRING_SCHEMA).schema();

    static {
        STRING_MAP.put("foo", "123");
        STRING_MAP.put("bar", "baz");
        STRING_SHORT_MAP.put("foo", (short) 12345);
        STRING_SHORT_MAP.put("bar", (short) 0);
        STRING_SHORT_MAP.put("baz", (short) -4321);
        STRING_INT_MAP.put("foo", 1234567890);
        STRING_INT_MAP.put("bar", 0);
        STRING_INT_MAP.put("baz", -987654321);
        STRING_LIST.add("foo");
        STRING_LIST.add("bar");
        INT_LIST.add(1234567890);
        INT_LIST.add(-987654321);
    }

    private SimpleHeaderConverter converter;

    @Before
    public void beforeEach() {
        converter = new SimpleHeaderConverter();
    }

    @Test
    public void shouldConvertNullValue() {
        assertRoundTrip(Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_STRING_SCHEMA, null);
    }

    @Test
    public void shouldConvertSimpleString() {
        assertRoundTrip(Schema.STRING_SCHEMA, "simple");
    }

    @Test
    public void shouldConvertEmptyString() {
        assertRoundTrip(Schema.STRING_SCHEMA, "");
    }

    @Test
    public void shouldConvertStringWithQuotesAndOtherDelimiterCharacters() {
        assertRoundTrip(Schema.STRING_SCHEMA, "three\"blind\\\"mice");
        assertRoundTrip(Schema.STRING_SCHEMA, "string with delimiters: <>?,./\\=+-!@#$%^&*(){}[]|;':");
    }

    @Test
    public void shouldConvertMapWithStringKeys() {
        assertRoundTrip(STRING_MAP_SCHEMA, STRING_MAP);
    }

    @Test
    public void shouldParseStringOfMapWithStringValuesWithoutWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(Schema.STRING_SCHEMA, "{\"foo\":\"123\",\"bar\":\"baz\"}");
        assertEquals(STRING_MAP_SCHEMA, result.schema());
        assertEquals(STRING_MAP, result.value());
    }

    @Test
    public void shouldParseStringOfMapWithStringValuesWithWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(Schema.STRING_SCHEMA, "{ \"foo\" : \"123\", \n\"bar\" : \"baz\" } ");
        assertEquals(STRING_MAP_SCHEMA, result.schema());
        assertEquals(STRING_MAP, result.value());
    }

    @Test
    public void shouldConvertMapWithStringKeysAndShortValues() {
        assertRoundTrip(STRING_SHORT_MAP_SCHEMA, STRING_SHORT_MAP);
    }

    @Test
    public void shouldParseStringOfMapWithShortValuesWithoutWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(Schema.STRING_SCHEMA, "{\"foo\":12345,\"bar\":0,\"baz\":-4321}");
        assertEquals(STRING_SHORT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_SHORT_MAP, result.value());
    }

    @Test
    public void shouldParseStringOfMapWithShortValuesWithWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(Schema.STRING_SCHEMA, " { \"foo\" :  12345 , \"bar\" : 0,  \"baz\" : -4321 }  ");
        assertEquals(STRING_SHORT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_SHORT_MAP, result.value());
    }

    @Test
    public void shouldConvertMapWithStringKeysAndIntegerValues() {
        assertRoundTrip(STRING_INT_MAP_SCHEMA, STRING_INT_MAP);
    }

    @Test
    public void shouldParseStringOfMapWithIntValuesWithoutWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(Schema.STRING_SCHEMA, "{\"foo\":1234567890,\"bar\":0,\"baz\":-987654321}");
        assertEquals(STRING_INT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_INT_MAP, result.value());
    }

    @Test
    public void shouldParseStringOfMapWithIntValuesWithWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(Schema.STRING_SCHEMA, " { \"foo\" :  1234567890 , \"bar\" : 0,  \"baz\" : -987654321 }  ");
        assertEquals(STRING_INT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_INT_MAP, result.value());
    }

    @Test
    public void shouldConvertListWithStringValues() {
        assertRoundTrip(STRING_LIST_SCHEMA, STRING_LIST);
    }

    @Test
    public void shouldConvertListWithIntegerValues() {
        assertRoundTrip(INT_LIST_SCHEMA, INT_LIST);
    }

    @Test
    public void shouldConvertMapWithStringKeysAndMixedValuesToMapWithoutSchema() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("foo", "bar");
        map.put("baz", (short) 3456);
        assertRoundTrip(null, map);
    }

    @Test
    public void shouldConvertListWithMixedValuesToListWithoutSchema() {
        List<Object> list = new ArrayList<>();
        list.add("foo");
        list.add((short) 13344);
        assertRoundTrip(null, list);
    }

    @Test
    public void shouldConvertEmptyMapToMapWithoutSchema() {
        assertRoundTrip(null, new LinkedHashMap<>());
    }

    @Test
    public void shouldConvertEmptyListToListWithoutSchema() {
        assertRoundTrip(null, new ArrayList<>());
    }

    protected SchemaAndValue roundTrip(Schema schema, Object input) {
        byte[] serialized = converter.fromConnectHeader(TOPIC, HEADER, schema, input);
        return converter.toConnectHeader(TOPIC, HEADER, serialized);
    }

    protected void assertRoundTrip(Schema schema, Object value) {
        byte[] serialized = converter.fromConnectHeader(TOPIC, HEADER, schema, value);
        SchemaAndValue result = converter.toConnectHeader(TOPIC, HEADER, serialized);

        if (value == null) {
            assertNull(serialized);
            assertNull(result.schema());
            assertNull(result.value());
        } else {
            assertNotNull(serialized);
            assertEquals(value, result.value());
            assertEquals(schema, result.schema());

            byte[] serialized2 = converter.fromConnectHeader(TOPIC, HEADER, result.schema(), result.value());
            SchemaAndValue result2 = converter.toConnectHeader(TOPIC, HEADER, serialized2);
            assertNotNull(serialized2);
            assertEquals(schema, result2.schema());
            assertEquals(value, result2.value());
            assertEquals(result, result2);
            assertArrayEquals(serialized, serialized);
        }
    }

}