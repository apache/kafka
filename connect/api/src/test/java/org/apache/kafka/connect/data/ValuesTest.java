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
package org.apache.kafka.connect.data;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Values.Parser;
import org.apache.kafka.connect.errors.DataException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ValuesTest {

    private static final String WHITESPACE = "\n \t \t\n";

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000;

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

    @Test
    public void shouldParseNullString() {
        SchemaAndValue schemaAndValue = Values.parseString(null);
        assertNull(schemaAndValue.schema());
        assertNull(schemaAndValue.value());
    }

    @Test
    public void shouldParseEmptyString() {
        SchemaAndValue schemaAndValue = Values.parseString("");
        assertEquals(Schema.STRING_SCHEMA, schemaAndValue.schema());
        assertEquals("", schemaAndValue.value());
    }

    @Test
    @Timeout(5)
    public void shouldNotEncounterInfiniteLoop() {
        // This byte sequence gets parsed as CharacterIterator.DONE and can cause issues if
        // comparisons to that character are done to check if the end of a string has been reached.
        // For more information, see https://issues.apache.org/jira/browse/KAFKA-10574
        byte[] bytes = new byte[] {-17, -65,  -65};
        String str = new String(bytes, StandardCharsets.UTF_8);
        SchemaAndValue schemaAndValue = Values.parseString(str);
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals(str, schemaAndValue.value());
    }

    @Test
    public void shouldNotParseUnquotedEmbeddedMapKeysAsStrings() {
        SchemaAndValue schemaAndValue = Values.parseString("{foo: 3}");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("{foo: 3}", schemaAndValue.value());
    }

    @Test
    public void shouldNotParseUnquotedEmbeddedMapValuesAsStrings() {
        SchemaAndValue schemaAndValue = Values.parseString("{3: foo}");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("{3: foo}", schemaAndValue.value());
    }

    @Test
    public void shouldNotParseUnquotedArrayElementsAsStrings() {
        SchemaAndValue schemaAndValue = Values.parseString("[foo]");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("[foo]", schemaAndValue.value());
    }

    @Test
    public void shouldNotParseStringsBeginningWithNullAsStrings() {
        SchemaAndValue schemaAndValue = Values.parseString("null=");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("null=", schemaAndValue.value());
    }

    @Test
    public void shouldParseStringsBeginningWithTrueAsStrings() {
        SchemaAndValue schemaAndValue = Values.parseString("true}");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("true}", schemaAndValue.value());
    }

    @Test
    public void shouldParseStringsBeginningWithFalseAsStrings() {
        SchemaAndValue schemaAndValue = Values.parseString("false]");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("false]", schemaAndValue.value());
    }

    @Test
    public void shouldParseTrueAsBooleanIfSurroundedByWhitespace() {
        SchemaAndValue schemaAndValue = Values.parseString(WHITESPACE + "true" + WHITESPACE);
        assertEquals(Type.BOOLEAN, schemaAndValue.schema().type());
        assertEquals(true, schemaAndValue.value());
    }

    @Test
    public void shouldParseFalseAsBooleanIfSurroundedByWhitespace() {
        SchemaAndValue schemaAndValue = Values.parseString(WHITESPACE + "false" + WHITESPACE);
        assertEquals(Type.BOOLEAN, schemaAndValue.schema().type());
        assertEquals(false, schemaAndValue.value());
    }

    @Test
    public void shouldParseNullAsNullIfSurroundedByWhitespace() {
        SchemaAndValue schemaAndValue = Values.parseString(WHITESPACE + "null" + WHITESPACE);
        assertNull(schemaAndValue);
    }

    @Test
    public void shouldParseBooleanLiteralsEmbeddedInArray() {
        SchemaAndValue schemaAndValue = Values.parseString("[true, false]");
        assertEquals(Type.ARRAY, schemaAndValue.schema().type());
        assertEquals(Type.BOOLEAN, schemaAndValue.schema().valueSchema().type());
        assertEquals(Arrays.asList(true, false), schemaAndValue.value());
    }

    @Test
    public void shouldParseBooleanLiteralsEmbeddedInMap() {
        SchemaAndValue schemaAndValue = Values.parseString("{true: false, false: true}");
        assertEquals(Type.MAP, schemaAndValue.schema().type());
        assertEquals(Type.BOOLEAN, schemaAndValue.schema().keySchema().type());
        assertEquals(Type.BOOLEAN, schemaAndValue.schema().valueSchema().type());
        Map<Boolean, Boolean> expectedValue = new HashMap<>();
        expectedValue.put(true, false);
        expectedValue.put(false, true);
        assertEquals(expectedValue, schemaAndValue.value());
    }

    @Test
    public void shouldNotParseAsMapWithoutCommas() {
        SchemaAndValue schemaAndValue = Values.parseString("{6:9 4:20}");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("{6:9 4:20}", schemaAndValue.value());
    }

    @Test
    public void shouldNotParseAsArrayWithoutCommas() {
        SchemaAndValue schemaAndValue = Values.parseString("[0 1 2]");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("[0 1 2]", schemaAndValue.value());
    }

    @Test
    public void shouldParseEmptyMap() {
        SchemaAndValue schemaAndValue = Values.parseString("{}");
        assertEquals(Type.MAP, schemaAndValue.schema().type());
        assertEquals(Collections.emptyMap(), schemaAndValue.value());
    }

    @Test
    public void shouldParseEmptyArray() {
        SchemaAndValue schemaAndValue = Values.parseString("[]");
        assertEquals(Type.ARRAY, schemaAndValue.schema().type());
        assertEquals(Collections.emptyList(), schemaAndValue.value());
    }

    @Test
    public void shouldNotParseAsMapWithNullKeys() {
        SchemaAndValue schemaAndValue = Values.parseString("{null: 3}");
        assertEquals(Type.STRING, schemaAndValue.schema().type());
        assertEquals("{null: 3}", schemaAndValue.value());
    }

    @Test
    public void shouldParseNull() {
        SchemaAndValue schemaAndValue = Values.parseString("null");
        assertNull(schemaAndValue);
    }

    @Test
    public void shouldConvertStringOfNull() {
        assertRoundTrip(Schema.STRING_SCHEMA, "null");
    }

    @Test
    public void shouldParseNullMapValues() {
        SchemaAndValue schemaAndValue = Values.parseString("{3: null}");
        assertEquals(Type.MAP, schemaAndValue.schema().type());
        assertEquals(Type.INT8, schemaAndValue.schema().keySchema().type());
        assertEquals(Collections.singletonMap((byte) 3, null), schemaAndValue.value());
    }

    @Test
    public void shouldParseNullArrayElements() {
        SchemaAndValue schemaAndValue = Values.parseString("[null]");
        assertEquals(Type.ARRAY, schemaAndValue.schema().type());
        assertEquals(Collections.singletonList(null), schemaAndValue.value());
    }

    @Test
    public void shouldEscapeStringsWithEmbeddedQuotesAndBackslashes() {
        String original = "three\"blind\\\"mice";
        String expected = "three\\\"blind\\\\\\\"mice";
        assertEquals(expected, Values.escape(original));
    }

    @Test
    public void shouldConvertNullValue() {
        assertRoundTrip(Schema.INT8_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_INT8_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.INT16_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_INT16_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_INT32_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.INT64_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_INT64_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.FLOAT32_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_FLOAT32_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.FLOAT64_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_FLOAT64_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.BOOLEAN_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_BOOLEAN_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA, null);
        assertRoundTrip(Schema.OPTIONAL_STRING_SCHEMA, Schema.STRING_SCHEMA, null);
    }

    @Test
    public void shouldConvertBooleanValues() {
        assertRoundTrip(Schema.BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
        assertShortCircuit(Schema.BOOLEAN_SCHEMA, Boolean.FALSE);
        SchemaAndValue resultFalse = roundTrip(Schema.BOOLEAN_SCHEMA, "false");
        assertEquals(Schema.BOOLEAN_SCHEMA, resultFalse.schema());
        assertEquals(Boolean.FALSE, resultFalse.value());
        resultFalse = roundTrip(Schema.BOOLEAN_SCHEMA, "0");
        assertEquals(Schema.BOOLEAN_SCHEMA, resultFalse.schema());
        assertEquals(Boolean.FALSE, resultFalse.value());

        assertRoundTrip(Schema.BOOLEAN_SCHEMA, Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
        assertShortCircuit(Schema.BOOLEAN_SCHEMA, Boolean.TRUE);
        SchemaAndValue resultTrue = roundTrip(Schema.BOOLEAN_SCHEMA, "true");
        assertEquals(Schema.BOOLEAN_SCHEMA, resultTrue.schema());
        assertEquals(Boolean.TRUE, resultTrue.value());
        resultTrue = roundTrip(Schema.BOOLEAN_SCHEMA, "1");
        assertEquals(Schema.BOOLEAN_SCHEMA, resultTrue.schema());
        assertEquals(Boolean.TRUE, resultTrue.value());
    }

    @Test
    public void shouldFailToParseInvalidBooleanValueString() {
        assertThrows(DataException.class, () -> Values.convertToBoolean(Schema.STRING_SCHEMA, "\"green\""));
    }

    @Test
    public void shouldConvertInt8() {
        assertRoundTrip(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA, (byte) 0);
        assertRoundTrip(Schema.INT8_SCHEMA, Schema.INT8_SCHEMA, (byte) 1);
    }

    @Test
    public void shouldConvertInt64() {
        assertRoundTrip(Schema.INT64_SCHEMA, Schema.INT64_SCHEMA, (long) 1);
        assertShortCircuit(Schema.INT64_SCHEMA, (long) 1);
    }

    @Test
    public void shouldConvertFloat32() {
        assertRoundTrip(Schema.FLOAT32_SCHEMA, Schema.FLOAT32_SCHEMA, (float) 1);
        assertShortCircuit(Schema.FLOAT32_SCHEMA, (float) 1);
    }

    @Test
    public void shouldConvertFloat64() {
        assertRoundTrip(Schema.FLOAT64_SCHEMA, Schema.FLOAT64_SCHEMA, (double) 1);
        assertShortCircuit(Schema.FLOAT64_SCHEMA, (double) 1);
    }

    @Test
    public void shouldConvertEmptyStruct() {
        Struct struct = new Struct(SchemaBuilder.struct().build());
        assertThrows(DataException.class, () -> Values.convertToStruct(struct.schema(), null));
        assertThrows(DataException.class, () -> Values.convertToStruct(struct.schema(), ""));
        Values.convertToStruct(struct.schema(), struct);
    }

    @Test
    public void shouldConvertSimpleString() {
        assertRoundTrip(Schema.STRING_SCHEMA,  "simple");
    }

    @Test
    public void shouldConvertEmptyString() {
        assertRoundTrip(Schema.STRING_SCHEMA, "");
    }

    @Test
    public void shouldConvertStringWithQuotesAndOtherDelimiterCharacters() {
        assertRoundTrip(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA, "three\"blind\\\"mice");
        assertRoundTrip(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA, "string with delimiters: <>?,./\\=+-!@#$%^&*(){}[]|;':");
    }

    @Test
    public void shouldConvertMapWithStringKeys() {
        assertRoundTrip(STRING_MAP_SCHEMA, STRING_MAP_SCHEMA, STRING_MAP);
    }

    @Test
    public void shouldParseStringOfMapWithStringValuesWithoutWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(STRING_MAP_SCHEMA, "{\"foo\":\"123\",\"bar\":\"baz\"}");
        assertEquals(STRING_MAP_SCHEMA, result.schema());
        assertEquals(STRING_MAP, result.value());
    }

    @Test
    public void shouldParseStringOfMapWithStringValuesWithWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(STRING_MAP_SCHEMA, "{ \"foo\" : \"123\", \n\"bar\" : \"baz\" } ");
        assertEquals(STRING_MAP_SCHEMA, result.schema());
        assertEquals(STRING_MAP, result.value());
    }

    @Test
    public void shouldConvertMapWithStringKeysAndShortValues() {
        assertRoundTrip(STRING_SHORT_MAP_SCHEMA, STRING_SHORT_MAP_SCHEMA, STRING_SHORT_MAP);
    }

    @Test
    public void shouldParseStringOfMapWithShortValuesWithoutWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(STRING_SHORT_MAP_SCHEMA, "{\"foo\":12345,\"bar\":0,\"baz\":-4321}");
        assertEquals(STRING_SHORT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_SHORT_MAP, result.value());
    }

    @Test
    public void shouldParseStringOfMapWithShortValuesWithWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(STRING_SHORT_MAP_SCHEMA, " { \"foo\" :  12345 , \"bar\" : 0,  \"baz\" : -4321 }  ");
        assertEquals(STRING_SHORT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_SHORT_MAP, result.value());
    }

    @Test
    public void shouldConvertMapWithStringKeysAndIntegerValues() {
        assertRoundTrip(STRING_INT_MAP_SCHEMA, STRING_INT_MAP_SCHEMA, STRING_INT_MAP);
    }

    @Test
    public void shouldParseStringOfMapWithIntValuesWithoutWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(STRING_INT_MAP_SCHEMA, "{\"foo\":1234567890,\"bar\":0,\"baz\":-987654321}");
        assertEquals(STRING_INT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_INT_MAP, result.value());
    }

    @Test
    public void shouldParseStringOfMapWithIntValuesWithWhitespaceAsMap() {
        SchemaAndValue result = roundTrip(STRING_INT_MAP_SCHEMA, " { \"foo\" :  1234567890 , \"bar\" : 0,  \"baz\" : -987654321 }  ");
        assertEquals(STRING_INT_MAP_SCHEMA, result.schema());
        assertEquals(STRING_INT_MAP, result.value());
    }

    @Test
    public void shouldConvertListWithStringValues() {
        assertRoundTrip(STRING_LIST_SCHEMA, STRING_LIST_SCHEMA, STRING_LIST);
    }

    @Test
    public void shouldConvertListWithIntegerValues() {
        assertRoundTrip(INT_LIST_SCHEMA, INT_LIST_SCHEMA, INT_LIST);
    }

    /**
     * The parsed array has byte values and one int value, so we should return list with single unified type of integers.
     */
    @Test
    public void shouldConvertStringOfListWithOnlyNumericElementTypesIntoListOfLargestNumericType() {
        int thirdValue = Short.MAX_VALUE + 1;
        List<?> list = Values.convertToList(Schema.STRING_SCHEMA, "[1, 2, " + thirdValue + "]");
        assertEquals(3, list.size());
        assertEquals(1, ((Number) list.get(0)).intValue());
        assertEquals(2, ((Number) list.get(1)).intValue());
        assertEquals(thirdValue, list.get(2));
    }

    @Test
    public void shouldConvertIntegralTypesToFloat() {
        float thirdValue = Float.MAX_VALUE;
        List<?> list = Values.convertToList(Schema.STRING_SCHEMA, "[1, 2, " + thirdValue + "]");
        assertEquals(3, list.size());
        assertEquals(1, ((Number) list.get(0)).intValue());
        assertEquals(2, ((Number) list.get(1)).intValue());
        assertEquals(thirdValue, list.get(2));
    }

    @Test
    public void shouldConvertIntegralTypesToDouble() {
        double thirdValue = Double.MAX_VALUE;
        List<?> list = Values.convertToList(Schema.STRING_SCHEMA, "[1, 2, " + thirdValue + "]");
        assertEquals(3, list.size());
        assertEquals(1, ((Number) list.get(0)).intValue());
        assertEquals(2, ((Number) list.get(1)).intValue());
        assertEquals(thirdValue, list.get(2));
    }

    /**
     * We parse into different element types, but cannot infer a common element schema.
     * This behavior should be independent of the order that the elements appear in the string
     */
    @Test
    public void shouldParseStringListWithMultipleElementTypes() {
        assertParseStringArrayWithNoSchema(
                Arrays.asList((byte) 1, (byte) 2, (short) 300, "four"),
                "[1, 2, 300, \"four\"]");
        assertParseStringArrayWithNoSchema(
                Arrays.asList((byte) 2, (short) 300, "four", (byte) 1),
                "[2, 300, \"four\", 1]");
        assertParseStringArrayWithNoSchema(
                Arrays.asList((short) 300, "four", (byte) 1, (byte) 2),
                "[300, \"four\", 1, 2]");
        assertParseStringArrayWithNoSchema(
                Arrays.asList("four", (byte) 1, (byte) 2, (short) 300),
                "[\"four\", 1, 2, 300]");
    }

    private void assertParseStringArrayWithNoSchema(List<Object> expected, String str) {
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.ARRAY, result.schema().type());
        assertNull(result.schema().valueSchema());
        List<?> list = (List<?>) result.value();
        assertEquals(expected, list);
    }

    /**
     * Maps with an inconsistent key type don't find a common type for the keys or the values
     * This behavior should be independent of the order that the pairs appear in the string
     */
    @Test
    public void shouldParseStringMapWithMultipleKeyTypes() {
        Map<Object, Object> expected = new HashMap<>();
        expected.put((byte) 1, (byte) 1);
        expected.put((byte) 2, (byte) 1);
        expected.put((short) 300, (short) 300);
        expected.put("four", (byte) 1);
        assertParseStringMapWithNoSchema(expected, "{1:1, 2:1, 300:300, \"four\":1}");
        assertParseStringMapWithNoSchema(expected, "{2:1, 300:300, \"four\":1, 1:1}");
        assertParseStringMapWithNoSchema(expected, "{300:300, \"four\":1, 1:1, 2:1}");
        assertParseStringMapWithNoSchema(expected, "{\"four\":1, 1:1, 2:1, 300:300}");
    }

    /**
     * Maps with a consistent key type may still not have a common type for the values
     * This behavior should be independent of the order that the pairs appear in the string
     */
    @Test
    public void shouldParseStringMapWithMultipleValueTypes() {
        Map<Object, Object> expected = new HashMap<>();
        expected.put((short) 1, (byte) 1);
        expected.put((short) 2, (byte) 1);
        expected.put((short) 300, (short) 300);
        expected.put((short) 4, "four");
        assertParseStringMapWithNoSchema(expected, "{1:1, 2:1, 300:300, 4:\"four\"}");
        assertParseStringMapWithNoSchema(expected, "{2:1, 300:300, 4:\"four\", 1:1}");
        assertParseStringMapWithNoSchema(expected, "{300:300, 4:\"four\", 1:1, 2:1}");
        assertParseStringMapWithNoSchema(expected, "{4:\"four\", 1:1, 2:1, 300:300}");
    }

    private void assertParseStringMapWithNoSchema(Map<Object, Object> expected, String str) {
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.MAP, result.schema().type());
        assertNull(result.schema().valueSchema());
        Map<?, ?> list = (Map<?, ?>) result.value();
        assertEquals(expected, list);
    }

    @Test
    public void shouldParseNestedArray() {
        SchemaAndValue schemaAndValue = Values.parseString("[[]]");
        assertEquals(Type.ARRAY, schemaAndValue.schema().type());
        assertEquals(Type.ARRAY, schemaAndValue.schema().valueSchema().type());
    }

    @Test
    public void shouldParseArrayContainingMap() {
        SchemaAndValue schemaAndValue = Values.parseString("[{}]");
        assertEquals(Type.ARRAY, schemaAndValue.schema().type());
        assertEquals(Type.MAP, schemaAndValue.schema().valueSchema().type());
    }

    @Test
    public void shouldParseNestedMap() {
        SchemaAndValue schemaAndValue = Values.parseString("{\"a\":{}}");
        assertEquals(Type.MAP, schemaAndValue.schema().type());
        assertEquals(Type.MAP, schemaAndValue.schema().valueSchema().type());
    }

    @Test
    public void shouldParseMapContainingArray() {
        SchemaAndValue schemaAndValue = Values.parseString("{\"a\":[]}");
        assertEquals(Type.MAP, schemaAndValue.schema().type());
        assertEquals(Type.ARRAY, schemaAndValue.schema().valueSchema().type());
    }

    /**
     * We can't infer or successfully parse into a different type, so this returns the same string.
     */
    @Test
    public void shouldParseStringListWithExtraDelimitersAndReturnString() {
        String str = "[1, 2, 3,,,]";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.STRING, result.schema().type());
        assertEquals(str, result.value());
    }

    @Test
    public void shouldParseStringListWithNullLastAsString() {
        String str = "[1, null]";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.STRING, result.schema().type());
        assertEquals(str, result.value());
    }

    @Test
    public void shouldParseStringListWithNullFirstAsString() {
        String str = "[null, 1]";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.STRING, result.schema().type());
        assertEquals(str, result.value());
    }

    @Test
    public void shouldParseTimestampStringAsTimestamp() throws Exception {
        String str = "2019-08-23T14:34:54.346Z";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.INT64, result.schema().type());
        assertEquals(Timestamp.LOGICAL_NAME, result.schema().name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(str);
        assertEquals(expected, result.value());
    }

    @Test
    public void shouldParseDateStringAsDate() throws Exception {
        String str = "2019-08-23";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.INT32, result.schema().type());
        assertEquals(Date.LOGICAL_NAME, result.schema().name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_DATE_FORMAT_PATTERN).parse(str);
        assertEquals(expected, result.value());
    }

    @Test
    public void shouldParseTimeStringAsDate() throws Exception {
        String str = "14:34:54.346Z";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.INT32, result.schema().type());
        assertEquals(Time.LOGICAL_NAME, result.schema().name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIME_FORMAT_PATTERN).parse(str);
        assertEquals(expected, result.value());
    }

    @Test
    public void shouldParseTimestampStringWithEscapedColonsAsTimestamp() throws Exception {
        String str = "2019-08-23T14\\:34\\:54.346Z";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.INT64, result.schema().type());
        assertEquals(Timestamp.LOGICAL_NAME, result.schema().name());
        String expectedStr = "2019-08-23T14:34:54.346Z";
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(expectedStr);
        assertEquals(expected, result.value());
    }

    @Test
    public void shouldParseTimeStringWithEscapedColonsAsDate() throws Exception {
        String str = "14\\:34\\:54.346Z";
        SchemaAndValue result = Values.parseString(str);
        assertEquals(Type.INT32, result.schema().type());
        assertEquals(Time.LOGICAL_NAME, result.schema().name());
        String expectedStr = "14:34:54.346Z";
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIME_FORMAT_PATTERN).parse(expectedStr);
        assertEquals(expected, result.value());
    }

    @Test
    public void shouldParseDateStringAsDateInArray() throws Exception {
        String dateStr = "2019-08-23";
        String arrayStr = "[" + dateStr + "]";
        SchemaAndValue result = Values.parseString(arrayStr);
        assertEquals(Type.ARRAY, result.schema().type());
        Schema elementSchema = result.schema().valueSchema();
        assertEquals(Type.INT32, elementSchema.type());
        assertEquals(Date.LOGICAL_NAME, elementSchema.name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_DATE_FORMAT_PATTERN).parse(dateStr);
        assertEquals(Collections.singletonList(expected), result.value());
    }

    @Test
    public void shouldParseTimeStringAsTimeInArray() throws Exception {
        String timeStr = "14:34:54.346Z";
        String arrayStr = "[" + timeStr + "]";
        SchemaAndValue result = Values.parseString(arrayStr);
        assertEquals(Type.ARRAY, result.schema().type());
        Schema elementSchema = result.schema().valueSchema();
        assertEquals(Type.INT32, elementSchema.type());
        assertEquals(Time.LOGICAL_NAME, elementSchema.name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIME_FORMAT_PATTERN).parse(timeStr);
        assertEquals(Collections.singletonList(expected), result.value());
    }

    @Test
    public void shouldParseTimestampStringAsTimestampInArray() throws Exception {
        String tsStr = "2019-08-23T14:34:54.346Z";
        String arrayStr = "[" + tsStr + "]";
        SchemaAndValue result = Values.parseString(arrayStr);
        assertEquals(Type.ARRAY, result.schema().type());
        Schema elementSchema = result.schema().valueSchema();
        assertEquals(Type.INT64, elementSchema.type());
        assertEquals(Timestamp.LOGICAL_NAME, elementSchema.name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(tsStr);
        assertEquals(Collections.singletonList(expected), result.value());
    }

    @Test
    public void shouldParseMultipleTimestampStringAsTimestampInArray() throws Exception {
        String tsStr1 = "2019-08-23T14:34:54.346Z";
        String tsStr2 = "2019-01-23T15:12:34.567Z";
        String tsStr3 = "2019-04-23T19:12:34.567Z";
        String arrayStr = "[" + tsStr1 + "," + tsStr2 + ",   " + tsStr3 + "]";
        SchemaAndValue result = Values.parseString(arrayStr);
        assertEquals(Type.ARRAY, result.schema().type());
        Schema elementSchema = result.schema().valueSchema();
        assertEquals(Type.INT64, elementSchema.type());
        assertEquals(Timestamp.LOGICAL_NAME, elementSchema.name());
        java.util.Date expected1 = new SimpleDateFormat(Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(tsStr1);
        java.util.Date expected2 = new SimpleDateFormat(Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(tsStr2);
        java.util.Date expected3 = new SimpleDateFormat(Values.ISO_8601_TIMESTAMP_FORMAT_PATTERN).parse(tsStr3);
        assertEquals(Arrays.asList(expected1, expected2, expected3), result.value());
    }

    @Test
    public void shouldParseQuotedTimeStringAsTimeInMap() throws Exception {
        String keyStr = "k1";
        String timeStr = "14:34:54.346Z";
        String mapStr = "{\"" + keyStr + "\":\"" + timeStr + "\"}";
        SchemaAndValue result = Values.parseString(mapStr);
        assertEquals(Type.MAP, result.schema().type());
        Schema keySchema = result.schema().keySchema();
        Schema valueSchema = result.schema().valueSchema();
        assertEquals(Type.STRING, keySchema.type());
        assertEquals(Type.INT32, valueSchema.type());
        assertEquals(Time.LOGICAL_NAME, valueSchema.name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIME_FORMAT_PATTERN).parse(timeStr);
        assertEquals(Collections.singletonMap(keyStr, expected), result.value());
    }

    @Test
    public void shouldParseTimeStringAsTimeInMap() throws Exception {
        String keyStr = "k1";
        String timeStr = "14:34:54.346Z";
        String mapStr = "{\"" + keyStr + "\":" + timeStr + "}";
        SchemaAndValue result = Values.parseString(mapStr);
        assertEquals(Type.MAP, result.schema().type());
        Schema keySchema = result.schema().keySchema();
        Schema valueSchema = result.schema().valueSchema();
        assertEquals(Type.STRING, keySchema.type());
        assertEquals(Type.INT32, valueSchema.type());
        assertEquals(Time.LOGICAL_NAME, valueSchema.name());
        java.util.Date expected = new SimpleDateFormat(Values.ISO_8601_TIME_FORMAT_PATTERN).parse(timeStr);
        assertEquals(Collections.singletonMap(keyStr, expected), result.value());
    }

    @Test
    public void shouldFailToConvertNullTime() {
        assertThrows(DataException.class, () -> Values.convertToTime(null, null));
        assertThrows(DataException.class, () -> Values.convertToDate(null, null));
        assertThrows(DataException.class, () -> Values.convertToTimestamp(null, null));
    }

    /**
     * This is technically invalid JSON, and we don't want to simply ignore the blank elements.
     */
    @Test
    public void shouldFailToConvertToListFromStringWithExtraDelimiters() {
        assertThrows(DataException.class, () -> Values.convertToList(Schema.STRING_SCHEMA, "[1, 2, 3,,,]"));
    }

    /**
     * Schema of type ARRAY requires a schema for the values, but Connect has no union or "any" schema type.
     * Therefore, we can't represent this.
     */
    @Test
    public void shouldFailToConvertToListFromStringWithNonCommonElementTypeAndBlankElement() {
        assertThrows(DataException.class, () -> Values.convertToList(Schema.STRING_SCHEMA, "[1, 2, 3, \"four\",,,]"));
    }

    /**
     * This is technically invalid JSON, and we don't want to simply ignore the blank entry.
     */
    @Test
    public void shouldFailToParseStringOfMapWithIntValuesWithBlankEntry() {
        assertThrows(DataException.class,
            () -> Values.convertToMap(Schema.STRING_SCHEMA, " { \"foo\" :  1234567890 ,, \"bar\" : 0,  \"baz\" : -987654321 }  "));
    }

    /**
     * This is technically invalid JSON, and we don't want to simply ignore the malformed entry.
     */
    @Test
    public void shouldFailToParseStringOfMalformedMap() {
        assertThrows(DataException.class,
            () -> Values.convertToMap(Schema.STRING_SCHEMA, " { \"foo\" :  1234567890 , \"a\", \"bar\" : 0,  \"baz\" : -987654321 }  "));
    }

    /**
     * This is technically invalid JSON, and we don't want to simply ignore the blank entries.
     */
    @Test
    public void shouldFailToParseStringOfMapWithIntValuesWithOnlyBlankEntries() {
        assertThrows(DataException.class, () -> Values.convertToMap(Schema.STRING_SCHEMA, " { ,,  , , }  "));
    }

    /**
     * This is technically invalid JSON, and we don't want to simply ignore the blank entry.
     */
    @Test
    public void shouldFailToParseStringOfMapWithIntValuesWithBlankEntries() {
        assertThrows(DataException.class,
            () -> Values.convertToMap(Schema.STRING_SCHEMA, " { \"foo\" :  \"1234567890\" ,, \"bar\" : \"0\",  \"baz\" : \"boz\" }  "));
    }

    @Test
    public void shouldConsumeMultipleTokens() {
        String value = "a:b:c:d:e:f:g:h";
        Parser parser = new Parser(value);
        String firstFive = parser.next(5);
        assertEquals("a:b:c", firstFive);
        assertEquals(":", parser.next());
        assertEquals("d", parser.next());
        assertEquals(":", parser.next());
        String lastEight = parser.next(8); // only 7 remain
        assertNull(lastEight);
        assertEquals("e", parser.next());
    }

    @Test
    public void shouldParseStringsWithoutDelimiters() {
        //assertParsed("");
        assertParsed("  ");
        assertParsed("simple");
        assertParsed("simple string");
        assertParsed("simple \n\t\bstring");
        assertParsed("'simple' string");
        assertParsed("si\\mple");
        assertParsed("si\\\\mple");
    }

    @Test
    public void shouldParseStringsWithEscapedDelimiters() {
        assertParsed("si\\\"mple");
        assertParsed("si\\{mple");
        assertParsed("si\\}mple");
        assertParsed("si\\]mple");
        assertParsed("si\\[mple");
        assertParsed("si\\:mple");
        assertParsed("si\\,mple");
    }

    @Test
    public void shouldParseStringsWithSingleDelimiter() {
        assertParsed("a{b", "a", "{", "b");
        assertParsed("a}b", "a", "}", "b");
        assertParsed("a[b", "a", "[", "b");
        assertParsed("a]b", "a", "]", "b");
        assertParsed("a:b", "a", ":", "b");
        assertParsed("a,b", "a", ",", "b");
        assertParsed("a\"b", "a", "\"", "b");
        assertParsed("{b", "{", "b");
        assertParsed("}b", "}", "b");
        assertParsed("[b", "[", "b");
        assertParsed("]b", "]", "b");
        assertParsed(":b", ":", "b");
        assertParsed(",b", ",", "b");
        assertParsed("\"b", "\"", "b");
        assertParsed("{", "{");
        assertParsed("}", "}");
        assertParsed("[", "[");
        assertParsed("]", "]");
        assertParsed(":", ":");
        assertParsed(",", ",");
        assertParsed("\"", "\"");
    }

    @Test
    public void shouldParseStringsWithMultipleDelimiters() {
        assertParsed("\"simple\" string", "\"", "simple", "\"", " string");
        assertParsed("a{bc}d", "a", "{", "bc", "}", "d");
        assertParsed("a { b c } d", "a ", "{", " b c ", "}", " d");
        assertParsed("a { b c } d", "a ", "{", " b c ", "}", " d");
    }

    @Test
    public void shouldConvertTimeValues() {
        java.util.Date current = new java.util.Date();
        long currentMillis = current.getTime() % MILLIS_PER_DAY;

        // java.util.Date - just copy
        java.util.Date t1 = Values.convertToTime(Time.SCHEMA, current);
        assertEquals(current, t1);

        // java.util.Date as a Timestamp - discard the date and keep just day's milliseconds
        t1 = Values.convertToTime(Timestamp.SCHEMA, current);
        assertEquals(new java.util.Date(currentMillis), t1);

        // ISO8601 strings - currently broken because tokenization breaks at colon

        // Millis as string
        java.util.Date t3 = Values.convertToTime(Time.SCHEMA, Long.toString(currentMillis));
        assertEquals(currentMillis, t3.getTime());

        // Millis as long
        java.util.Date t4 = Values.convertToTime(Time.SCHEMA, currentMillis);
        assertEquals(currentMillis, t4.getTime());
    }

    @Test
    public void shouldConvertDateValues() {
        java.util.Date current = new java.util.Date();
        long currentMillis = current.getTime() % MILLIS_PER_DAY;
        long days = current.getTime() / MILLIS_PER_DAY;

        // java.util.Date - just copy
        java.util.Date d1 = Values.convertToDate(Date.SCHEMA, current);
        assertEquals(current, d1);

        // java.util.Date as a Timestamp - discard the day's milliseconds and keep the date
        java.util.Date currentDate = new java.util.Date(current.getTime() - currentMillis);
        d1 = Values.convertToDate(Timestamp.SCHEMA, currentDate);
        assertEquals(currentDate, d1);

        // ISO8601 strings - currently broken because tokenization breaks at colon

        // Days as string
        java.util.Date d3 = Values.convertToDate(Date.SCHEMA, Long.toString(days));
        assertEquals(currentDate, d3);

        // Days as long
        java.util.Date d4 = Values.convertToDate(Date.SCHEMA, days);
        assertEquals(currentDate, d4);
    }

    @Test
    public void shouldConvertTimestampValues() {
        java.util.Date current = new java.util.Date();
        long currentMillis = current.getTime() % MILLIS_PER_DAY;

        // java.util.Date - just copy
        java.util.Date ts1 = Values.convertToTimestamp(Timestamp.SCHEMA, current);
        assertEquals(current, ts1);

        // java.util.Date as a Timestamp - discard the day's milliseconds and keep the date
        java.util.Date currentDate = new java.util.Date(current.getTime() - currentMillis);
        ts1 = Values.convertToTimestamp(Date.SCHEMA, currentDate);
        assertEquals(currentDate, ts1);

        // java.util.Date as a Time - discard the date and keep the day's milliseconds
        ts1 = Values.convertToTimestamp(Time.SCHEMA, currentMillis);
        assertEquals(new java.util.Date(currentMillis), ts1);

        // ISO8601 strings - currently broken because tokenization breaks at colon

        // Millis as string
        java.util.Date ts3 = Values.convertToTimestamp(Timestamp.SCHEMA, Long.toString(current.getTime()));
        assertEquals(current, ts3);

        // Millis as long
        java.util.Date ts4 = Values.convertToTimestamp(Timestamp.SCHEMA, current.getTime());
        assertEquals(current, ts4);
    }

    @Test
    public void shouldConvertDecimalValues() {
        // Various forms of the same number should all be parsed to the same BigDecimal
        Number number = 1.0f;
        String string = number.toString();
        BigDecimal value = new BigDecimal(string);
        byte[] bytes = Decimal.fromLogical(Decimal.schema(1), value);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        assertEquals(value, Values.convertToDecimal(null, number, 1));
        assertEquals(value, Values.convertToDecimal(null, string, 1));
        assertEquals(value, Values.convertToDecimal(null, value, 1));
        assertEquals(value, Values.convertToDecimal(null, bytes, 1));
        assertEquals(value, Values.convertToDecimal(null, buffer, 1));
    }

    @Test
    public void shouldFailToConvertNullToDecimal() {
        assertThrows(DataException.class, () -> Values.convertToDecimal(null, null, 1));
    }

    @Test
    public void shouldInferByteSchema() {
        byte[] bytes = new byte[1];
        Schema byteSchema = Values.inferSchema(bytes);
        assertEquals(Schema.BYTES_SCHEMA, byteSchema);
        Schema byteBufferSchema = Values.inferSchema(ByteBuffer.wrap(bytes));
        assertEquals(Schema.BYTES_SCHEMA, byteBufferSchema);
    }

    @Test
    public void shouldInferStructSchema() {
        Struct struct = new Struct(SchemaBuilder.struct().build());
        Schema structSchema = Values.inferSchema(struct);
        assertEquals(struct.schema(), structSchema);
    }

    @Test
    public void shouldInferNoSchemaForEmptyList() {
        Schema listSchema = Values.inferSchema(Collections.emptyList());
        assertNull(listSchema);
    }

    @Test
    public void shouldInferNoSchemaForListContainingObject() {
        Schema listSchema = Values.inferSchema(Collections.singletonList(new Object()));
        assertNull(listSchema);
    }

    @Test
    public void shouldInferNoSchemaForEmptyMap() {
        Schema listSchema = Values.inferSchema(Collections.emptyMap());
        assertNull(listSchema);
    }

    @Test
    public void shouldInferNoSchemaForMapContainingObject() {
        Schema listSchema = Values.inferSchema(Collections.singletonMap(new Object(), new Object()));
        assertNull(listSchema);
    }

    /**
     * Test parsing distinct number-like types (strings containing numbers, and logical Decimals) in the same list
     * The parser does not convert Numbers to Decimals, or Strings containing numbers to Numbers automatically.
     */
    @Test
    public void shouldNotConvertArrayValuesToDecimal() {
        List<Object> decimals = Arrays.asList("\"1.0\"", BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE),
                BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE), (byte) 1, (byte) 1);
        List<Object> expected = new ArrayList<>(decimals); // most values are directly reproduced with the same type
        expected.set(0, "1.0"); // The quotes are parsed away, but the value remains a string
        SchemaAndValue schemaAndValue = Values.parseString(decimals.toString());
        Schema schema = schemaAndValue.schema();
        assertEquals(Type.ARRAY, schema.type());
        assertNull(schema.valueSchema());
        assertEquals(expected, schemaAndValue.value());
    }

    @Test
    public void shouldParseArrayOfOnlyDecimals() {
        List<Object> decimals = Arrays.asList(BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE),
                BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE));
        SchemaAndValue schemaAndValue = Values.parseString(decimals.toString());
        Schema schema = schemaAndValue.schema();
        assertEquals(Type.ARRAY, schema.type());
        assertEquals(Decimal.schema(0), schema.valueSchema());
        assertEquals(decimals, schemaAndValue.value());
    }

    @Test
    public void canConsume() {
    }

    @Test
    public void shouldParseBigIntegerAsDecimalWithZeroScale() {
        BigInteger value = BigInteger.valueOf(Long.MAX_VALUE).add(new BigInteger("1"));
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Decimal.schema(0), schemaAndValue.schema());
        assertInstanceOf(BigDecimal.class, schemaAndValue.value());
        assertEquals(value, ((BigDecimal) schemaAndValue.value()).unscaledValue());
        value = BigInteger.valueOf(Long.MIN_VALUE).subtract(new BigInteger("1"));
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Decimal.schema(0), schemaAndValue.schema());
        assertInstanceOf(BigDecimal.class, schemaAndValue.value());
        assertEquals(value, ((BigDecimal) schemaAndValue.value()).unscaledValue());
    }

    @Test
    public void shouldParseByteAsInt8() {
        Byte value = Byte.MAX_VALUE;
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT8_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Byte.class, schemaAndValue.value());
        assertEquals(value.byteValue(), ((Byte) schemaAndValue.value()).byteValue());
        value = Byte.MIN_VALUE;
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT8_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Byte.class, schemaAndValue.value());
        assertEquals(value.byteValue(), ((Byte) schemaAndValue.value()).byteValue());
    }

    @Test
    public void shouldParseShortAsInt16() {
        Short value = Short.MAX_VALUE;
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT16_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Short.class, schemaAndValue.value());
        assertEquals(value.shortValue(), ((Short) schemaAndValue.value()).shortValue());
        value = Short.MIN_VALUE;
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT16_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Short.class, schemaAndValue.value());
        assertEquals(value.shortValue(), ((Short) schemaAndValue.value()).shortValue());
    }

    @Test
    public void shouldParseIntegerAsInt32() {
        Integer value = Integer.MAX_VALUE;
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT32_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Integer.class, schemaAndValue.value());
        assertEquals(value.intValue(), ((Integer) schemaAndValue.value()).intValue());
        value = Integer.MIN_VALUE;
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT32_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Integer.class, schemaAndValue.value());
        assertEquals(value.intValue(), ((Integer) schemaAndValue.value()).intValue());
    }

    @Test
    public void shouldParseLongAsInt64() {
        Long value = Long.MAX_VALUE;
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT64_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Long.class, schemaAndValue.value());
        assertEquals(value.longValue(), ((Long) schemaAndValue.value()).longValue());
        value = Long.MIN_VALUE;
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.INT64_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Long.class, schemaAndValue.value());
        assertEquals(value.longValue(), ((Long) schemaAndValue.value()).longValue());
    }

    @Test
    public void shouldParseFloatAsFloat32() {
        Float value = Float.MAX_VALUE;
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.FLOAT32_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Float.class, schemaAndValue.value());
        assertEquals(value, (Float) schemaAndValue.value(), 0);
        value = -Float.MAX_VALUE;
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.FLOAT32_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Float.class, schemaAndValue.value());
        assertEquals(value, (Float) schemaAndValue.value(), 0);
    }

    @Test
    public void shouldParseDoubleAsFloat64() {
        Double value = Double.MAX_VALUE;
        SchemaAndValue schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.FLOAT64_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Double.class, schemaAndValue.value());
        assertEquals(value, (Double) schemaAndValue.value(), 0);
        value = -Double.MAX_VALUE;
        schemaAndValue = Values.parseString(
            String.valueOf(value)
        );
        assertEquals(Schema.FLOAT64_SCHEMA, schemaAndValue.schema());
        assertInstanceOf(Double.class, schemaAndValue.value());
        assertEquals(value, (Double) schemaAndValue.value(), 0);
    }

    @Test
    public void shouldParseFractionalPartsAsIntegerWhenNoFractionalPart() {
        assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 1), Values.parseString("1.0"));
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 1.1f), Values.parseString("1.1"));
        assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 300), Values.parseString("300.0"));
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 300.01f), Values.parseString("300.01"));
        assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 66000), Values.parseString("66000.0"));
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 66000.0008f), Values.parseString("66000.0008"));
    }

    protected void assertParsed(String input) {
        assertParsed(input, input);
    }

    protected void assertParsed(String input, String... expectedTokens) {
        Parser parser = new Parser(input);
        if (!parser.hasNext()) {
            assertEquals(1, expectedTokens.length);
            assertTrue(expectedTokens[0].isEmpty());
            return;
        }

        for (String expectedToken : expectedTokens) {
            assertTrue(parser.hasNext());
            int position = parser.mark();
            assertEquals(expectedToken, parser.next());
            assertEquals(position + expectedToken.length(), parser.position());
            assertEquals(expectedToken, parser.previous());
            parser.rewindTo(position);
            assertEquals(position, parser.position());
            assertEquals(expectedToken, parser.next());
            int newPosition = parser.mark();
            assertEquals(position + expectedToken.length(), newPosition);
            assertEquals(expectedToken, parser.previous());
        }
        assertFalse(parser.hasNext());

        // Rewind and try consuming expected tokens ...
        parser.rewindTo(0);
        assertConsumable(parser, expectedTokens);

        // Parse again and try consuming expected tokens ...
        parser = new Parser(input);
        assertConsumable(parser, expectedTokens);
    }

    protected void assertConsumable(Parser parser, String... expectedTokens) {
        for (String expectedToken : expectedTokens) {
            if (!Utils.isBlank(expectedToken)) {
                int position = parser.mark();
                assertTrue(parser.canConsume(expectedToken.trim()));
                parser.rewindTo(position);
                assertTrue(parser.canConsume(expectedToken.trim(), true));
                parser.rewindTo(position);
                assertTrue(parser.canConsume(expectedToken, false));
            }
        }
    }

    protected SchemaAndValue roundTrip(Schema desiredSchema, String currentValue) {
        return roundTrip(desiredSchema, new SchemaAndValue(Schema.STRING_SCHEMA, currentValue));
    }

    protected SchemaAndValue roundTrip(Schema desiredSchema, SchemaAndValue input) {
        String serialized = input != null ? Values.convertToString(input.schema(), input.value()) : null;
        if (input != null && input.value() != null) {
            assertNotNull(serialized);
        }
        if (desiredSchema == null) {
            desiredSchema = Values.inferSchema(input);
            assertNotNull(desiredSchema);
        }
        return convertTo(desiredSchema, serialized);
    }

    protected SchemaAndValue convertTo(Schema desiredSchema, Object value) {
        Object newValue = null;
        switch (desiredSchema.type()) {
            case STRING:
                newValue = Values.convertToString(Schema.STRING_SCHEMA, value);
                break;
            case INT8:
                newValue = Values.convertToByte(Schema.STRING_SCHEMA, value);
                break;
            case INT16:
                newValue = Values.convertToShort(Schema.STRING_SCHEMA, value);
                break;
            case INT32:
                newValue = Values.convertToInteger(Schema.STRING_SCHEMA, value);
                break;
            case INT64:
                newValue = Values.convertToLong(Schema.STRING_SCHEMA, value);
                break;
            case FLOAT32:
                newValue = Values.convertToFloat(Schema.STRING_SCHEMA, value);
                break;
            case FLOAT64:
                newValue = Values.convertToDouble(Schema.STRING_SCHEMA, value);
                break;
            case BOOLEAN:
                newValue = Values.convertToBoolean(Schema.STRING_SCHEMA, value);
                break;
            case ARRAY:
                newValue = Values.convertToList(Schema.STRING_SCHEMA, value);
                break;
            case MAP:
                newValue = Values.convertToMap(Schema.STRING_SCHEMA, value);
                break;
            case STRUCT:
            case BYTES:
                fail("unexpected schema type");
                break;
        }
        Schema newSchema = Values.inferSchema(newValue);
        return new SchemaAndValue(newSchema, newValue);
    }

    protected void assertRoundTrip(Schema schema, String value) {
        assertRoundTrip(schema, Schema.STRING_SCHEMA, value);
    }

    protected void assertRoundTrip(Schema schema, Schema currentSchema, Object value) {
        SchemaAndValue result = roundTrip(schema, new SchemaAndValue(currentSchema, value));

        if (value == null) {
            assertNull(result.schema());
            assertNull(result.value());
        } else {
            assertEquals(value, result.value());
            assertEquals(schema, result.schema());

            SchemaAndValue result2 = roundTrip(result.schema(), result);
            assertEquals(schema, result2.schema());
            assertEquals(value, result2.value());
            assertEquals(result, result2);
        }
    }

    protected void assertShortCircuit(Schema schema, Object value) {
        SchemaAndValue result = convertTo(schema, value);

        if (value == null) {
            assertNull(result.schema());
            assertNull(result.value());
        } else {
            assertEquals(value, result.value());
            assertEquals(schema, result.schema());
        }
    }
}
