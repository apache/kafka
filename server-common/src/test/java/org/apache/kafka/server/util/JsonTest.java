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

package org.apache.kafka.server.util;

import org.apache.kafka.server.util.json.DecodeJson;
import org.apache.kafka.server.util.json.JsonObject;
import org.apache.kafka.server.util.json.JsonValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonTest {
    private static final String JSON = "{\n" +
            "  \"boolean\": false,\n" +
            "  \"int\": 1234,\n" +
            "  \"long\": 3000000000,\n" +
            "  \"double\": 16.244355,\n" +
            "  \"string\": \"string\",\n" +
            "  \"number_as_string\": \"123\",\n" +
            "  \"array\": [4.0, 11.1, 44.5],\n" +
            "  \"object\": {\n" +
            "    \"a\": true,\n" +
            "    \"b\": false\n" +
            "  },\n" +
            "  \"null\": null\n" +
            "}";

    private JsonValue parse(String s) {
        return Json.parseFull(s).orElseThrow(() -> new RuntimeException("Failed to parse json: " + s));
    }

    @Test
    public void testAsJsonObject() throws JsonProcessingException {
        JsonObject parsed = parse(JSON).asJsonObject();
        JsonValue obj = parsed.apply("object");
        assertEquals(obj, obj.asJsonObject());
        assertThrows(JsonMappingException.class, () -> parsed.apply("array").asJsonObject());
    }

    @Test
    public void testAsJsonObjectOption() throws JsonProcessingException {
        JsonObject parsed = parse(JSON).asJsonObject();
        assertTrue(parsed.apply("object").asJsonObjectOptional().isPresent());
        assertEquals(Optional.empty(), parsed.apply("array").asJsonObjectOptional());
    }

    @Test
    public void testAsJsonArray() throws JsonProcessingException {
        JsonObject parsed = parse(JSON).asJsonObject();
        JsonValue array = parsed.apply("array");
        assertEquals(array, array.asJsonArray());
        assertThrows(JsonMappingException.class, () -> parsed.apply("object").asJsonArray());
    }

    @Test
    public void testAsJsonArrayOption() throws JsonProcessingException {
        JsonObject parsed = parse(JSON).asJsonObject();
        assertTrue(parsed.apply("array").asJsonArrayOptional().isPresent());
        assertEquals(Optional.empty(), parsed.apply("object").asJsonArrayOptional());
    }

    @Test
    public void testJsonObjectGet() throws JsonProcessingException {
        JsonObject parsed = parse(JSON).asJsonObject();
        assertEquals(Optional.of(parse("{\"a\":true,\"b\":false}")), parsed.get("object"));
        assertEquals(Optional.empty(), parsed.get("aaaaa"));
    }

    @Test
    public void testJsonObjectApply() throws JsonProcessingException {
        JsonObject parsed = parse(JSON).asJsonObject();
        assertEquals(parse("{\"a\":true,\"b\":false}"), parsed.apply("object"));
        assertThrows(JsonMappingException.class, () -> parsed.apply("aaaaaaaa"));
    }

    @Test
    public void testJsonObjectIterator() throws JsonProcessingException {
        List<Map.Entry<String, JsonValue>> results = new ArrayList<>();
        parse(JSON).asJsonObject().apply("object").asJsonObject().iterator().forEachRemaining(results::add);

        Map.Entry<String, JsonValue> entryA = new AbstractMap.SimpleEntry<>("a", parse("true"));
        AbstractMap.SimpleEntry<String, JsonValue> entryB = new AbstractMap.SimpleEntry<>("b", parse("false"));
        List<Map.Entry<String, JsonValue>> expectedResult = Arrays.asList(entryA, entryB);

        assertEquals(expectedResult, results);
    }

    @Test
    public void testJsonArrayIterator() throws JsonProcessingException {
        List<JsonValue> results = new ArrayList<>();
        parse(JSON).asJsonObject().apply("array").asJsonArray().iterator().forEachRemaining(results::add);

        List<JsonValue> expected = Stream.of("4.0", "11.1", "44.5")
                .map(this::parse)
                .toList();
        assertEquals(expected, results);
    }

    @Test
    public void testJsonValueEquals() {
        assertEquals(parse(JSON), parse(JSON));
        assertEquals(parse("{\"blue\": true, \"red\": false}"), parse("{\"red\": false, \"blue\": true}"));
        assertNotEquals(parse("{\"blue\": true, \"red\": true}"), parse("{\"red\": false, \"blue\": true}"));

        assertEquals(parse("[1, 2, 3]"), parse("[1, 2, 3]"));
        assertNotEquals(parse("[1, 2, 3]"), parse("[2, 1, 3]"));

        assertEquals(parse("1344"), parse("1344"));
        assertNotEquals(parse("1344"), parse("144"));
    }

    @Test
    public void testJsonValueHashCode() throws JsonProcessingException {
        assertEquals(new ObjectMapper().readTree(JSON).hashCode(), parse(JSON).hashCode());
    }

    @Test
    public void testJsonValueToString() {
        String js = "{\"boolean\":false,\"int\":1234,\"array\":[4.0,11.1,44.5],\"object\":{\"a\":true,\"b\":false}}";
        assertEquals(js, parse(js).toString());
    }

    @Test
    public void testDecodeBoolean() throws JsonMappingException {
        DecodeJson.DecodeBoolean decodeJson = new DecodeJson.DecodeBoolean();
        assertTo(false, decodeJson, jsonObject -> jsonObject.get("boolean").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("int").get());
    }

    @Test
    public void testDecodeString() throws JsonMappingException {
        DecodeJson.DecodeString decodeJson = new DecodeJson.DecodeString();
        assertTo("string", decodeJson, jsonObject -> jsonObject.get("string").get());
        assertTo("123", decodeJson, jsonObject -> jsonObject.get("number_as_string").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("int").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("array").get());
    }

    @Test
    public void testDecodeInt() throws JsonMappingException {
        DecodeJson.DecodeInteger decodeJson = new DecodeJson.DecodeInteger();
        assertTo(1234, decodeJson, jsonObject -> jsonObject.get("int").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("long").get());
    }

    @Test
    public void testDecodeLong() throws JsonMappingException {
        DecodeJson.DecodeLong decodeJson = new DecodeJson.DecodeLong();
        assertTo(3000000000L, decodeJson, jsonObject -> jsonObject.get("long").get());
        assertTo(1234L, decodeJson, jsonObject -> jsonObject.get("int").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("string").get());
    }

    @Test
    public void testDecodeDouble() throws JsonMappingException {
        DecodeJson.DecodeDouble decodeJson = new DecodeJson.DecodeDouble();
        assertTo(16.244355, decodeJson, jsonObject -> jsonObject.get("double").get());
        assertTo(1234.0, decodeJson, jsonObject -> jsonObject.get("int").get());
        assertTo(3000000000.0, decodeJson, jsonObject -> jsonObject.get("long").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("string").get());
    }

    @Test
    public void testDecodeSeq() throws JsonMappingException {
        DecodeJson<List<Double>> decodeJson = DecodeJson.decodeList(new DecodeJson.DecodeDouble());
        assertTo(Arrays.asList(4.0, 11.1, 44.5), decodeJson, jsonObject -> jsonObject.get("array").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("string").get());
        assertToFails(decodeJson, jsonObject -> jsonObject.get("object").get());
        assertToFails(DecodeJson.decodeList(new DecodeJson.DecodeString()), jsonObject -> jsonObject.get("array").get());
    }

    @Test
    public void testDecodeMap() throws JsonMappingException {
        DecodeJson<Map<String, Boolean>> decodeJson = DecodeJson.decodeMap(new DecodeJson.DecodeBoolean());
        HashMap<String, Boolean> stringBooleanMap = new HashMap<>();
        stringBooleanMap.put("a", true);
        stringBooleanMap.put("b", false);
        assertTo(stringBooleanMap, decodeJson, jsonObject -> jsonObject.get("object").get());
        assertToFails(DecodeJson.decodeMap(new DecodeJson.DecodeInteger()), jsonObject -> jsonObject.get("object").get());
        assertToFails(DecodeJson.decodeMap(new DecodeJson.DecodeString()), jsonObject -> jsonObject.get("object").get());
        assertToFails(DecodeJson.decodeMap(new DecodeJson.DecodeDouble()), jsonObject -> jsonObject.get("array").get());
    }

    @Test
    public void testDecodeOptional() throws JsonMappingException {
        DecodeJson<Optional<Integer>> decodeJson = DecodeJson.decodeOptional(new DecodeJson.DecodeInteger());
        assertTo(Optional.empty(), decodeJson, jsonObject -> jsonObject.get("null").get());
        assertTo(Optional.of(1234), decodeJson, jsonObject -> jsonObject.get("int").get());
        assertToFails(DecodeJson.decodeOptional(new DecodeJson.DecodeString()), jsonObject -> jsonObject.get("int").get());
    }
    private <T> void assertTo(T expected, DecodeJson<T> decodeJson, Function<JsonObject, JsonValue> jsonValue) throws JsonMappingException {
        JsonValue parsed = jsonValue.apply(parse(JSON).asJsonObject());
        assertEquals(expected, parsed.to(decodeJson));
    }

    private <T> void assertToFails(DecodeJson<T> decoder, Function<JsonObject, JsonValue> jsonValue) throws JsonMappingException {
        JsonObject obj = parse(JSON).asJsonObject();
        JsonValue parsed = jsonValue.apply(obj);
        assertThrows(JsonMappingException.class, () -> parsed.to(decoder));
    }
}
