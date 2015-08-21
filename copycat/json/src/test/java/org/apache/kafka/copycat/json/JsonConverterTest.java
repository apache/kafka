/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.copycat.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.copycat.data.Schema;
import org.apache.kafka.copycat.data.SchemaAndValue;
import org.apache.kafka.copycat.data.SchemaBuilder;
import org.apache.kafka.copycat.data.Struct;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonConverterTest {

    ObjectMapper objectMapper = new ObjectMapper();
    JsonConverter converter = new JsonConverter();

    // Schema metadata

    @Test
    public void testCopycatSchemaMetadataTranslation() {
        // this validates the non-type fields are translated and handled properly
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }")));
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, null), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\", \"optional\": true }, \"payload\": null }")));
        assertEquals(new SchemaAndValue(SchemaBuilder.bool().defaultValue(true).build(), true),
                converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\", \"default\": true }, \"payload\": null }")));
        assertEquals(new SchemaAndValue(SchemaBuilder.bool().required().name("bool").version("versionnum".getBytes()).doc("the documentation").build(), true),
                converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\", \"optional\": false, \"name\": \"bool\", \"version\": \"" + JsonNodeFactory.instance.binaryNode("versionnum".getBytes()).asText() + "\", \"doc\": \"the documentation\"}, \"payload\": true }")));
    }

    // Schema types

    @Test
    public void booleanToCopycat() {
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }")));
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\" }, \"payload\": false }")));
    }

    @Test
    public void byteToCopycat() {
        assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 12), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"int8\" }, \"payload\": 12 }")));
    }

    @Test
    public void shortToCopycat() {
        assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 12), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"int16\" }, \"payload\": 12 }")));
    }

    @Test
    public void intToCopycat() {
        assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 12), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"int32\" }, \"payload\": 12 }")));
    }

    @Test
    public void longToCopycat() {
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 12L), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"int64\" }, \"payload\": 12 }")));
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 4398046511104L), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"int64\" }, \"payload\": 4398046511104 }")));
    }

    @Test
    public void floatToCopycat() {
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.34f), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"float\" }, \"payload\": 12.34 }")));
    }

    @Test
    public void doubleToCopycat() {
        assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.34), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"double\" }, \"payload\": 12.34 }")));
    }


    @Test
    public void bytesToCopycat() throws UnsupportedEncodingException {
        ByteBuffer reference = ByteBuffer.wrap("test-string".getBytes("UTF-8"));
        String msg = "{ \"schema\": { \"type\": \"bytes\" }, \"payload\": \"dGVzdC1zdHJpbmc=\" }";
        SchemaAndValue schemaAndValue = converter.toCopycatData(parse(msg));
        ByteBuffer converted = ByteBuffer.wrap((byte[]) schemaAndValue.getValue());
        assertEquals(reference, converted);
    }

    @Test
    public void stringToCopycat() {
        assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "foo-bar-baz"), converter.toCopycatData(parse("{ \"schema\": { \"type\": \"string\" }, \"payload\": \"foo-bar-baz\" }")));
    }

    @Test
    public void arrayToCopycat() {
        JsonNode arrayJson = parse("{ \"schema\": { \"type\": \"array\", \"items\": { \"type\" : \"int32\" } }, \"payload\": [1, 2, 3] }");
        assertEquals(new SchemaAndValue(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Arrays.asList(1, 2, 3)), converter.toCopycatData(arrayJson));
    }

    @Test
    public void mapToCopycatStringKeys() {
        JsonNode mapJson = parse("{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"string\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": { \"key1\": 12, \"key2\": 15} }");
        Map<String, Integer> expected = new HashMap<>();
        expected.put("key1", 12);
        expected.put("key2", 15);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), expected), converter.toCopycatData(mapJson));
    }

    @Test
    public void mapToCopycatNonStringKeys() {
        JsonNode mapJson = parse("{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"int32\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": [ [1, 12], [2, 15] ] }");
        Map<Integer, Integer> expected = new HashMap<>();
        expected.put(1, 12);
        expected.put(2, 15);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(), expected), converter.toCopycatData(mapJson));
    }

    @Test
    public void structToCopycat() {
        JsonNode structJson = parse("{ \"schema\": { \"type\": \"struct\", \"fields\": [{ \"field\": \"field1\", \"type\": \"boolean\" }, { \"field\": \"field2\", \"type\": \"string\" }] }, \"payload\": { \"field1\": true, \"field2\": \"string\" } }");
        Schema expectedSchema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).build();
        Struct expected = new Struct(expectedSchema).put("field1", true).put("field2", "string");
        SchemaAndValue converted = converter.toCopycatData(structJson);
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    // Schema metadata

    @Test
    public void testJsonSchemaMetadataTranslation() {
        JsonNode converted = converter.fromCopycatData(Schema.BOOLEAN_SCHEMA, true);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());

        converted = converter.fromCopycatData(Schema.OPTIONAL_BOOLEAN_SCHEMA, null);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertTrue(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).isNull());

        converted = converter.fromCopycatData(SchemaBuilder.bool().defaultValue(true).build(), true);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false, \"default\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());

        // Irritatingly, using a BinaryNode does not work with equals() if you parse the JSON (resulting in a TextNode
        // since it's base64-encoded), so we handle that test separately
        converted = converter.fromCopycatData(SchemaBuilder.bool().required().name("bool").doc("the documentation").build(), true);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false, \"name\": \"bool\", \"doc\": \"the documentation\"}"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());

        // Test specifically targeting the version field
        converted = converter.fromCopycatData(SchemaBuilder.bool().version("versionnum".getBytes()).build(), true);
        validateEnvelope(converted);
        assertEquals(JsonNodeFactory.instance.binaryNode("versionnum".getBytes()),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).get("version"));
    }

    // Schema types

    @Test
    public void booleanToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.BOOLEAN_SCHEMA, true);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());
    }

    @Test
    public void byteToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.INT8_SCHEMA, (byte) 12);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int8\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void shortToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.INT16_SCHEMA, (short) 12);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int16\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void intToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.INT32_SCHEMA, 12);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void longToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.INT64_SCHEMA, 4398046511104L);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int64\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(4398046511104L, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).longValue());
    }

    @Test
    public void floatToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.FLOAT32_SCHEMA, 12.34f);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"float\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12.34f, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).floatValue(), 0.001);
    }

    @Test
    public void doubleToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.FLOAT64_SCHEMA, 12.34);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"double\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12.34, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).doubleValue(), 0.001);
    }

    @Test
    public void bytesToJson() throws IOException {
        JsonNode converted = converter.fromCopycatData(Schema.BYTES_SCHEMA, "test-string".getBytes());
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"bytes\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(ByteBuffer.wrap("test-string".getBytes()),
                ByteBuffer.wrap(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).binaryValue()));
    }

    @Test
    public void stringToJson() {
        JsonNode converted = converter.fromCopycatData(Schema.STRING_SCHEMA, "test-string");
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"string\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals("test-string", converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).textValue());
    }

    @Test
    public void arrayToJson() {
        Schema int32Array = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        JsonNode converted = converter.fromCopycatData(int32Array, Arrays.asList(1, 2, 3));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"array\", \"items\": { \"type\": \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(JsonNodeFactory.instance.arrayNode().add(1).add(2).add(3),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void mapToJsonStringKeys() {
        Schema stringIntMap = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build();
        Map<String, Integer> input = new HashMap<>();
        input.put("key1", 12);
        input.put("key2", 15);
        JsonNode converted = converter.fromCopycatData(stringIntMap, input);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"map\", \"keys\": { \"type\" : \"string\", \"optional\": false }, \"values\": { \"type\" : \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(JsonNodeFactory.instance.objectNode().put("key1", 12).put("key2", 15),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void mapToJsonNonStringKeys() {
        Schema intIntMap = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build();
        Map<Integer, Integer> input = new HashMap<>();
        input.put(1, 12);
        input.put(2, 15);
        JsonNode converted = converter.fromCopycatData(intIntMap, input);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"map\", \"keys\": { \"type\" : \"int32\", \"optional\": false }, \"values\": { \"type\" : \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(JsonNodeFactory.instance.arrayNode()
                        .add(JsonNodeFactory.instance.arrayNode().add(1).add(12))
                        .add(JsonNodeFactory.instance.arrayNode().add(2).add(15)),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void structToJson() {
        Schema schema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).build();
        Struct input = new Struct(schema).put("field1", true).put("field2", "string");
        JsonNode converted = converter.fromCopycatData(schema, input);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"struct\", \"optional\": false, \"fields\": [{ \"field\": \"field1\", \"type\": \"boolean\", \"optional\": false }, { \"field\": \"field2\", \"type\": \"string\", \"optional\": false }] }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(JsonNodeFactory.instance.objectNode()
                        .put("field1", true)
                        .put("field2", "string"),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    private JsonNode parse(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
    }

    private void validateEnvelope(JsonNode env) {
        assertNotNull(env);
        assertTrue(env.isObject());
        assertEquals(2, env.size());
        assertTrue(env.has(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertTrue(env.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).isObject());
        assertTrue(env.has(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }
}
