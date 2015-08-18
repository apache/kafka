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
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonConverterTest {

    ObjectMapper objectMapper = new ObjectMapper();
    JsonConverter converter = new JsonConverter();

    @Test
    public void booleanToCopycat() {
        assertEquals(true, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }")));
        assertEquals(false, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"boolean\" }, \"payload\": false }")));
    }

    @Test
    public void intToCopycat() {
        assertEquals(12, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"int\" }, \"payload\": 12 }")));
    }

    @Test
    public void longToCopycat() {
        assertEquals(12L, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"long\" }, \"payload\": 12 }")));
        assertEquals(4398046511104L, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"long\" }, \"payload\": 4398046511104 }")));
    }

    @Test
    public void floatToCopycat() {
        assertEquals(12.34f, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"float\" }, \"payload\": 12.34 }")));
    }

    @Test
    public void doubleToCopycat() {
        assertEquals(12.34, converter.toCopycatData(parse("{ \"schema\": { \"type\": \"double\" }, \"payload\": 12.34 }")));
    }


    @Test
    public void bytesToCopycat() throws UnsupportedEncodingException {
        ByteBuffer reference = ByteBuffer.wrap("test-string".getBytes("UTF-8"));
        String msg = "{ \"schema\": { \"type\": \"bytes\" }, \"payload\": \"dGVzdC1zdHJpbmc=\" }";
        ByteBuffer converted = ByteBuffer.wrap((byte[]) converter.toCopycatData(parse(msg)));
        assertEquals(reference, converted);
    }

    @Test
    public void stringToCopycat() {
        assertEquals("foo-bar-baz", converter.toCopycatData(parse("{ \"schema\": { \"type\": \"string\" }, \"payload\": \"foo-bar-baz\" }")));
    }

    @Test
    public void arrayToCopycat() {
        JsonNode arrayJson = parse("{ \"schema\": { \"type\": \"array\", \"items\": { \"type\" : \"int\" } }, \"payload\": [1, 2, 3] }");
        assertEquals(Arrays.asList(1, 2, 3), converter.toCopycatData(arrayJson));
    }


    @Test
    public void booleanToJson() {
        JsonNode converted = converter.fromCopycatData(true);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());
    }

    @Test
    public void intToJson() {
        JsonNode converted = converter.fromCopycatData(12);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void longToJson() {
        JsonNode converted = converter.fromCopycatData(4398046511104L);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"long\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(4398046511104L, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).longValue());
    }

    @Test
    public void floatToJson() {
        JsonNode converted = converter.fromCopycatData(12.34f);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"float\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12.34f, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).floatValue(), 0.001);
    }

    @Test
    public void doubleToJson() {
        JsonNode converted = converter.fromCopycatData(12.34);
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"double\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12.34, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).doubleValue(), 0.001);
    }

    @Test
    public void bytesToJson() throws IOException {
        JsonNode converted = converter.fromCopycatData("test-string".getBytes());
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"bytes\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(ByteBuffer.wrap("test-string".getBytes()),
                ByteBuffer.wrap(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).binaryValue()));
    }

    @Test
    public void stringToJson() {
        JsonNode converted = converter.fromCopycatData("test-string");
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"string\" }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals("test-string", converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).textValue());
    }

    @Test
    public void arrayToJson() {
        JsonNode converted = converter.fromCopycatData(Arrays.asList(1, 2, 3));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"array\", \"items\": { \"type\": \"int\" } }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(JsonNodeFactory.instance.arrayNode().add(1).add(2).add(3),
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
