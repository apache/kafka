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

package org.apache.kafka.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonConverterTest {
    private static final String TOPIC = "topic";

    ObjectMapper objectMapper = new ObjectMapper();
    JsonConverter converter = new JsonConverter();

    @Before
    public void setUp() {
        converter.configure(Collections.EMPTY_MAP, false);
    }

    // Schema metadata

    @Test
    public void testConnectSchemaMetadataTranslation() {
        // this validates the non-type fields are translated and handled properly
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }".getBytes()));
        assertEquals(new SchemaAndValue(Schema.OPTIONAL_BOOLEAN_SCHEMA, null), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": true }, \"payload\": null }".getBytes()));
        assertEquals(new SchemaAndValue(SchemaBuilder.bool().defaultValue(true).build(), true),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"default\": true }, \"payload\": null }".getBytes()));
        assertEquals(new SchemaAndValue(SchemaBuilder.bool().required().name("bool").version(2).doc("the documentation").parameter("foo", "bar").build(), true),
                converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": false, \"name\": \"bool\", \"version\": 2, \"doc\": \"the documentation\", \"parameters\": { \"foo\": \"bar\" }}, \"payload\": true }".getBytes()));
    }

    // Schema types

    @Test
    public void booleanToConnect() {
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }".getBytes()));
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, false), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": false }".getBytes()));
    }

    @Test
    public void byteToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT8_SCHEMA, (byte) 12), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int8\" }, \"payload\": 12 }".getBytes()));
    }

    @Test
    public void shortToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT16_SCHEMA, (short) 12), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int16\" }, \"payload\": 12 }".getBytes()));
    }

    @Test
    public void intToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT32_SCHEMA, 12), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int32\" }, \"payload\": 12 }".getBytes()));
    }

    @Test
    public void longToConnect() {
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 12L), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int64\" }, \"payload\": 12 }".getBytes()));
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 4398046511104L), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"int64\" }, \"payload\": 4398046511104 }".getBytes()));
    }

    @Test
    public void floatToConnect() {
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.34f), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"float\" }, \"payload\": 12.34 }".getBytes()));
    }

    @Test
    public void doubleToConnect() {
        assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.34), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"double\" }, \"payload\": 12.34 }".getBytes()));
    }


    @Test
    public void bytesToConnect() throws UnsupportedEncodingException {
        ByteBuffer reference = ByteBuffer.wrap("test-string".getBytes("UTF-8"));
        String msg = "{ \"schema\": { \"type\": \"bytes\" }, \"payload\": \"dGVzdC1zdHJpbmc=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        ByteBuffer converted = ByteBuffer.wrap((byte[]) schemaAndValue.value());
        assertEquals(reference, converted);
    }

    @Test
    public void stringToConnect() {
        assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "foo-bar-baz"), converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"string\" }, \"payload\": \"foo-bar-baz\" }".getBytes()));
    }

    @Test
    public void arrayToConnect() {
        byte[] arrayJson = "{ \"schema\": { \"type\": \"array\", \"items\": { \"type\" : \"int32\" } }, \"payload\": [1, 2, 3] }".getBytes();
        assertEquals(new SchemaAndValue(SchemaBuilder.array(Schema.INT32_SCHEMA).build(), Arrays.asList(1, 2, 3)), converter.toConnectData(TOPIC, arrayJson));
    }

    @Test
    public void mapToConnectStringKeys() {
        byte[] mapJson = "{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"string\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": { \"key1\": 12, \"key2\": 15} }".getBytes();
        Map<String, Integer> expected = new HashMap<>();
        expected.put("key1", 12);
        expected.put("key2", 15);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build(), expected), converter.toConnectData(TOPIC, mapJson));
    }

    @Test
    public void mapToConnectNonStringKeys() {
        byte[] mapJson = "{ \"schema\": { \"type\": \"map\", \"keys\": { \"type\" : \"int32\" }, \"values\": { \"type\" : \"int32\" } }, \"payload\": [ [1, 12], [2, 15] ] }".getBytes();
        Map<Integer, Integer> expected = new HashMap<>();
        expected.put(1, 12);
        expected.put(2, 15);
        assertEquals(new SchemaAndValue(SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build(), expected), converter.toConnectData(TOPIC, mapJson));
    }

    @Test
    public void structToConnect() {
        byte[] structJson = "{ \"schema\": { \"type\": \"struct\", \"fields\": [{ \"field\": \"field1\", \"type\": \"boolean\" }, { \"field\": \"field2\", \"type\": \"string\" }] }, \"payload\": { \"field1\": true, \"field2\": \"string\" } }".getBytes();
        Schema expectedSchema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).build();
        Struct expected = new Struct(expectedSchema).put("field1", true).put("field2", "string");
        SchemaAndValue converted = converter.toConnectData(TOPIC, structJson);
        assertEquals(new SchemaAndValue(expectedSchema, expected), converted);
    }

    @Test(expected = DataException.class)
    public void nullToConnect() {
        // When schemas are enabled, trying to decode a null should be an error -- we should *always* have the envelope
        assertEquals(SchemaAndValue.NULL, converter.toConnectData(TOPIC, null));
    }

    @Test
    public void nullSchemaPrimitiveToConnect() {
        SchemaAndValue converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": null }".getBytes());
        assertEquals(SchemaAndValue.NULL, converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": true }".getBytes());
        assertEquals(new SchemaAndValue(null, true), converted);

        // Integers: Connect has more data types, and JSON unfortunately mixes all number types. We try to preserve
        // info as best we can, so we always use the largest integer and floating point numbers we can and have Jackson
        // determine if it's an integer or not
        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": 12 }".getBytes());
        assertEquals(new SchemaAndValue(null, 12L), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": 12.24 }".getBytes());
        assertEquals(new SchemaAndValue(null, 12.24), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": \"a string\" }".getBytes());
        assertEquals(new SchemaAndValue(null, "a string"), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": [1, \"2\", 3] }".getBytes());
        assertEquals(new SchemaAndValue(null, Arrays.asList(1L, "2", 3L)), converted);

        converted = converter.toConnectData(TOPIC, "{ \"schema\": null, \"payload\": { \"field1\": 1, \"field2\": 2} }".getBytes());
        Map<String, Long> obj = new HashMap<>();
        obj.put("field1", 1L);
        obj.put("field2", 2L);
        assertEquals(new SchemaAndValue(null, obj), converted);
    }

    @Test
    public void decimalToConnect() {
        Schema schema = Decimal.schema(2);
        BigDecimal reference = new BigDecimal(new BigInteger("156"), 2);
        // Payload is base64 encoded byte[]{0, -100}, which is the two's complement encoding of 156.
        String msg = "{ \"schema\": { \"type\": \"bytes\", \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }, \"payload\": \"AJw=\" }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        BigDecimal converted = (BigDecimal) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void dateToConnect() {
        Schema schema = Date.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DATE, 10000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Date\", \"version\": 1 }, \"payload\": 10000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void timeToConnect() {
        Schema schema = Time.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 14400000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int32\", \"name\": \"org.apache.kafka.connect.data.Time\", \"version\": 1 }, \"payload\": 14400000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    @Test
    public void timestampToConnect() {
        Schema schema = Timestamp.SCHEMA;
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 2000000000);
        calendar.add(Calendar.MILLISECOND, 2000000000);
        java.util.Date reference = calendar.getTime();
        String msg = "{ \"schema\": { \"type\": \"int64\", \"name\": \"org.apache.kafka.connect.data.Timestamp\", \"version\": 1 }, \"payload\": 4000000000 }";
        SchemaAndValue schemaAndValue = converter.toConnectData(TOPIC, msg.getBytes());
        java.util.Date converted = (java.util.Date) schemaAndValue.value();
        assertEquals(schema, schemaAndValue.schema());
        assertEquals(reference, converted);
    }

    // Schema metadata

    @Test
    public void testJsonSchemaMetadataTranslation() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, true));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());

        converted = parse(converter.fromConnectData(TOPIC, Schema.OPTIONAL_BOOLEAN_SCHEMA, null));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertTrue(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).isNull());

        converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().defaultValue(true).build(), true));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false, \"default\": true }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());

        converted = parse(converter.fromConnectData(TOPIC, SchemaBuilder.bool().required().name("bool").version(3).doc("the documentation").parameter("foo", "bar").build(), true));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false, \"name\": \"bool\", \"version\": 3, \"doc\": \"the documentation\", \"parameters\": { \"foo\": \"bar\" }}"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());
    }


    @Test
    public void testCacheSchemaToConnectConversion() {
        Cache<JsonNode, Schema> cache = Whitebox.getInternalState(converter, "toConnectSchemaCache");
        assertEquals(0, cache.size());

        converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }".getBytes());
        assertEquals(1, cache.size());

        converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\" }, \"payload\": true }".getBytes());
        assertEquals(1, cache.size());

        // Different schema should also get cached
        converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": true }, \"payload\": true }".getBytes());
        assertEquals(2, cache.size());

        // Even equivalent, but different JSON encoding of schema, should get different cache entry
        converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"boolean\", \"optional\": false }, \"payload\": true }".getBytes());
        assertEquals(3, cache.size());
    }

    // Schema types

    @Test
    public void booleanToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.BOOLEAN_SCHEMA, true));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"boolean\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());
    }

    @Test
    public void byteToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.INT8_SCHEMA, (byte) 12));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int8\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void shortToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.INT16_SCHEMA, (short) 12));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int16\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void intToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.INT32_SCHEMA, 12));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).intValue());
    }

    @Test
    public void longToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.INT64_SCHEMA, 4398046511104L));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int64\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(4398046511104L, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).longValue());
    }

    @Test
    public void floatToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.FLOAT32_SCHEMA, 12.34f));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"float\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12.34f, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).floatValue(), 0.001);
    }

    @Test
    public void doubleToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.FLOAT64_SCHEMA, 12.34));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"double\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(12.34, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).doubleValue(), 0.001);
    }

    @Test
    public void bytesToJson() throws IOException {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.BYTES_SCHEMA, "test-string".getBytes()));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"bytes\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(ByteBuffer.wrap("test-string".getBytes()),
                ByteBuffer.wrap(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).binaryValue()));
    }

    @Test
    public void stringToJson() {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Schema.STRING_SCHEMA, "test-string"));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"string\", \"optional\": false }"), converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals("test-string", converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).textValue());
    }

    @Test
    public void arrayToJson() {
        Schema int32Array = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        JsonNode converted = parse(converter.fromConnectData(TOPIC, int32Array, Arrays.asList(1, 2, 3)));
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
        JsonNode converted = parse(converter.fromConnectData(TOPIC, stringIntMap, input));
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
        JsonNode converted = parse(converter.fromConnectData(TOPIC, intIntMap, input));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"map\", \"keys\": { \"type\" : \"int32\", \"optional\": false }, \"values\": { \"type\" : \"int32\", \"optional\": false }, \"optional\": false }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));

        assertTrue(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).isArray());
        ArrayNode payload = (ArrayNode) converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(2, payload.size());
        Set<JsonNode> payloadEntries = new HashSet<>();
        for (JsonNode elem : payload)
            payloadEntries.add(elem);
        assertEquals(new HashSet<>(Arrays.asList(JsonNodeFactory.instance.arrayNode().add(1).add(12),
                        JsonNodeFactory.instance.arrayNode().add(2).add(15))),
                payloadEntries
        );
    }

    @Test
    public void structToJson() {
        Schema schema = SchemaBuilder.struct().field("field1", Schema.BOOLEAN_SCHEMA).field("field2", Schema.STRING_SCHEMA).field("field3", Schema.STRING_SCHEMA).field("field4", Schema.BOOLEAN_SCHEMA).build();
        Struct input = new Struct(schema).put("field1", true).put("field2", "string2").put("field3", "string3").put("field4", false);
        JsonNode converted = parse(converter.fromConnectData(TOPIC, schema, input));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"struct\", \"optional\": false, \"fields\": [{ \"field\": \"field1\", \"type\": \"boolean\", \"optional\": false }, { \"field\": \"field2\", \"type\": \"string\", \"optional\": false }, { \"field\": \"field3\", \"type\": \"string\", \"optional\": false }, { \"field\": \"field4\", \"type\": \"boolean\", \"optional\": false }] }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertEquals(JsonNodeFactory.instance.objectNode()
                        .put("field1", true)
                        .put("field2", "string2")
                        .put("field3", "string3")
                        .put("field4", false),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }


    @Test
    public void decimalToJson() throws IOException {
        JsonNode converted = parse(converter.fromConnectData(TOPIC, Decimal.schema(2), new BigDecimal(new BigInteger("156"), 2)));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"bytes\", \"optional\": false, \"name\": \"org.apache.kafka.connect.data.Decimal\", \"version\": 1, \"parameters\": { \"scale\": \"2\" } }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertArrayEquals(new byte[]{0, -100}, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).binaryValue());
    }

    @Test
    public void dateToJson() throws IOException {
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.DATE, 10000);
        java.util.Date date = calendar.getTime();

        JsonNode converted = parse(converter.fromConnectData(TOPIC, Date.SCHEMA, date));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": false, \"name\": \"org.apache.kafka.connect.data.Date\", \"version\": 1 }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        JsonNode payload = converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertTrue(payload.isInt());
        assertEquals(10000, payload.intValue());
    }

    @Test
    public void timeToJson() throws IOException {
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 14400000);
        java.util.Date date = calendar.getTime();

        JsonNode converted = parse(converter.fromConnectData(TOPIC, Time.SCHEMA, date));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int32\", \"optional\": false, \"name\": \"org.apache.kafka.connect.data.Time\", \"version\": 1 }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        JsonNode payload = converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertTrue(payload.isInt());
        assertEquals(14400000, payload.longValue());
    }

    @Test
    public void timestampToJson() throws IOException {
        GregorianCalendar calendar = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
        calendar.add(Calendar.MILLISECOND, 2000000000);
        calendar.add(Calendar.MILLISECOND, 2000000000);
        java.util.Date date = calendar.getTime();

        JsonNode converted = parse(converter.fromConnectData(TOPIC, Timestamp.SCHEMA, date));
        validateEnvelope(converted);
        assertEquals(parse("{ \"type\": \"int64\", \"optional\": false, \"name\": \"org.apache.kafka.connect.data.Timestamp\", \"version\": 1 }"),
                converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        JsonNode payload = converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertTrue(payload.isLong());
        assertEquals(4000000000L, payload.longValue());
    }


    @Test
    public void nullSchemaAndPrimitiveToJson() {
        // This still needs to do conversion of data, null schema means "anything goes"
        JsonNode converted = parse(converter.fromConnectData(TOPIC, null, true));
        validateEnvelopeNullSchema(converted);
        assertTrue(converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).isNull());
        assertEquals(true, converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).booleanValue());
    }

    @Test
    public void nullSchemaAndArrayToJson() {
        // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
        // types to verify conversion still works.
        JsonNode converted = parse(converter.fromConnectData(TOPIC, null, Arrays.asList(1, "string", true)));
        validateEnvelopeNullSchema(converted);
        assertTrue(converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).isNull());
        assertEquals(JsonNodeFactory.instance.arrayNode().add(1).add("string").add(true),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void nullSchemaAndMapToJson() {
        // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
        // types to verify conversion still works.
        Map<String, Object> input = new HashMap<>();
        input.put("key1", 12);
        input.put("key2", "string");
        input.put("key3", true);
        JsonNode converted = parse(converter.fromConnectData(TOPIC, null, input));
        validateEnvelopeNullSchema(converted);
        assertTrue(converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).isNull());
        assertEquals(JsonNodeFactory.instance.objectNode().put("key1", 12).put("key2", "string").put("key3", true),
                converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }

    @Test
    public void nullSchemaAndMapNonStringKeysToJson() {
        // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
        // types to verify conversion still works.
        Map<Object, Object> input = new HashMap<>();
        input.put("string", 12);
        input.put(52, "string");
        input.put(false, true);
        JsonNode converted = parse(converter.fromConnectData(TOPIC, null, input));
        validateEnvelopeNullSchema(converted);
        assertTrue(converted.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).isNull());
        assertTrue(converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME).isArray());
        ArrayNode payload = (ArrayNode) converted.get(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME);
        assertEquals(3, payload.size());
        Set<JsonNode> payloadEntries = new HashSet<>();
        for (JsonNode elem : payload)
            payloadEntries.add(elem);
        assertEquals(new HashSet<>(Arrays.asList(JsonNodeFactory.instance.arrayNode().add("string").add(12),
                        JsonNodeFactory.instance.arrayNode().add(52).add("string"),
                        JsonNodeFactory.instance.arrayNode().add(false).add(true))),
                payloadEntries
        );
    }


    @Test(expected = DataException.class)
    public void mismatchSchemaJson() {
        // If we have mismatching schema info, we should properly convert to a DataException
        converter.fromConnectData(TOPIC, Schema.FLOAT64_SCHEMA, true);
    }



    @Test
    public void noSchemaToConnect() {
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props, true);
        assertEquals(new SchemaAndValue(null, true), converter.toConnectData(TOPIC, "true".getBytes()));
    }

    @Test
    public void noSchemaToJson() {
        Map<String, Boolean> props = Collections.singletonMap("schemas.enable", false);
        converter.configure(props, true);
        JsonNode converted = parse(converter.fromConnectData(TOPIC, null, true));
        assertTrue(converted.isBoolean());
        assertEquals(true, converted.booleanValue());
    }

    @Test
    public void testCacheSchemaToJsonConversion() {
        Cache<Schema, ObjectNode> cache = Whitebox.getInternalState(converter, "fromConnectSchemaCache");
        assertEquals(0, cache.size());

        // Repeated conversion of the same schema, even if the schema object is different should return the same Java
        // object
        converter.fromConnectData(TOPIC, SchemaBuilder.bool().build(), true);
        assertEquals(1, cache.size());

        converter.fromConnectData(TOPIC, SchemaBuilder.bool().build(), true);
        assertEquals(1, cache.size());

        // Validate that a similar, but different schema correctly returns a different schema.
        converter.fromConnectData(TOPIC, SchemaBuilder.bool().optional().build(), true);
        assertEquals(2, cache.size());
    }


    private JsonNode parse(byte[] json) {
        try {
            return objectMapper.readTree(json);
        } catch (IOException e) {
            fail("IOException during JSON parse: " + e.getMessage());
            throw new RuntimeException("failed");
        }
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

    private void validateEnvelopeNullSchema(JsonNode env) {
        assertNotNull(env);
        assertTrue(env.isObject());
        assertEquals(2, env.size());
        assertTrue(env.has(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME));
        assertTrue(env.get(JsonSchema.ENVELOPE_SCHEMA_FIELD_NAME).isNull());
        assertTrue(env.has(JsonSchema.ENVELOPE_PAYLOAD_FIELD_NAME));
    }
}
