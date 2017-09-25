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
package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MaskFieldTest {

    private static MaskField<SinkRecord> transform(List<String> fields) {
        final MaskField<SinkRecord> xform = new MaskField.Value<>();
        xform.configure(Collections.singletonMap("fields", fields));
        return xform;
    }

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("", 0, null, null, schema, value, 0);
    }

    @Test
    public void schemaless() {
        final Map<String, Object> value = new HashMap<>();
        value.put("magic", 42);
        value.put("bool", true);
        value.put("byte", (byte) 42);
        value.put("short", (short) 42);
        value.put("int", 42);
        value.put("long", 42L);
        value.put("float", 42f);
        value.put("double", 42d);
        value.put("string", "blabla");
        value.put("date", new Date());
        value.put("bigint", new BigInteger("42"));
        value.put("bigdec", new BigDecimal("42.0"));
        value.put("list", Collections.singletonList(42));
        value.put("map", Collections.singletonMap("key", "value"));

        final List<String> maskFields = new ArrayList<>(value.keySet());
        maskFields.remove("magic");

        final Map<String, Object> updatedValue = (Map) transform(maskFields).apply(record(null, value)).value();

        assertEquals(42, updatedValue.get("magic"));
        assertEquals(false, updatedValue.get("bool"));
        assertEquals((byte) 0, updatedValue.get("byte"));
        assertEquals((short) 0, updatedValue.get("short"));
        assertEquals(0, updatedValue.get("int"));
        assertEquals(0L, updatedValue.get("long"));
        assertEquals(0f, updatedValue.get("float"));
        assertEquals(0d, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
        assertEquals(new Date(0), updatedValue.get("date"));
        assertEquals(BigInteger.ZERO, updatedValue.get("bigint"));
        assertEquals(BigDecimal.ZERO, updatedValue.get("bigdec"));
        assertEquals(Collections.emptyList(), updatedValue.get("list"));
        assertEquals(Collections.emptyMap(), updatedValue.get("map"));
    }

    @Test
    public void withSchema() {
        Schema schema = SchemaBuilder.struct()
                .field("magic", Schema.INT32_SCHEMA)
                .field("bool", Schema.BOOLEAN_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("string", Schema.STRING_SCHEMA)
                .field("date", org.apache.kafka.connect.data.Date.SCHEMA)
                .field("time", Time.SCHEMA)
                .field("timestamp", Timestamp.SCHEMA)
                .field("decimal", Decimal.schema(0))
                .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA))
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .build();

        final Struct value = new Struct(schema);
        value.put("magic", 42);
        value.put("bool", true);
        value.put("byte", (byte) 42);
        value.put("short", (short) 42);
        value.put("int", 42);
        value.put("long", 42L);
        value.put("float", 42f);
        value.put("double", 42d);
        value.put("string", "hmm");
        value.put("date", new Date());
        value.put("time", new Date());
        value.put("timestamp", new Date());
        value.put("decimal", new BigDecimal(42));
        value.put("array", Arrays.asList(1, 2, 3));
        value.put("map", Collections.singletonMap("what", "what"));

        final List<String> maskFields = new ArrayList<>(schema.fields().size());
        for (Field field: schema.fields()) {
            if (!field.name().equals("magic")) {
                maskFields.add(field.name());
            }
        }

        final Struct updatedValue = (Struct) transform(maskFields).apply(record(schema, value)).value();

        assertEquals(42, updatedValue.get("magic"));
        assertEquals(false, updatedValue.get("bool"));
        assertEquals((byte) 0, updatedValue.get("byte"));
        assertEquals((short) 0, updatedValue.get("short"));
        assertEquals(0, updatedValue.get("int"));
        assertEquals(0L, updatedValue.get("long"));
        assertEquals(0f, updatedValue.get("float"));
        assertEquals(0d, updatedValue.get("double"));
        assertEquals("", updatedValue.get("string"));
        assertEquals(new Date(0), updatedValue.get("date"));
        assertEquals(new Date(0), updatedValue.get("time"));
        assertEquals(new Date(0), updatedValue.get("timestamp"));
        assertEquals(BigDecimal.ZERO, updatedValue.get("decimal"));
        assertEquals(Collections.emptyList(), updatedValue.get("array"));
        assertEquals(Collections.emptyMap(), updatedValue.get("map"));
    }

}
