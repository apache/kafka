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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExtractFieldsTest {

    private static SinkRecord record(Schema schema, Object value) {
        return new SinkRecord("", 0, null, null, schema, value, 0);
    }

    @Test
    public void schemaless() {
        final ExtractFields<SinkRecord> xform1 = new ExtractFields.Value<>();
        final ExtractFields<SinkRecord> xform2 = new ExtractFields.Value<>();

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "last_name");
        xform1.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("first_name", "Franz");
        value.put("last_name", "Kafka");
        value.put("birthyear", 1883);
        value.put("birthplace", "Prague");

        final SinkRecord origRecord = new SinkRecord("test", 0, null, null, null, value, 0);
        SinkRecord transformedRecord;

        transformedRecord = xform1.apply(origRecord);
        final Map updatedValue1 = (Map) transformedRecord.value();
        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.valueSchema());
        assertNull(updatedValue1.get("first_name"));
        assertEquals("Kafka", updatedValue1.get("last_name"));
        assertNull(updatedValue1.get("birthyear"));

        props.put("fields", "last_name,birthyear");
        xform2.configure(props);

        transformedRecord = xform2.apply(origRecord);
        final Map updatedValue2 = (Map) transformedRecord.value();
        assertNull(transformedRecord.keySchema());
        assertNull(transformedRecord.valueSchema());
        assertNull(updatedValue2.get("first_name"));
        assertEquals("Kafka", updatedValue2.get("last_name"));
        assertEquals(1883, updatedValue2.get("birthyear"));
    }

    @Test
    public void withSchema() {
        final ExtractFields<SinkRecord> xform1 = new ExtractFields.Value<>();
        final ExtractFields<SinkRecord> xform2 = new ExtractFields.Value<>();

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "last_name");
        xform1.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("birthyear", Schema.INT32_SCHEMA)
                .field("birthplace", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("first_name", "Franz");
        value.put("last_name", "Kafka");
        value.put("birthyear", 1883);
        value.put("birthplace", "Prague");

        final SinkRecord origRecord = new SinkRecord("test", 0, null, null, schema, value, 0);
        SinkRecord transformedRecord;

        transformedRecord = xform1.apply(origRecord);
        final Struct updatedValue1 = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue1.schema().fields().size());
        assertEquals("Kafka", updatedValue1.getString("last_name"));

        props.put("fields", "last_name,birthyear");
        xform2.configure(props);

        transformedRecord = xform2.apply(origRecord);
        final Struct updatedValue2 = (Struct) transformedRecord.value();

        assertEquals(2, updatedValue2.schema().fields().size());
        assertEquals("Kafka", updatedValue2.getString("last_name"));
        assertEquals(new Integer(1883), updatedValue2.getInt32("birthyear"));
    }

    @Test
    public void nestedSchemaless() {
        final ExtractFields<SinkRecord> xform = new ExtractFields.Value<>();

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "User.id");
        xform.configure(props);

        final Map<String, Object> userRecord = new HashMap<>();
        userRecord.put("first_name", "Franz");
        userRecord.put("last_name", "Kafka");
        userRecord.put("id", "1");

        final Map<String, Object> value = new HashMap<>();
        value.put("Date", "Today");
        value.put("User", userRecord);

        final SinkRecord origRecord = new SinkRecord("test", 0, null, null, null, value, 0);
        SinkRecord transformedRecord;

        transformedRecord = xform.apply(origRecord);
        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(1, updatedValue.size());
    }

    @Test
    public void nestedWithSchema() {
        final ExtractFields<SinkRecord> xform = new ExtractFields.Value<>();

        final Map<String, String> props = new HashMap<>();
        props.put("fields", "User-id,User-address-state");
        props.put("delimiter", "-");
        xform.configure(props);

        final Schema addressSchema = SchemaBuilder.struct()
                .field("streetNumber", Schema.INT32_SCHEMA)
                .field("streetName", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .field("state", Schema.STRING_SCHEMA)
                .field("zip", Schema.STRING_SCHEMA)
                .build();

        final Schema userSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("address", addressSchema)
                .build();

        final Schema schema = SchemaBuilder.struct()
                .field("Date", Schema.STRING_SCHEMA)
                .field("User", userSchema)
                .build();

        final Struct addressRecord = new Struct(addressSchema);
        addressRecord.put("streetNumber", 101);
        addressRecord.put("streetName", "University Ave");
        addressRecord.put("city", "Palo Alto");
        addressRecord.put("state", "CA");
        addressRecord.put("zip", "95304");

        final Struct userRecord = new Struct(userSchema);
        userRecord.put("id", 1);
        userRecord.put("first_name", "Franz");
        userRecord.put("last_name", "Kafka");
        userRecord.put("address", addressRecord);

        final Struct value = new Struct(schema);
        value.put("Date", "Today");
        value.put("User", userRecord);

        final SinkRecord origRecord = new SinkRecord("test", 0, null, null, schema, value, 0);
        SinkRecord transformedRecord;

        transformedRecord = xform.apply(origRecord);
        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals(new Integer(1), updatedValue.getInt32("User-id"));
        assertEquals("CA", updatedValue.getString("User-address-state"));

    }

}
