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
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class InsertFieldTest {
    private InsertField<SourceRecord> xform = new InsertField.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("topic.field", "topic_field"));
        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndInsertConfiguredFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("topic_field").schema());
        assertEquals("test", ((Struct) transformedRecord.value()).getString("topic_field"));

        assertEquals(Schema.OPTIONAL_INT32_SCHEMA, transformedRecord.valueSchema().field("partition_field").schema());
        assertEquals(0, ((Struct) transformedRecord.value()).getInt32("partition_field").intValue());

        assertEquals(Timestamp.builder().optional().build(), transformedRecord.valueSchema().field("timestamp_field").schema());
        assertEquals(null, ((Struct) transformedRecord.value()).getInt64("timestamp_field"));

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("instance_id").schema());
        assertEquals("my-instance-id", ((Struct) transformedRecord.value()).getString("instance_id"));

        // Exercise caching
        final SourceRecord transformedRecord2 = xform.apply(
                new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
    }

    @Test
    public void schemalessInsertConfiguredFields() {
        final Map<String, Object> props = new HashMap<>();
        props.put("topic.field", "topic_field!");
        props.put("partition.field", "partition_field");
        props.put("timestamp.field", "timestamp_field?");
        props.put("static.field", "instance_id");
        props.put("static.value", "my-instance-id");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Collections.singletonMap("magic", 42L));

        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(42L, ((Map) transformedRecord.value()).get("magic"));
        assertEquals("test", ((Map) transformedRecord.value()).get("topic_field"));
        assertEquals(0, ((Map) transformedRecord.value()).get("partition_field"));
        assertEquals(null, ((Map) transformedRecord.value()).get("timestamp_field"));
        assertEquals("my-instance-id", ((Map) transformedRecord.value()).get("instance_id"));
    }

}
