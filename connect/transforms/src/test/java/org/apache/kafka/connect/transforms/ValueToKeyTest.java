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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValueToKeyTest {
    private final ValueToKey<SinkRecord> xform = new ValueToKey<>();

    @AfterEach
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {
        xform.configure(Collections.singletonMap("fields", "a,b"));

        final HashMap<String, Integer> value = new HashMap<>();
        value.put("a", 1);
        value.put("b", 2);
        value.put("c", 3);

        final SinkRecord record = new SinkRecord("", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final HashMap<String, Integer> expectedKey = new HashMap<>();
        expectedKey.put("a", 1);
        expectedKey.put("b", 2);

        assertNull(transformedRecord.keySchema());
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    public void withSchema() {
        xform.configure(Collections.singletonMap("fields", "a,b"));

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .field("c", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);
        value.put("b", 2);
        value.put("c", 3);

        final SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("b", Schema.INT32_SCHEMA)
                .build();

        final Struct expectedKey = new Struct(expectedKeySchema)
                .put("a", 1)
                .put("b", 2);

        assertEquals(expectedKeySchema, transformedRecord.keySchema());
        assertEquals(expectedKey, transformedRecord.key());
    }

    @Test
    public void nonExistingField() {
        xform.configure(Collections.singletonMap("fields", "not_exist"));

        final Schema valueSchema = SchemaBuilder.struct()
            .field("a", Schema.INT32_SCHEMA)
            .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);

        final SinkRecord record = new SinkRecord("", 0, null, null, valueSchema, value, 0);

        DataException actual = assertThrows(DataException.class, () -> xform.apply(record));
        assertEquals("Field does not exist: not_exist", actual.getMessage());
    }
}
