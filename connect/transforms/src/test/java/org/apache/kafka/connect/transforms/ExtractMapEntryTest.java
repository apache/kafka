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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExtractMapEntryTest {

    private final ExtractMapEntry<SourceRecord> xform = new ExtractMapEntry.Key<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void schemaless() {

        xform.configure(Collections.singletonMap("entry", "magic"));

        final SourceRecord record = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test",
                null,
                Collections.singletonMap("magic", 42),
                null,
                null
            );

        Assert.assertThrows(DataException.class, ()->xform.apply(record));

    }

    @Test
    public void testWithEntry() {

        xform.configure(Collections.singletonMap("entry", "magic"));

        final SourceRecord record = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA),
                Collections.singletonMap("magic", 42),
                null,
                null
        );

        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void testWithNullMap() {

        xform.configure(Collections.singletonMap("entry", "magic"));

        final SourceRecord record = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA),
                null,
                null,
                null
        );

        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void testWithoutEntry() {
        xform.configure(Collections.singletonMap("entry", "magic"));

        final SourceRecord record = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test",
                SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA),
                Collections.emptyMap(),
                null,
                null
        );

        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertNull(transformedRecord.key());
    }

    @Test
    public void testWithIntEntry() {

        xform.configure(Collections.singletonMap("entry", "42"));

        final SourceRecord record = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "test",
                SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA),
                Collections.singletonMap(42, "magic"),
                null,
                null
        );

        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.STRING_SCHEMA, transformedRecord.keySchema());
        assertEquals("magic", transformedRecord.key());
    }

}
