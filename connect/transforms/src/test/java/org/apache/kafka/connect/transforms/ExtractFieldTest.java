/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ExtractFieldTest {

    @Test
    public void schemaless() {
        final ExtractField<SinkRecord> xform = new ExtractField.Key<>();
        xform.configure(Collections.singletonMap("field", "magic"));

        final SinkRecord record = new SinkRecord("test", 0, null, Collections.singletonMap("magic", 42), null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

    @Test
    public void withSchema() {
        final ExtractField<SinkRecord> xform = new ExtractField.Key<>();
        xform.configure(Collections.singletonMap("field", "magic"));

        final Schema keySchema = SchemaBuilder.struct().field("magic", Schema.INT32_SCHEMA).build();
        final Struct key = new Struct(keySchema).put("magic", 42);
        final SinkRecord record = new SinkRecord("test", 0, keySchema, key, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertEquals(Schema.INT32_SCHEMA, transformedRecord.keySchema());
        assertEquals(42, transformedRecord.key());
    }

}
