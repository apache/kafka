/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.connect.transforms;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SetSchemaMetadataTest {

    @Test
    public void schemaNameUpdate() {
        final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();
        xform.configure(Collections.singletonMap("schema.name", "foo"));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals("foo", updatedRecord.valueSchema().name());
    }

    @Test
    public void schemaVersionUpdate() {
        final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();
        xform.configure(Collections.singletonMap("schema.version", 42));
        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);
        final SinkRecord updatedRecord = xform.apply(record);
        assertEquals(new Integer(42), updatedRecord.valueSchema().version());
    }

    @Test
    public void schemaNameAndVersionUpdate() {
        final Map<String, String> props = new HashMap<>();
        props.put("schema.name", "foo");
        props.put("schema.version", "42");

        final SetSchemaMetadata<SinkRecord> xform = new SetSchemaMetadata.Value<>();
        xform.configure(props);

        final SinkRecord record = new SinkRecord("", 0, null, null, SchemaBuilder.struct().build(), null, 0);

        final SinkRecord updatedRecord = xform.apply(record);

        assertEquals("foo", updatedRecord.valueSchema().name());
        assertEquals(new Integer(42), updatedRecord.valueSchema().version());
    }

}
