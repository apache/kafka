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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ReplaceFieldTest {

    @Test
    public void schemaless() {
        final ReplaceField<SinkRecord> xform = new ReplaceField.Value<>();

        final Map<String, String> props = new HashMap<>();
        props.put("blacklist", "dont");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map updatedValue = (Map) transformedRecord.value();
        assertEquals(3, updatedValue.size());
        assertEquals(42, updatedValue.get("xyz"));
        assertEquals(true, updatedValue.get("bar"));
        assertEquals("etc", updatedValue.get("etc"));
    }

    @Test
    public void withSchema() {
        final ReplaceField<SinkRecord> xform = new ReplaceField.Value<>();

        final Map<String, String> props = new HashMap<>();
        props.put("whitelist", "abc,foo");
        props.put("renames", "abc:xyz,foo:bar");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema);
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(2, updatedValue.schema().fields().size());
        assertEquals(new Integer(42), updatedValue.getInt32("xyz"));
        assertEquals(true, updatedValue.getBoolean("bar"));
    }

}
