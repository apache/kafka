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
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ReplaceFieldTest {
    private ReplaceField<SinkRecord> xform = new ReplaceField.Value<>();

    @After
    public void teardown() {
        xform.close();
    }

    @Test
    public void tombstoneSchemaless() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.INCLUDE, "abc,foo");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void tombstoneWithSchema() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.INCLUDE, "abc,foo");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
            .field("dont", Schema.STRING_SCHEMA)
            .field("abc", Schema.INT32_SCHEMA)
            .field("foo", Schema.BOOLEAN_SCHEMA)
            .field("etc", Schema.STRING_SCHEMA)
            .build();

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertEquals(schema, transformedRecord.valueSchema());
    }

    @Test
    public void schemaless() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.EXCLUDE, "dont");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");

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
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.INCLUDE, "abc,foo");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");

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
        assertEquals(Integer.valueOf(42), updatedValue.getInt32("xyz"));
        assertEquals(true, updatedValue.getBoolean("bar"));
    }

    @Test
    public void testIncludeBackwardsCompatibility() {
        final Map<String, Object> props = new HashMap<>();
        props.put("whitelist", "abc,foo");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");

        xform.configure(props);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, null, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        assertNull(transformedRecord.value());
        assertNull(transformedRecord.valueSchema());
    }

    @Test
    public void testExcludeBackwardsCompatibility() {
        final Map<String, Object> props = new HashMap<>();
        props.put("blacklist", "dont");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");

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

    @SuppressWarnings("unchecked")
    @Test
    public void schemalessRecursive() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.EXCLUDE, "dont");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");
        props.put(ReplaceField.ConfigName.RECURSIVE, true);

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("dont", "whatever");
        value.put("abc", 42);
        value.put("foo", true);
        value.put("etc", "etc");

        final List<Object> array = new ArrayList<Object>();
        array.add(value);
        array.add(new HashMap<String, Object>(value)); //shallow copy to force new identity

        final Map<String, Object> parentValueA = new HashMap<>();
        parentValueA.put("abc", 42);
        parentValueA.put("foo", true);
        parentValueA.put("etc", "etc");
        parentValueA.put("dont", value);
        parentValueA.put("do", value);
        parentValueA.put("array", array);

        final Map<String, Object> parentValueB = new HashMap<>();
        parentValueB.put("dont", parentValueA);
        parentValueB.put("do", parentValueA);
        parentValueB.put("array", array);

        final SinkRecord record = new SinkRecord("test", 0, null, null, null, parentValueB, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Map<String, Object> updatedParentB = (Map<String, Object>) transformedRecord.value();
        assertEquals(2, updatedParentB.size());

        final Map<String, Object> updatedParentA_do = (Map<String, Object>) updatedParentB.get("do");
        assertEquals(5, updatedParentA_do.size());
        assertEquals(42, updatedParentA_do.get("xyz"));
        assertEquals(true, updatedParentA_do.get("bar"));
        assertEquals("etc", updatedParentA_do.get("etc"));

        final Map<String, Object> updatedParentA_do_do = (Map<String, Object>) updatedParentA_do.get("do");
        assertEquals(3, updatedParentA_do_do.size());
        assertEquals(42, updatedParentA_do_do.get("xyz"));
        assertEquals(true, updatedParentA_do_do.get("bar"));
        assertEquals("etc", updatedParentA_do_do.get("etc"));

        final List<Object> updatedParentA_array = (ArrayList<Object>) updatedParentB.get("array");
        assertEquals(2, updatedParentA_array.size());

        final Map<String, Object> updatedParentA_array_0 = (Map<String, Object>) updatedParentA_array.get(0);
        assertEquals(3, updatedParentA_array_0.size());
        assertEquals(42, updatedParentA_array_0.get("xyz"));
        assertEquals(true, updatedParentA_array_0.get("bar"));
        assertEquals("etc", updatedParentA_array_0.get("etc"));

        final Map<String, Object> updatedParentA_array_1 = (Map<String, Object>) updatedParentA_array.get(1);
        assertEquals(3, updatedParentA_array_1.size());
        assertEquals(42, updatedParentA_array_1.get("xyz"));
        assertEquals(true, updatedParentA_array_1.get("bar"));
        assertEquals("etc", updatedParentA_array_1.get("etc"));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void withSchemaRecursive() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.INCLUDE, "abc,foo,do,array,map");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");
        props.put(ReplaceField.ConfigName.RECURSIVE, true);

        xform.configure(props);

        // Schema

        final Schema schema = SchemaBuilder.struct()
                .field("dont", Schema.STRING_SCHEMA)
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .build();

        final Schema arraySchema = SchemaBuilder.array(schema);

        final Schema parentASchema = SchemaBuilder.struct()
                .field("abc", Schema.INT32_SCHEMA)
                .field("foo", Schema.BOOLEAN_SCHEMA)
                .field("etc", Schema.STRING_SCHEMA)
                .field("dont", schema)
                .field("do", schema)
                .field("array", arraySchema)
                .build();

        final Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, parentASchema);

        final Schema parentBSchema = SchemaBuilder.struct()
                .field("dont", parentASchema)
                .field("do", parentASchema)
                .field("array", arraySchema)
                .field("map", mapSchema)
                .build();

        // Value

        final Struct value1 = new Struct(schema)
                .put("dont", "whatever")
                .put("abc", 42)
                .put("foo", true)
                .put("etc", "etc");

        final Struct value2 = new Struct(schema)
                .put("dont", "whatever")
                .put("abc", 42)
                .put("foo", true)
                .put("etc", "etc");

        final List<Object> array = new ArrayList<Object>();
        array.add(value1);
        array.add(value2);

        final Struct parentAValue = new Struct(parentASchema)
                .put("abc", 42)
                .put("foo", true)
                .put("etc", "etc")
                .put("dont", value1)
                .put("do", value2)
                .put("array", array);

        final Map<String, Object> map = new HashMap<>();
        map.put("dont", parentAValue);
        map.put("do", parentAValue);

        final Struct parentBValue = new Struct(parentBSchema)
                .put("dont", parentAValue)
                .put("do", parentAValue)
                .put("array", array)
                .put("map", map);

        // Create and transform record

        final SinkRecord record = new SinkRecord("test", 0, null, null, parentBSchema, parentBValue, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        // Assert results

        final Struct updatedParentB = (Struct) transformedRecord.value();
        assertEquals(3, updatedParentB.schema().fields().size());

        final Struct updatedDo = (Struct) updatedParentB.get("do");
        assertEquals(4, updatedDo.schema().fields().size());
        assertEquals(42, updatedDo.get("xyz"));
        assertEquals(true, updatedDo.get("bar"));
        
        final List<Object> updatedDo_array = (List<Object>) updatedDo.get("array");
        assertEquals(2, updatedDo_array.size());

        final Struct updatedDo_array_0 = (Struct) updatedDo_array.get(0);
        assertEquals(2, updatedDo_array_0.schema().fields().size());
        assertEquals(42, updatedDo_array_0.get("xyz"));
        assertEquals(true, updatedDo_array_0.get("bar"));

        final Struct updatedDo_array_1 = (Struct) updatedDo_array.get(1);
        assertEquals(2, updatedDo_array_1.schema().fields().size());
        assertEquals(42, updatedDo_array_1.get("xyz"));
        assertEquals(true, updatedDo_array_1.get("bar"));

        final List<Object> updatedArray = (List<Object>) updatedParentB.get("array");
        assertEquals(2, updatedArray.size());

        final Struct updatedArray_0 = (Struct) updatedArray.get(0);
        assertEquals(2, updatedArray_0.schema().fields().size());
        assertEquals(42, updatedArray_0.get("xyz"));
        assertEquals(true, updatedArray_0.get("bar"));

        final Struct updatedArray_1 = (Struct) updatedArray.get(1);
        assertEquals(2, updatedArray_1.schema().fields().size());
        assertEquals(42, updatedArray_1.get("xyz"));
        assertEquals(true, updatedArray_1.get("bar"));

        final Map<String, Object> updatedMap = (Map<String, Object>) updatedParentB.get("map");
        assertEquals(1, updatedMap.size());

        final Struct updatedMap_do = (Struct) updatedMap.get("do");
        assertEquals(4, updatedMap_do.schema().fields().size());
        assertEquals(42, updatedMap_do.get("xyz"));
        assertEquals(true, updatedMap_do.get("bar"));

        final List<Object> updatedMap_do_array = (List<Object>) updatedMap_do.get("array");
        assertEquals(2, updatedMap_do_array.size());

        final Struct updatedMap_do_array_0 = (Struct) updatedMap_do_array.get(0);
        assertEquals(2, updatedMap_do_array_0.schema().fields().size());
        assertEquals(42, updatedMap_do_array_0.get("xyz"));
        assertEquals(true, updatedMap_do_array_0.get("bar"));

        final Struct updatedMap_do_array_1 = (Struct) updatedMap_do_array.get(1);
        assertEquals(2, updatedMap_do_array_1.schema().fields().size());
        assertEquals(42, updatedMap_do_array_1.get("xyz"));
        assertEquals(true, updatedMap_do_array_1.get("bar"));
    }

}
