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

        final Map<String, Object> updatedParentADo = (Map<String, Object>) updatedParentB.get("do");
        assertEquals(5, updatedParentADo.size());
        assertEquals(42, updatedParentADo.get("xyz"));
        assertEquals(true, updatedParentADo.get("bar"));
        assertEquals("etc", updatedParentADo.get("etc"));

        final Map<String, Object> updatedParentADoDo = (Map<String, Object>) updatedParentADo.get("do");
        assertEquals(3, updatedParentADoDo.size());
        assertEquals(42, updatedParentADoDo.get("xyz"));
        assertEquals(true, updatedParentADoDo.get("bar"));
        assertEquals("etc", updatedParentADoDo.get("etc"));

        final List<Object> updatedParentAArray = (ArrayList<Object>) updatedParentB.get("array");
        assertEquals(2, updatedParentAArray.size());

        final Map<String, Object> updatedParentAArray0 = (Map<String, Object>) updatedParentAArray.get(0);
        assertEquals(3, updatedParentAArray0.size());
        assertEquals(42, updatedParentAArray0.get("xyz"));
        assertEquals(true, updatedParentAArray0.get("bar"));
        assertEquals("etc", updatedParentAArray0.get("etc"));

        final Map<String, Object> updatedParentAArray1 = (Map<String, Object>) updatedParentAArray.get(1);
        assertEquals(3, updatedParentAArray1.size());
        assertEquals(42, updatedParentAArray1.get("xyz"));
        assertEquals(true, updatedParentAArray1.get("bar"));
        assertEquals("etc", updatedParentAArray1.get("etc"));
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
        
        final List<Object> updatedDoArray = (List<Object>) updatedDo.get("array");
        assertEquals(2, updatedDoArray.size());

        final Struct updatedDoArray0 = (Struct) updatedDoArray.get(0);
        assertEquals(2, updatedDoArray0.schema().fields().size());
        assertEquals(42, updatedDoArray0.get("xyz"));
        assertEquals(true, updatedDoArray0.get("bar"));

        final Struct updatedDoArray1 = (Struct) updatedDoArray.get(1);
        assertEquals(2, updatedDoArray1.schema().fields().size());
        assertEquals(42, updatedDoArray1.get("xyz"));
        assertEquals(true, updatedDoArray1.get("bar"));

        final List<Object> updatedArray = (List<Object>) updatedParentB.get("array");
        assertEquals(2, updatedArray.size());

        final Struct updatedArray0 = (Struct) updatedArray.get(0);
        assertEquals(2, updatedArray0.schema().fields().size());
        assertEquals(42, updatedArray0.get("xyz"));
        assertEquals(true, updatedArray0.get("bar"));

        final Struct updatedArray1 = (Struct) updatedArray.get(1);
        assertEquals(2, updatedArray1.schema().fields().size());
        assertEquals(42, updatedArray1.get("xyz"));
        assertEquals(true, updatedArray1.get("bar"));

        final Map<String, Object> updatedMap = (Map<String, Object>) updatedParentB.get("map");
        assertEquals(1, updatedMap.size());

        final Struct updatedMapDo = (Struct) updatedMap.get("do");
        assertEquals(4, updatedMapDo.schema().fields().size());
        assertEquals(42, updatedMapDo.get("xyz"));
        assertEquals(true, updatedMapDo.get("bar"));

        final List<Object> updatedMapDoArray = (List<Object>) updatedMapDo.get("array");
        assertEquals(2, updatedMapDoArray.size());

        final Struct updatedMapDoArray0 = (Struct) updatedMapDoArray.get(0);
        assertEquals(2, updatedMapDoArray0.schema().fields().size());
        assertEquals(42, updatedMapDoArray0.get("xyz"));
        assertEquals(true, updatedMapDoArray0.get("bar"));

        final Struct updatedMapDoArray1 = (Struct) updatedMapDoArray.get(1);
        assertEquals(2, updatedMapDoArray1.schema().fields().size());
        assertEquals(42, updatedMapDoArray1.get("xyz"));
        assertEquals(true, updatedMapDoArray1.get("bar"));
    }

    @Test
    public void withSchemaRenameContainsOne() {
        // This is a somewhat special case where if a schema has multiple optional fields, but a design rule that says 
        //   each record "must contain exactly one of these fields," then you can use renames to consolidate all of these 
        //   possible fields into one field as a result of the transformation.
        // Note that if a value exists in more than one of the fields which have the same target rename, then the transformation will fail.
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.RENAME, "value_one:value,value_two:value,value_three:value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("value_one", Schema.OPTIONAL_INT32_SCHEMA)
                .field("value_two", Schema.OPTIONAL_INT32_SCHEMA)
                .field("value_three", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("value_two", 42);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals(Integer.valueOf(42), updatedValue.getInt32("value"));
    }

    @Test(expected = DataException.class)
    public void withSchemaRenameContainsOneMultipleValues() {
        // This test is for the expected failure case of the above withSchemaRenameContainsOne (more than one value exists for the target rename).
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.RENAME, "value_one:value,value_two:value,value_three:value");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("value_one", Schema.OPTIONAL_INT32_SCHEMA)
                .field("value_two", Schema.OPTIONAL_INT32_SCHEMA)
                .field("value_three", Schema.OPTIONAL_INT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("value_two", 42)
                .put("value_three", 15);

        final SinkRecord record = new SinkRecord("test", 0, null, null, schema, value, 0);
        final SinkRecord transformedRecord = xform.apply(record);

        final Struct updatedValue = (Struct) transformedRecord.value();

        assertEquals(1, updatedValue.schema().fields().size());
        assertEquals(Integer.valueOf(42), updatedValue.getInt32("value"));
    }

}
