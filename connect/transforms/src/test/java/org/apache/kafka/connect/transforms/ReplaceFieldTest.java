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
        final Map<String, Object> transformedMap = (Map<String, Object>) xform.apply(record).value();

        assertEquals(2, transformedMap.size());

        assertEquals(5, ((Map<String, Object>) transformedMap.get("do")).size());
        assertEquals(42, ((Map<String, Object>) transformedMap.get("do")).get("xyz"));
        assertEquals(true, ((Map<String, Object>) transformedMap.get("do")).get("bar"));
        assertEquals("etc", ((Map<String, Object>) transformedMap.get("do")).get("etc"));

        assertEquals(3, ((Map<String, Object>) ((Map<String, Object>) transformedMap.get("do")).get("do")).size());
        assertEquals(42, ((Map<String, Object>) ((Map<String, Object>) transformedMap.get("do")).get("do")).get("xyz"));
        assertEquals(true, ((Map<String, Object>) ((Map<String, Object>) transformedMap.get("do")).get("do")).get("bar"));
        assertEquals("etc", ((Map<String, Object>) ((Map<String, Object>) transformedMap.get("do")).get("do")).get("etc"));

        assertEquals(2, ((ArrayList<Object>) transformedMap.get("array")).size());

        assertEquals(3, ((Map<String, Object>) ((ArrayList<Object>) transformedMap.get("array")).get(0)).size());
        assertEquals(42, ((Map<String, Object>) ((ArrayList<Object>) transformedMap.get("array")).get(0)).get("xyz"));
        assertEquals(true, ((Map<String, Object>) ((ArrayList<Object>) transformedMap.get("array")).get(0)).get("bar"));
        assertEquals("etc", ((Map<String, Object>) ((ArrayList<Object>) transformedMap.get("array")).get(0)).get("etc"));

        assertEquals(((ArrayList<Object>) transformedMap.get("array")).get(0), ((ArrayList<Object>) transformedMap.get("array")).get(1));
    }

    @Test
    public void withSchemaRecursive() {
        final Map<String, Object> props = new HashMap<>();
        props.put(ReplaceField.ConfigName.INCLUDE, "abc,foo,do,array,map");
        props.put(ReplaceField.ConfigName.RENAME, "abc:xyz,foo:bar");
        props.put(ReplaceField.ConfigName.RECURSIVE, true);

        xform.configure(props);

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

        final SinkRecord record = new SinkRecord("test", 0, null, null, parentBSchema, parentBValue, 0);
        final Struct transformedStruct = (Struct) xform.apply(record).value();

        assertEquals(3, transformedStruct.schema().fields().size());

        assertEquals(4, transformedStruct.getStruct("do").schema().fields().size());
        assertEquals(42, transformedStruct.getStruct("do").get("xyz"));
        assertEquals(true, transformedStruct.getStruct("do").get("bar"));

        assertEquals(2, transformedStruct.getStruct("do").getArray("array").size());

        assertEquals(2, ((Struct) transformedStruct.getStruct("do").getArray("array").get(0)).schema().fields().size());
        assertEquals(42, ((Struct) transformedStruct.getStruct("do").getArray("array").get(0)).get("xyz"));
        assertEquals(true, ((Struct) transformedStruct.getStruct("do").getArray("array").get(0)).get("bar"));

        assertEquals(transformedStruct.getStruct("do").getArray("array").get(0), transformedStruct.getStruct("do").getArray("array").get(1));

        assertEquals(2, transformedStruct.getArray("array").size());

        assertEquals(2, ((Struct) transformedStruct.getArray("array").get(0)).schema().fields().size());
        assertEquals(42, ((Struct) transformedStruct.getArray("array").get(0)).get("xyz"));
        assertEquals(true, ((Struct) transformedStruct.getArray("array").get(0)).get("bar"));

        assertEquals(transformedStruct.getArray("array").get(0), transformedStruct.getArray("array").get(1));

        assertEquals(1, transformedStruct.getMap("map").size());

        assertEquals(4, ((Struct) transformedStruct.getMap("map").get("do")).schema().fields().size());
        assertEquals(42, ((Struct) transformedStruct.getMap("map").get("do")).get("xyz"));
        assertEquals(true, ((Struct) transformedStruct.getMap("map").get("do")).get("bar"));

        assertEquals(2, ((Struct) transformedStruct.getMap("map").get("do")).getArray("array").size());

        assertEquals(2, ((Struct) ((Struct) transformedStruct.getMap("map").get("do")).getArray("array").get(0)).schema().fields().size());
        assertEquals(42, ((Struct) ((Struct) transformedStruct.getMap("map").get("do")).getArray("array").get(0)).get("xyz"));
        assertEquals(true, ((Struct) ((Struct) transformedStruct.getMap("map").get("do")).getArray("array").get(0)).get("bar"));

        assertEquals(((Struct) transformedStruct.getMap("map").get("do")).getArray("array").get(0), 
                ((Struct) transformedStruct.getMap("map").get("do")).getArray("array").get(1));
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
        final Struct transformedStruct = (Struct) xform.apply(record).value();

        assertEquals(1, transformedStruct.schema().fields().size());
        assertEquals(Integer.valueOf(42), transformedStruct.getInt32("value"));
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
        final Struct transformedStruct = (Struct) xform.apply(record).value();

        assertEquals(1, transformedStruct.schema().fields().size());
        assertEquals(Integer.valueOf(42), transformedStruct.getInt32("value"));
    }

}
