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
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MergeFieldTest {
    private final MergeField<SourceRecord> xformKey = new MergeField.Key<>();
    private final MergeField<SourceRecord> xformValue = new MergeField.Value<>();

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        final Map<String, String> props = new HashMap<>();
        props.put(MergeField.FIELD_ROOT_CONFIG, "root");
        props.put(MergeField.FIELD_LIST_CONFIG, "field");

        xformValue.configure(props);

        xformValue.apply(new SourceRecord(null, null, "topic", 0, Schema.INT32_SCHEMA, 42));
    }


    @Test(expected = DataException.class)
    public void topLevelMapRequired() {
        final Map<String, String> props = new HashMap<>();
        props.put(MergeField.FIELD_ROOT_CONFIG, "root");
        props.put(MergeField.FIELD_LIST_CONFIG, "field");

        xformKey.configure(props);
        xformKey.apply(new SourceRecord(null, null, "topic", 0, null, 42));
    }

    @Test
    public void testMakeUpdatedSchema() {

        String rootField = "root";

        final Map<String, String> props = new HashMap<>();
        props.put(MergeField.FIELD_ROOT_CONFIG, rootField);
        props.put(MergeField.FIELD_LIST_CONFIG, "*field1?, *field2, field3?, field4");

        xformValue.configure(props);

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("field", Schema.INT32_SCHEMA);
        builder.field("field1", Schema.INT32_SCHEMA);
        builder.field("field2", Schema.INT32_SCHEMA);
        builder.field("field3", Schema.INT32_SCHEMA);
        builder.field("field4", Schema.INT32_SCHEMA);
        Schema schema = builder.build();


        builder = SchemaBuilder.struct();
        builder.field("field1", Schema.OPTIONAL_INT32_SCHEMA);
        builder.field("field2", Schema.INT32_SCHEMA);
        builder.field("field3", Schema.OPTIONAL_INT32_SCHEMA);
        builder.field("field4", Schema.INT32_SCHEMA);
        Schema nestedSchema = builder.build();


        builder = SchemaBuilder.struct();
        builder.field("field", Schema.INT32_SCHEMA);
        builder.field("field1", Schema.INT32_SCHEMA);
        builder.field("field2", Schema.INT32_SCHEMA);
        builder.field(rootField, nestedSchema);

        Schema expectedSchem = builder.build();

        List<MergeField.MergeSpec> list = new ArrayList<MergeField.MergeSpec>();
        list.add(new MergeField.MergeSpec("field1", true, true));
        list.add(new MergeField.MergeSpec("field2", true, false));
        list.add(new MergeField.MergeSpec("field3", false, true));
        list.add(new MergeField.MergeSpec("field4", false, false));


        assertEquals(xformValue.makeUpdatedSchema(schema, rootField,  list), expectedSchem);

    }

    @Test
    public void testNestedStruct() {
        String rootField = "root";

        final Map<String, String> props = new HashMap<>();
        props.put(MergeField.FIELD_ROOT_CONFIG, rootField);
        props.put(MergeField.FIELD_LIST_CONFIG, "*field1?, *field2, field3?, field4, field5?");

        xformValue.configure(props);

        SchemaBuilder builder = SchemaBuilder.struct();
        builder.field("field", Schema.INT32_SCHEMA);
        builder.field("field1", Schema.INT32_SCHEMA);
        builder.field("field2", Schema.INT32_SCHEMA);
        builder.field("field3", Schema.INT32_SCHEMA);
        builder.field("field4", Schema.INT32_SCHEMA);
        builder.field("field5", Schema.OPTIONAL_INT32_SCHEMA);
        Schema schema = builder.build();




        Struct struct = new Struct(schema);
        struct.put("field", 0);
        struct.put("field1", 1);
        struct.put("field2", 2);
        struct.put("field3", 3);
        struct.put("field4", 4);
        struct.put("field5", 5);


        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, schema, struct));



        builder = SchemaBuilder.struct();
        builder.field("field1", Schema.OPTIONAL_INT32_SCHEMA);
        builder.field("field2", Schema.INT32_SCHEMA);
        builder.field("field3", Schema.OPTIONAL_INT32_SCHEMA);
        builder.field("field4", Schema.INT32_SCHEMA);
        builder.field("field5", Schema.OPTIONAL_INT32_SCHEMA);
        Schema nestedSchema = builder.build();


        builder = SchemaBuilder.struct();
        builder.field("field", Schema.INT32_SCHEMA);
        builder.field("field1", Schema.INT32_SCHEMA);
        builder.field("field2", Schema.INT32_SCHEMA);
        builder.field(rootField, nestedSchema);

        Schema expectedSchema = builder.build();



        Struct expectedStruct = new Struct(expectedSchema);

        expectedStruct.put("field", 0);
        expectedStruct.put("field1", 1);
        expectedStruct.put("field2", 2);


        List<MergeField.MergeSpec> list = new ArrayList<MergeField.MergeSpec>();
        list.add(new MergeField.MergeSpec("field1", true, true));
        list.add(new MergeField.MergeSpec("field2", true, false));
        list.add(new MergeField.MergeSpec("field3", false, true));
        list.add(new MergeField.MergeSpec("field4", false, false));
        list.add(new MergeField.MergeSpec("field5", false, true));


        Struct nestedExpectedStruct = new Struct(xformValue.buildNewStructSchema(schema, list));
        nestedExpectedStruct.put("field1", 1);
        nestedExpectedStruct.put("field2", 2);
        nestedExpectedStruct.put("field3", 3);
        nestedExpectedStruct.put("field4", 4);
        nestedExpectedStruct.put("field5", 5);

        expectedStruct.put(rootField, nestedExpectedStruct);

        assertEquals(expectedStruct, transformed.value());



        Struct struct2 = new Struct(schema);
        struct2.put("field", 0);
        struct2.put("field1", 1);
        struct2.put("field2", 2);
        struct2.put("field3", 3);
        struct2.put("field4", 4);


        SourceRecord transformed2 = xformValue.apply(new SourceRecord(null, null,
                "topic", 0, schema, struct2));


        Struct nestedExpectedStruct2 = new Struct(xformValue.buildNewStructSchema(schema, list));
        nestedExpectedStruct2.put("field1", 1);
        nestedExpectedStruct2.put("field2", 2);
        nestedExpectedStruct2.put("field3", 3);
        nestedExpectedStruct2.put("field4", 4);

        Struct expectedStruct2 = new Struct(expectedSchema);

        expectedStruct2.put("field", 0);
        expectedStruct2.put("field1", 1);
        expectedStruct2.put("field2", 2);
        expectedStruct2.put(rootField, nestedExpectedStruct2);

        assertEquals(expectedStruct2, transformed2.value());
    }

    @Test
    public void testKey() {
        String rootField = "root";
        final Map<String, String> props = new HashMap<>();
        props.put(MergeField.FIELD_ROOT_CONFIG, rootField);
        props.put(MergeField.FIELD_LIST_CONFIG, "*field1, field2");

        xformKey.configure(props);

        final Map<String, Object> record = new HashMap<>();
        record.put("field", (byte) 8);
        record.put("field1", (short) 16);
        record.put("field2", 32);

        SourceRecord transformed =  xformKey.apply(new SourceRecord(null, null, "topic", null, record, null, null));

        final Map<String, Object> expectedNestedRecord = new HashMap<>();
        expectedNestedRecord.put("field1", (short) 16);
        expectedNestedRecord.put("field2", 32);

        final Map<String, Object> expectedRecord = new HashMap<>();
        expectedRecord.put("field", (byte) 8);
        expectedRecord.put("field1", (short) 16);
        expectedRecord.put(rootField, expectedNestedRecord);

        assertNull(transformed.keySchema());
        assertEquals(transformed.key(), expectedRecord);
    }


    void testMergeSpecHelper(MergeField.MergeSpec result, MergeField.MergeSpec expected) {
        assertEquals(result.name,   expected.name);
        assertEquals(result.keepIt,  expected.keepIt);
        assertEquals(result.optional, expected.optional);
    }

    @Test
    public void mergeSpecTest() {
        String optionalMergeAndKeep = "*abcd?";
        String optionalMergeAndRemove = "abcd?";
        String mergeAndKeep = "*abcd";
        String mergeAndRemove = "abcd";

        testMergeSpecHelper(MergeField.MergeSpec.parse(optionalMergeAndKeep), new MergeField.MergeSpec("abcd", true, true));
        testMergeSpecHelper(MergeField.MergeSpec.parse(optionalMergeAndRemove), new MergeField.MergeSpec("abcd", false, true));
        testMergeSpecHelper(MergeField.MergeSpec.parse(mergeAndKeep), new MergeField.MergeSpec("abcd", true, false));
        testMergeSpecHelper(MergeField.MergeSpec.parse(mergeAndRemove), new MergeField.MergeSpec("abcd", false, false));
    }

}
