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
package org.apache.kafka.connect.data;

import org.apache.kafka.connect.errors.DataException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;


public class StructTest {

    private static final Schema FLAT_STRUCT_SCHEMA = SchemaBuilder.struct()
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .build();

    private static final Schema ARRAY_SCHEMA = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
    private static final Schema MAP_SCHEMA = SchemaBuilder.map(
            Schema.INT32_SCHEMA,
            Schema.STRING_SCHEMA
    ).build();
    private static final Schema NESTED_CHILD_SCHEMA = SchemaBuilder.struct()
            .field("int8", Schema.INT8_SCHEMA)
            .build();
    private static final Schema NESTED_SCHEMA = SchemaBuilder.struct()
            .field("array", ARRAY_SCHEMA)
            .field("map", MAP_SCHEMA)
            .field("nested", NESTED_CHILD_SCHEMA)
            .build();

    private static final Schema REQUIRED_FIELD_SCHEMA = Schema.INT8_SCHEMA;
    private static final Schema OPTIONAL_FIELD_SCHEMA = SchemaBuilder.int8().optional().build();
    private static final Schema DEFAULT_FIELD_SCHEMA = SchemaBuilder.int8().defaultValue((byte) 0).build();

    @Test
    public void testFlatStruct() {
        Struct struct = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "foobar")
                .put("bytes", "foobar".getBytes());

        // Test equality, and also the type-specific getters
        assertEquals((byte) 12, (byte) struct.getInt8("int8"));
        assertEquals((short) 12, (short) struct.getInt16("int16"));
        assertEquals(12, (int) struct.getInt32("int32"));
        assertEquals((long) 12, (long) struct.getInt64("int64"));
        assertEquals((Float) 12.f, struct.getFloat32("float32"));
        assertEquals((Double) 12., struct.getFloat64("float64"));
        assertEquals(true, struct.getBoolean("boolean"));
        assertEquals("foobar", struct.getString("string"));
        assertEquals(ByteBuffer.wrap("foobar".getBytes()), ByteBuffer.wrap(struct.getBytes("bytes")));

        struct.validate();
    }

    @Test
    public void testComplexStruct() {
        List<Byte> array = Arrays.asList((byte) 1, (byte) 2);
        Map<Integer, String> map = Collections.singletonMap(1, "string");
        Struct struct = new Struct(NESTED_SCHEMA)
                .put("array", array)
                .put("map", map)
                .put("nested", new Struct(NESTED_CHILD_SCHEMA).put("int8", (byte) 12));

        // Separate the call to get the array and map to validate the typed get methods work properly
        List<Byte> arrayExtracted = struct.getArray("array");
        assertEquals(array, arrayExtracted);
        Map<Byte, Byte> mapExtracted = struct.getMap("map");
        assertEquals(map, mapExtracted);
        assertEquals((byte) 12, struct.getStruct("nested").get("int8"));

        struct.validate();
    }


    // These don't test all the ways validation can fail, just one for each element. See more extensive validation
    // tests in SchemaTest. These are meant to ensure that we are invoking the same code path and that we do deeper
    // inspection than just checking the class of the object

    @Test(expected = DataException.class)
    public void testInvalidFieldType() {
        new Struct(FLAT_STRUCT_SCHEMA).put("int8", "should fail because this is a string, not int8");
    }

    @Test(expected = DataException.class)
    public void testInvalidArrayFieldElements() {
        new Struct(NESTED_SCHEMA).put("array", Arrays.asList("should fail since elements should be int8s"));
    }

    @Test(expected = DataException.class)
    public void testInvalidMapKeyElements() {
        new Struct(NESTED_SCHEMA).put("map", Collections.singletonMap("should fail because keys should be int8s", (byte) 12));
    }

    @Test(expected = DataException.class)
    public void testInvalidStructFieldSchema() {
        new Struct(NESTED_SCHEMA).put("nested", new Struct(MAP_SCHEMA));
    }

    @Test(expected = DataException.class)
    public void testInvalidStructFieldValue() {
        new Struct(NESTED_SCHEMA).put("nested", new Struct(NESTED_CHILD_SCHEMA));
    }


    @Test(expected = DataException.class)
    public void testMissingFieldValidation() {
        // Required int8 field
        Schema schema = SchemaBuilder.struct().field("field", REQUIRED_FIELD_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.validate();
    }

    @Test
    public void testMissingOptionalFieldValidation() {
        Schema schema = SchemaBuilder.struct().field("field", OPTIONAL_FIELD_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.validate();
    }

    @Test
    public void testMissingFieldWithDefaultValidation() {
        Schema schema = SchemaBuilder.struct().field("field", DEFAULT_FIELD_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.validate();
    }

    @Test
    public void testMissingFieldWithDefaultValue() {
        Schema schema = SchemaBuilder.struct().field("field", DEFAULT_FIELD_SCHEMA).build();
        Struct struct = new Struct(schema);
        assertEquals((byte) 0, struct.get("field"));
    }

    @Test
    public void testMissingFieldWithoutDefaultValue() {
        Schema schema = SchemaBuilder.struct().field("field", REQUIRED_FIELD_SCHEMA).build();
        Struct struct = new Struct(schema);
        assertNull(struct.get("field"));
    }


    @Test
    public void testEquals() {
        Struct struct1 = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "foobar")
                .put("bytes", ByteBuffer.wrap("foobar".getBytes()));
        Struct struct2 = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "foobar")
                .put("bytes", ByteBuffer.wrap("foobar".getBytes()));
        Struct struct3 = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "mismatching string")
                .put("bytes", ByteBuffer.wrap("foobar".getBytes()));

        assertEquals(struct1, struct2);
        assertNotEquals(struct1, struct3);

        List<Byte> array = Arrays.asList((byte) 1, (byte) 2);
        Map<Integer, String> map = Collections.singletonMap(1, "string");
        struct1 = new Struct(NESTED_SCHEMA)
                .put("array", array)
                .put("map", map)
                .put("nested", new Struct(NESTED_CHILD_SCHEMA).put("int8", (byte) 12));
        List<Byte> array2 = Arrays.asList((byte) 1, (byte) 2);
        Map<Integer, String> map2 = Collections.singletonMap(1, "string");
        struct2 = new Struct(NESTED_SCHEMA)
                .put("array", array2)
                .put("map", map2)
                .put("nested", new Struct(NESTED_CHILD_SCHEMA).put("int8", (byte) 12));
        List<Byte> array3 = Arrays.asList((byte) 1, (byte) 2, (byte) 3);
        Map<Integer, String> map3 = Collections.singletonMap(2, "string");
        struct3 = new Struct(NESTED_SCHEMA)
                .put("array", array3)
                .put("map", map3)
                .put("nested", new Struct(NESTED_CHILD_SCHEMA).put("int8", (byte) 13));

        assertEquals(struct1, struct2);
        assertNotEquals(struct1, struct3);
    }

    @Test
    public void testEqualsAndHashCodeWithByteArrayValue() {
        Struct struct1 = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "foobar")
                .put("bytes", "foobar".getBytes());

        Struct struct2 = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "foobar")
                .put("bytes", "foobar".getBytes());

        Struct struct3 = new Struct(FLAT_STRUCT_SCHEMA)
                .put("int8", (byte) 12)
                .put("int16", (short) 12)
                .put("int32", 12)
                .put("int64", (long) 12)
                .put("float32", 12.f)
                .put("float64", 12.)
                .put("boolean", true)
                .put("string", "foobar")
                .put("bytes", "mismatching_string".getBytes());

        // Verify contract for equals: method must be reflexive and transitive
        assertEquals(struct1, struct2);
        assertEquals(struct2, struct1);
        assertNotEquals(struct1, struct3);
        assertNotEquals(struct2, struct3);
        // Testing hashCode against a hardcoded value here would be incorrect: hashCode values need not be equal for any
        // two distinct executions. However, based on the general contract for hashCode, if two objects are equal, their
        // hashCodes must be equal. If they are not equal, their hashCodes should not be equal for performance reasons.
        assertEquals(struct1.hashCode(), struct2.hashCode());
        assertNotEquals(struct1.hashCode(), struct3.hashCode());
        assertNotEquals(struct2.hashCode(), struct3.hashCode());
    }

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testValidateStructWithNullValue() {
        Schema schema = SchemaBuilder.struct()
                .field("one", Schema.STRING_SCHEMA)
                .field("two", Schema.STRING_SCHEMA)
                .field("three", Schema.STRING_SCHEMA)
                .build();

        Struct struct = new Struct(schema);
        thrown.expect(DataException.class);
        thrown.expectMessage("Invalid value: null used for required field: \"one\", schema type: STRING");
        struct.validate();
    }

    @Test
    public void testValidateFieldWithInvalidValueType() {
        String fieldName = "field";
        FakeSchema fakeSchema = new FakeSchema();

        thrown.expect(DataException.class);
        thrown.expectMessage("Invalid Java object for schema type null: class java.lang.Object for field: \"field\"");
        ConnectSchema.validateValue(fieldName, fakeSchema, new Object());

        thrown.expect(DataException.class);
        thrown.expectMessage("Invalid Java object for schema type INT8: class java.lang.Object for field: \"field\"");
        ConnectSchema.validateValue(fieldName, Schema.INT8_SCHEMA, new Object());
    }

    @Test
    public void testPutNullField() {
        final String fieldName = "fieldName";
        Schema testSchema = SchemaBuilder.struct()
            .field(fieldName, Schema.STRING_SCHEMA);
        Struct struct = new Struct(testSchema);

        thrown.expect(DataException.class);
        Field field = null;
        struct.put(field, "valid");
    }

    @Test
    public void testInvalidPutIncludesFieldName() {
        final String fieldName = "fieldName";
        Schema testSchema = SchemaBuilder.struct()
            .field(fieldName, Schema.STRING_SCHEMA);
        Struct struct = new Struct(testSchema);

        thrown.expect(DataException.class);
        thrown.expectMessage("Invalid value: null used for required field: \"fieldName\", schema type: STRING");
        struct.put(fieldName, null);
    }
}
