/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.copycat.data;

import org.apache.kafka.copycat.data.GenericData.Record;
import org.apache.kafka.copycat.data.Schema.Field;
import org.apache.kafka.copycat.data.Schema.Type;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.*;

public class TestGenericData {

    @Test(expected = DataRuntimeException.class)
    public void testrecordConstructorNullSchema() throws Exception {
        new GenericData.Record(null);
    }

    @Test(expected = DataRuntimeException.class)
    public void testrecordConstructorWrongSchema() throws Exception {
        new GenericData.Record(Schema.create(Schema.Type.INT));
    }

    @Test(expected = DataRuntimeException.class)
    public void testArrayConstructorNullSchema() throws Exception {
        new GenericData.Array<Object>(1, null);
    }

    @Test(expected = DataRuntimeException.class)
    public void testArrayConstructorWrongSchema() throws Exception {
        new GenericData.Array<Object>(1, Schema.create(Schema.Type.INT));
    }

    @Test(expected = DataRuntimeException.class)
    public void testRecordCreateEmptySchema() throws Exception {
        Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
        Record r = new GenericData.Record(s);
    }

    @Test(expected = DataRuntimeException.class)
    public void testGetEmptySchemaFields() throws Exception {
        Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
        s.getFields();
    }

    @Test(expected = DataRuntimeException.class)
    public void testGetEmptySchemaField() throws Exception {
        Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
        s.getField("foo");
    }

    @Test(expected = DataRuntimeException.class)
    public void testRecordPutInvalidField() throws Exception {
        Schema s = Schema.createRecord("schemaName", "schemaDoc", "namespace", false);
        List<Schema.Field> fields = new ArrayList<Schema.Field>();
        fields.add(new Schema.Field("someFieldName", s, "docs", null));
        s.setFields(fields);
        Record r = new GenericData.Record(s);
        r.put("invalidFieldName", "someValue");
    }

    @Test
    /** Make sure that even with nulls, hashCode() doesn't throw NPE. */
    public void testHashCode() {
        GenericData.get().hashCode(null, Schema.create(Type.NULL));
        GenericData.get().hashCode(null, Schema.createUnion(
                Arrays.asList(Schema.create(Type.BOOLEAN), Schema.create(Type.STRING))));
        List<CharSequence> stuff = new ArrayList<CharSequence>();
        stuff.add("string");
        Schema schema = recordSchema();
        GenericRecord r = new GenericData.Record(schema);
        r.put(0, stuff);
        GenericData.get().hashCode(r, schema);
    }

    @Test
    public void testEquals() {
        Schema s = recordSchema();
        GenericRecord r0 = new GenericData.Record(s);
        GenericRecord r1 = new GenericData.Record(s);
        GenericRecord r2 = new GenericData.Record(s);
        Collection<CharSequence> l0 = new ArrayDeque<CharSequence>();
        List<CharSequence> l1 = new ArrayList<CharSequence>();
        GenericArray<CharSequence> l2 =
                new GenericData.Array<CharSequence>(1, s.getFields().get(0).schema());
        String foo = "foo";
        l0.add(new StringBuffer(foo));
        l1.add(foo);
        l2.add(new Utf8(foo));
        r0.put(0, l0);
        r1.put(0, l1);
        r2.put(0, l2);
        assertEquals(r0, r1);
        assertEquals(r0, r2);
        assertEquals(r1, r2);
    }

    private Schema recordSchema() {
        List<Field> fields = new ArrayList<Field>();
        fields.add(new Field("anArray", Schema.createArray(Schema.create(Type.STRING)), null, null));
        Schema schema = Schema.createRecord("arrayFoo", "test", "mytest", false);
        schema.setFields(fields);

        return schema;
    }

    @Test
    public void testEquals2() {
        Schema schema1 = Schema.createRecord("r", null, "x", false);
        List<Field> fields1 = new ArrayList<Field>();
        fields1.add(new Field("a", Schema.create(Schema.Type.STRING), null, null,
                Field.Order.IGNORE));
        schema1.setFields(fields1);

        // only differs in field order
        Schema schema2 = Schema.createRecord("r", null, "x", false);
        List<Field> fields2 = new ArrayList<Field>();
        fields2.add(new Field("a", Schema.create(Schema.Type.STRING), null, null,
                Field.Order.ASCENDING));
        schema2.setFields(fields2);

        GenericRecord record1 = new GenericData.Record(schema1);
        record1.put("a", "1");

        GenericRecord record2 = new GenericData.Record(schema2);
        record2.put("a", "2");

        assertFalse(record2.equals(record1));
        assertFalse(record1.equals(record2));
    }

    @Test
    public void testRecordGetFieldDoesntExist() throws Exception {
        List<Field> fields = new ArrayList<Field>();
        Schema schema = Schema.createRecord(fields);
        GenericData.Record record = new GenericData.Record(schema);
        assertNull(record.get("does not exist"));
    }

    @Test
    public void testArrayReversal() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> forward = new GenericData.Array<Integer>(10, schema);
        GenericArray<Integer> backward = new GenericData.Array<Integer>(10, schema);
        for (int i = 0; i <= 9; i++) {
            forward.add(i);
        }
        for (int i = 9; i >= 0; i--) {
            backward.add(i);
        }
        forward.reverse();
        assertTrue(forward.equals(backward));
    }

    @Test
    public void testArrayListInterface() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<Integer>(1, schema);
        array.add(99);
        assertEquals(new Integer(99), array.get(0));
        List<Integer> list = new ArrayList<Integer>();
        list.add(99);
        assertEquals(array, list);
        assertEquals(list, array);
        assertEquals(list.hashCode(), array.hashCode());
        try {
            array.get(2);
            fail("Expected IndexOutOfBoundsException getting index 2");
        } catch (IndexOutOfBoundsException e) {
        }
        array.clear();
        assertEquals(0, array.size());
        try {
            array.get(0);
            fail("Expected IndexOutOfBoundsException getting index 0 after clear()");
        } catch (IndexOutOfBoundsException e) {
        }

    }

    @Test
    public void testArrayAddAtLocation() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<Integer>(6, schema);
        array.clear();
        for (int i = 0; i < 5; ++i)
            array.add(i);
        assertEquals(5, array.size());
        array.add(0, 6);
        assertEquals(new Integer(6), array.get(0));
        assertEquals(6, array.size());
        assertEquals(new Integer(0), array.get(1));
        assertEquals(new Integer(4), array.get(5));
        array.add(6, 7);
        assertEquals(new Integer(7), array.get(6));
        assertEquals(7, array.size());
        assertEquals(new Integer(6), array.get(0));
        assertEquals(new Integer(4), array.get(5));
        array.add(1, 8);
        assertEquals(new Integer(8), array.get(1));
        assertEquals(new Integer(0), array.get(2));
        assertEquals(new Integer(6), array.get(0));
        assertEquals(8, array.size());
        try {
            array.get(9);
            fail("Expected IndexOutOfBoundsException after adding elements");
        } catch (IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testArrayRemove() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<Integer>(10, schema);
        array.clear();
        for (int i = 0; i < 10; ++i)
            array.add(i);
        assertEquals(10, array.size());
        assertEquals(new Integer(0), array.get(0));
        assertEquals(new Integer(9), array.get(9));

        array.remove(0);
        assertEquals(9, array.size());
        assertEquals(new Integer(1), array.get(0));
        assertEquals(new Integer(2), array.get(1));
        assertEquals(new Integer(9), array.get(8));

        // Test boundary errors.
        try {
            array.get(9);
            fail("Expected IndexOutOfBoundsException after removing an element");
        } catch (IndexOutOfBoundsException e) {
        }
        try {
            array.set(9, 99);
            fail("Expected IndexOutOfBoundsException after removing an element");
        } catch (IndexOutOfBoundsException e) {
        }
        try {
            array.remove(9);
            fail("Expected IndexOutOfBoundsException after removing an element");
        } catch (IndexOutOfBoundsException e) {
        }

        // Test that we can still remove for properly sized arrays, and the rval
        assertEquals(new Integer(9), array.remove(8));
        assertEquals(8, array.size());


        // Test insertion after remove
        array.add(88);
        assertEquals(new Integer(88), array.get(8));
    }

    @Test
    public void testArraySet() {
        Schema schema = Schema.createArray(Schema.create(Schema.Type.INT));
        GenericArray<Integer> array = new GenericData.Array<Integer>(10, schema);
        array.clear();
        for (int i = 0; i < 10; ++i)
            array.add(i);
        assertEquals(10, array.size());
        assertEquals(new Integer(0), array.get(0));
        assertEquals(new Integer(5), array.get(5));

        assertEquals(new Integer(5), array.set(5, 55));
        assertEquals(10, array.size());
        assertEquals(new Integer(55), array.get(5));
    }

    @Test
    public void testToStringDoesNotEscapeForwardSlash() throws Exception {
        GenericData data = GenericData.get();
        assertEquals("\"/\"", data.toString("/"));
    }

    @Test
    public void testToStringNanInfinity() throws Exception {
        GenericData data = GenericData.get();
        assertEquals("\"Infinity\"", data.toString(Float.POSITIVE_INFINITY));
        assertEquals("\"-Infinity\"", data.toString(Float.NEGATIVE_INFINITY));
        assertEquals("\"NaN\"", data.toString(Float.NaN));
        assertEquals("\"Infinity\"", data.toString(Double.POSITIVE_INFINITY));
        assertEquals("\"-Infinity\"", data.toString(Double.NEGATIVE_INFINITY));
        assertEquals("\"NaN\"", data.toString(Double.NaN));
    }

    @Test
    public void testEnumCompare() {
        Schema s = Schema.createEnum("Kind", null, null, Arrays.asList("Z", "Y", "X"));
        GenericEnumSymbol z = new GenericData.EnumSymbol(s, "Z");
        GenericEnumSymbol y = new GenericData.EnumSymbol(s, "Y");
        assertEquals(0, z.compareTo(z));
        assertTrue(y.compareTo(z) > 0);
        assertTrue(z.compareTo(y) < 0);
    }

    @Test
    public void testByteBufferDeepCopy() {
        // Test that a deep copy of a byte buffer respects the byte buffer
        // limits and capacity.
        byte[] buffer_value = {0, 1, 2, 3, 0, 0, 0};
        ByteBuffer buffer = ByteBuffer.wrap(buffer_value, 1, 4);
        Schema schema = Schema.createRecord("my_record", "doc", "mytest", false);
        Field byte_field =
                new Field("bytes", Schema.create(Type.BYTES), null, null);
        schema.setFields(Arrays.asList(byte_field));

        GenericRecord record = new GenericData.Record(schema);
        record.put(byte_field.name(), buffer);

        GenericRecord copy = GenericData.get().deepCopy(schema, record);
        ByteBuffer buffer_copy = (ByteBuffer) copy.get(byte_field.name());

        assertEquals(buffer, buffer_copy);
    }

    @Test
    public void testValidateNullableEnum() {
        List<Schema> unionTypes = new ArrayList<Schema>();
        Schema schema;
        Schema nullSchema = Schema.create(Type.NULL);
        Schema enumSchema = Schema.createEnum("AnEnum", null, null, Arrays.asList("X", "Y", "Z"));
        GenericEnumSymbol w = new GenericData.EnumSymbol(enumSchema, "W");
        GenericEnumSymbol x = new GenericData.EnumSymbol(enumSchema, "X");
        GenericEnumSymbol y = new GenericData.EnumSymbol(enumSchema, "Y");
        GenericEnumSymbol z = new GenericData.EnumSymbol(enumSchema, "Z");

        // null is first
        unionTypes.clear();
        unionTypes.add(nullSchema);
        unionTypes.add(enumSchema);
        schema = Schema.createUnion(unionTypes);

        assertTrue(GenericData.get().validate(schema, z));
        assertTrue(GenericData.get().validate(schema, y));
        assertTrue(GenericData.get().validate(schema, x));
        assertFalse(GenericData.get().validate(schema, w));
        assertTrue(GenericData.get().validate(schema, null));

        // null is last
        unionTypes.clear();
        unionTypes.add(enumSchema);
        unionTypes.add(nullSchema);
        schema = Schema.createUnion(unionTypes);

        assertTrue(GenericData.get().validate(schema, z));
        assertTrue(GenericData.get().validate(schema, y));
        assertTrue(GenericData.get().validate(schema, x));
        assertFalse(GenericData.get().validate(schema, w));
        assertTrue(GenericData.get().validate(schema, null));
    }

    private enum anEnum {ONE, TWO, THREE}

    ;

    @Test
    public void validateRequiresGenericSymbolForEnumSchema() {
        final Schema schema = Schema.createEnum("my_enum", "doc", "namespace", Arrays.asList("ONE", "TWO", "THREE"));
        final GenericData gd = GenericData.get();
    
    /* positive cases */
        assertTrue(gd.validate(schema, new GenericData.EnumSymbol(schema, "ONE")));
        assertTrue(gd.validate(schema, new GenericData.EnumSymbol(schema, anEnum.ONE)));

    /* negative cases */
        assertFalse("We don't expect GenericData to allow a String datum for an enum schema", gd.validate(schema, "ONE"));
        assertFalse("We don't expect GenericData to allow a Java Enum for an enum schema", gd.validate(schema, anEnum.ONE));
    }

    @Test
    public void testValidateUnion() {
        Schema type1Schema = SchemaBuilder.record("Type1")
                .fields()
                .requiredString("myString")
                .requiredInt("myInt")
                .endRecord();

        Schema type2Schema = SchemaBuilder.record("Type2")
                .fields()
                .requiredString("myString")
                .endRecord();

        Schema unionSchema = SchemaBuilder.unionOf()
                .type(type1Schema).and().type(type2Schema)
                .endUnion();

        GenericRecord record = new GenericData.Record(type2Schema);
        record.put("myString", "myValue");
        assertTrue(GenericData.get().validate(unionSchema, record));
    }
}
