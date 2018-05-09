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
package org.apache.kafka.common.protocol.types;

import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ProtocolSerializationTest {

    private Schema schema;
    private Struct struct;

    @Before
    public void setup() {
        this.schema = new Schema(new Field("boolean", Type.BOOLEAN),
                                 new Field("int8", Type.INT8),
                                 new Field("int16", Type.INT16),
                                 new Field("int32", Type.INT32),
                                 new Field("int64", Type.INT64),
                                 new Field("varint", Type.VARINT),
                                 new Field("varlong", Type.VARLONG),
                                 new Field("string", Type.STRING),
                                 new Field("nullable_string", Type.NULLABLE_STRING),
                                 new Field("bytes", Type.BYTES),
                                 new Field("nullable_bytes", Type.NULLABLE_BYTES),
                                 new Field("array", new ArrayOf(Type.INT32)),
                                 new Field("null_array", ArrayOf.nullable(Type.INT32)),
                                 new Field("struct", new Schema(new Field("field", new ArrayOf(Type.INT32)))));
        this.struct = new Struct(this.schema).set("boolean", true)
                                             .set("int8", (byte) 1)
                                             .set("int16", (short) 1)
                                             .set("int32", 1)
                                             .set("int64", 1L)
                                             .set("varint", 300)
                                             .set("varlong", 500L)
                                             .set("string", "1")
                                             .set("nullable_string", null)
                                             .set("bytes", ByteBuffer.wrap("1".getBytes()))
                                             .set("nullable_bytes", null)
                                             .set("array", new Object[] {1})
                                             .set("null_array", null);
        this.struct.set("struct", this.struct.instance("struct").set("field", new Object[] {1, 2, 3}));
    }

    @Test
    public void testSimple() {
        check(Type.BOOLEAN, false);
        check(Type.BOOLEAN, true);
        check(Type.INT8, (byte) -111);
        check(Type.INT16, (short) -11111);
        check(Type.INT32, -11111111);
        check(Type.INT64, -11111111111L);
        check(Type.STRING, "");
        check(Type.STRING, "hello");
        check(Type.STRING, "A\u00ea\u00f1\u00fcC");
        check(Type.NULLABLE_STRING, null);
        check(Type.NULLABLE_STRING, "");
        check(Type.NULLABLE_STRING, "hello");
        check(Type.BYTES, ByteBuffer.allocate(0));
        check(Type.BYTES, ByteBuffer.wrap("abcd".getBytes()));
        check(Type.NULLABLE_BYTES, null);
        check(Type.NULLABLE_BYTES, ByteBuffer.allocate(0));
        check(Type.NULLABLE_BYTES, ByteBuffer.wrap("abcd".getBytes()));
        check(Type.VARINT, Integer.MAX_VALUE);
        check(Type.VARINT, Integer.MIN_VALUE);
        check(Type.VARLONG, Long.MAX_VALUE);
        check(Type.VARLONG, Long.MIN_VALUE);
        check(new ArrayOf(Type.INT32), new Object[] {1, 2, 3, 4});
        check(new ArrayOf(Type.STRING), new Object[] {});
        check(new ArrayOf(Type.STRING), new Object[] {"hello", "there", "beautiful"});
        check(ArrayOf.nullable(Type.STRING), null);
    }

    @Test
    public void testNulls() {
        for (BoundField f : this.schema.fields()) {
            Object o = this.struct.get(f);
            try {
                this.struct.set(f, null);
                this.struct.validate();
                if (!f.def.type.isNullable())
                    fail("Should not allow serialization of null value.");
            } catch (SchemaException e) {
                assertFalse(f.def.type.isNullable());
            } finally {
                this.struct.set(f, o);
            }
        }
    }

    @Test
    public void testDefault() {
        Schema schema = new Schema(new Field("field", Type.INT32, "doc", 42));
        Struct struct = new Struct(schema);
        assertEquals("Should get the default value", 42, struct.get("field"));
        struct.validate(); // should be valid even with missing value
    }

    @Test
    public void testNullableDefault() {
        checkNullableDefault(Type.NULLABLE_BYTES, ByteBuffer.allocate(0));
        checkNullableDefault(Type.NULLABLE_STRING, "default");
    }

    private void checkNullableDefault(Type type, Object defaultValue) {
        // Should use default even if the field allows null values
        Schema schema = new Schema(new Field("field", type, "doc", defaultValue));
        Struct struct = new Struct(schema);
        assertEquals("Should get the default value", defaultValue, struct.get("field"));
        struct.validate(); // should be valid even with missing value
    }

    @Test
    public void testReadArraySizeTooLarge() {
        Type type = new ArrayOf(Type.INT8);
        int size = 10;
        ByteBuffer invalidBuffer = ByteBuffer.allocate(4 + size);
        invalidBuffer.putInt(Integer.MAX_VALUE);
        for (int i = 0; i < size; i++)
            invalidBuffer.put((byte) i);
        invalidBuffer.rewind();
        try {
            type.read(invalidBuffer);
            fail("Array size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadNegativeArraySize() {
        Type type = new ArrayOf(Type.INT8);
        int size = 10;
        ByteBuffer invalidBuffer = ByteBuffer.allocate(4 + size);
        invalidBuffer.putInt(-1);
        for (int i = 0; i < size; i++)
            invalidBuffer.put((byte) i);
        invalidBuffer.rewind();
        try {
            type.read(invalidBuffer);
            fail("Array size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadStringSizeTooLarge() {
        byte[] stringBytes = "foo".getBytes();
        ByteBuffer invalidBuffer = ByteBuffer.allocate(2 + stringBytes.length);
        invalidBuffer.putShort((short) (stringBytes.length * 5));
        invalidBuffer.put(stringBytes);
        invalidBuffer.rewind();
        try {
            Type.STRING.read(invalidBuffer);
            fail("String size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
        invalidBuffer.rewind();
        try {
            Type.NULLABLE_STRING.read(invalidBuffer);
            fail("String size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadNegativeStringSize() {
        byte[] stringBytes = "foo".getBytes();
        ByteBuffer invalidBuffer = ByteBuffer.allocate(2 + stringBytes.length);
        invalidBuffer.putShort((short) -1);
        invalidBuffer.put(stringBytes);
        invalidBuffer.rewind();
        try {
            Type.STRING.read(invalidBuffer);
            fail("String size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadBytesSizeTooLarge() {
        byte[] stringBytes = "foo".getBytes();
        ByteBuffer invalidBuffer = ByteBuffer.allocate(4 + stringBytes.length);
        invalidBuffer.putInt(stringBytes.length * 5);
        invalidBuffer.put(stringBytes);
        invalidBuffer.rewind();
        try {
            Type.BYTES.read(invalidBuffer);
            fail("Bytes size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
        invalidBuffer.rewind();
        try {
            Type.NULLABLE_BYTES.read(invalidBuffer);
            fail("Bytes size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
    }

    @Test
    public void testReadNegativeBytesSize() {
        byte[] stringBytes = "foo".getBytes();
        ByteBuffer invalidBuffer = ByteBuffer.allocate(4 + stringBytes.length);
        invalidBuffer.putInt(-20);
        invalidBuffer.put(stringBytes);
        invalidBuffer.rewind();
        try {
            Type.BYTES.read(invalidBuffer);
            fail("Bytes size not validated");
        } catch (SchemaException e) {
            // Expected exception
        }
    }

    @Test
    public void testToString() {
        String structStr = this.struct.toString();
        assertNotNull("Struct string should not be null.", structStr);
        assertFalse("Struct string should not be empty.", structStr.isEmpty());
    }

    private Object roundtrip(Type type, Object obj) {
        ByteBuffer buffer = ByteBuffer.allocate(type.sizeOf(obj));
        type.write(buffer, obj);
        assertFalse("The buffer should now be full.", buffer.hasRemaining());
        buffer.rewind();
        Object read = type.read(buffer);
        assertFalse("All bytes should have been read.", buffer.hasRemaining());
        return read;
    }

    private void check(Type type, Object obj) {
        Object result = roundtrip(type, obj);
        if (obj instanceof Object[]) {
            obj = Arrays.asList((Object[]) obj);
            result = Arrays.asList((Object[]) result);
        }
        assertEquals("The object read back should be the same as what was written.", obj, result);
    }

    @Test
    public void testStructEquals() {
        Schema schema = new Schema(new Field("field1", Type.NULLABLE_STRING), new Field("field2", Type.NULLABLE_STRING));
        Struct emptyStruct1 = new Struct(schema);
        Struct emptyStruct2 = new Struct(schema);
        assertEquals(emptyStruct1, emptyStruct2);

        Struct mostlyEmptyStruct = new Struct(schema).set("field1", "foo");
        assertNotEquals(emptyStruct1, mostlyEmptyStruct);
        assertNotEquals(mostlyEmptyStruct, emptyStruct1);
    }
}
