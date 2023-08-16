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

import org.apache.kafka.common.utils.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class ProtocolSerializationTest {

    private Schema schema;
    private Struct struct;

    @BeforeEach
    public void setup() {
        this.schema = new Schema(new Field("boolean", Type.BOOLEAN),
                                 new Field("int8", Type.INT8),
                                 new Field("int16", Type.INT16),
                                 new Field("int32", Type.INT32),
                                 new Field("int64", Type.INT64),
                                 new Field("varint", Type.VARINT),
                                 new Field("varlong", Type.VARLONG),
                                 new Field("float64", Type.FLOAT64),
                                 new Field("string", Type.STRING),
                                 new Field("compact_string", Type.COMPACT_STRING),
                                 new Field("nullable_string", Type.NULLABLE_STRING),
                                 new Field("compact_nullable_string", Type.COMPACT_NULLABLE_STRING),
                                 new Field("bytes", Type.BYTES),
                                 new Field("compact_bytes", Type.COMPACT_BYTES),
                                 new Field("nullable_bytes", Type.NULLABLE_BYTES),
                                 new Field("compact_nullable_bytes", Type.COMPACT_NULLABLE_BYTES),
                                 new Field("array", new ArrayOf(Type.INT32)),
                                 new Field("compact_array", new CompactArrayOf(Type.INT32)),
                                 new Field("null_array", ArrayOf.nullable(Type.INT32)),
                                 new Field("compact_null_array", CompactArrayOf.nullable(Type.INT32)),
                                 new Field("struct", new Schema(new Field("field", new ArrayOf(Type.INT32)))));
        this.struct = new Struct(this.schema).set("boolean", true)
                                             .set("int8", (byte) 1)
                                             .set("int16", (short) 1)
                                             .set("int32", 1)
                                             .set("int64", 1L)
                                             .set("varint", 300)
                                             .set("varlong", 500L)
                                             .set("float64", 0.5D)
                                             .set("string", "1")
                                             .set("compact_string", "1")
                                             .set("nullable_string", null)
                                             .set("compact_nullable_string", null)
                                             .set("bytes", ByteBuffer.wrap("1".getBytes()))
                                             .set("compact_bytes", ByteBuffer.wrap("1".getBytes()))
                                             .set("nullable_bytes", null)
                                             .set("compact_nullable_bytes", null)
                                             .set("array", new Object[] {1})
                                             .set("compact_array", new Object[] {1})
                                             .set("null_array", null)
                                             .set("compact_null_array", null);
        this.struct.set("struct", this.struct.instance("struct").set("field", new Object[] {1, 2, 3}));
    }

    @Test
    public void testSimple() {
        check(Type.BOOLEAN, false, "BOOLEAN");
        check(Type.BOOLEAN, true, "BOOLEAN");
        check(Type.INT8, (byte) -111, "INT8");
        check(Type.INT16, (short) -11111, "INT16");
        check(Type.INT32, -11111111, "INT32");
        check(Type.INT64, -11111111111L, "INT64");
        check(Type.FLOAT64, 2.5, "FLOAT64");
        check(Type.FLOAT64, -0.5, "FLOAT64");
        check(Type.FLOAT64, 1e300, "FLOAT64");
        check(Type.FLOAT64, 0.0, "FLOAT64");
        check(Type.FLOAT64, -0.0, "FLOAT64");
        check(Type.FLOAT64, Double.MAX_VALUE, "FLOAT64");
        check(Type.FLOAT64, Double.MIN_VALUE, "FLOAT64");
        check(Type.FLOAT64, Double.NaN, "FLOAT64");
        check(Type.FLOAT64, Double.NEGATIVE_INFINITY, "FLOAT64");
        check(Type.FLOAT64, Double.POSITIVE_INFINITY, "FLOAT64");
        check(Type.STRING, "", "STRING");
        check(Type.STRING, "hello", "STRING");
        check(Type.STRING, "A\u00ea\u00f1\u00fcC", "STRING");
        check(Type.COMPACT_STRING, "", "COMPACT_STRING");
        check(Type.COMPACT_STRING, "hello", "COMPACT_STRING");
        check(Type.COMPACT_STRING, "A\u00ea\u00f1\u00fcC", "COMPACT_STRING");
        check(Type.NULLABLE_STRING, null, "NULLABLE_STRING");
        check(Type.NULLABLE_STRING, "", "NULLABLE_STRING");
        check(Type.NULLABLE_STRING, "hello", "NULLABLE_STRING");
        check(Type.COMPACT_NULLABLE_STRING, null, "COMPACT_NULLABLE_STRING");
        check(Type.COMPACT_NULLABLE_STRING, "", "COMPACT_NULLABLE_STRING");
        check(Type.COMPACT_NULLABLE_STRING, "hello", "COMPACT_NULLABLE_STRING");
        check(Type.BYTES, ByteBuffer.allocate(0), "BYTES");
        check(Type.BYTES, ByteBuffer.wrap("abcd".getBytes()), "BYTES");
        check(Type.COMPACT_BYTES, ByteBuffer.allocate(0), "COMPACT_BYTES");
        check(Type.COMPACT_BYTES, ByteBuffer.wrap("abcd".getBytes()), "COMPACT_BYTES");
        check(Type.NULLABLE_BYTES, null, "NULLABLE_BYTES");
        check(Type.NULLABLE_BYTES, ByteBuffer.allocate(0), "NULLABLE_BYTES");
        check(Type.NULLABLE_BYTES, ByteBuffer.wrap("abcd".getBytes()), "NULLABLE_BYTES");
        check(Type.COMPACT_NULLABLE_BYTES, null, "COMPACT_NULLABLE_BYTES");
        check(Type.COMPACT_NULLABLE_BYTES, ByteBuffer.allocate(0), "COMPACT_NULLABLE_BYTES");
        check(Type.COMPACT_NULLABLE_BYTES, ByteBuffer.wrap("abcd".getBytes()),
                "COMPACT_NULLABLE_BYTES");
        check(Type.VARINT, Integer.MAX_VALUE, "VARINT");
        check(Type.VARINT, Integer.MIN_VALUE, "VARINT");
        check(Type.VARLONG, Long.MAX_VALUE, "VARLONG");
        check(Type.VARLONG, Long.MIN_VALUE, "VARLONG");
        check(new ArrayOf(Type.INT32), new Object[] {1, 2, 3, 4}, "ARRAY(INT32)");
        check(new ArrayOf(Type.STRING), new Object[] {}, "ARRAY(STRING)");
        check(new ArrayOf(Type.STRING), new Object[] {"hello", "there", "beautiful"},
                "ARRAY(STRING)");
        check(new CompactArrayOf(Type.INT32), new Object[] {1, 2, 3, 4},
                "COMPACT_ARRAY(INT32)");
        check(new CompactArrayOf(Type.COMPACT_STRING), new Object[] {},
                "COMPACT_ARRAY(COMPACT_STRING)");
        check(new CompactArrayOf(Type.COMPACT_STRING),
                new Object[] {"hello", "there", "beautiful"},
                "COMPACT_ARRAY(COMPACT_STRING)");
        check(ArrayOf.nullable(Type.STRING), null, "ARRAY(STRING)");
        check(CompactArrayOf.nullable(Type.COMPACT_STRING), null,
                "COMPACT_ARRAY(COMPACT_STRING)");
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
                assertFalse(f.def.type.isNullable(), f + " should not be nullable");
            } finally {
                this.struct.set(f, o);
            }
        }
    }

    @Test
    public void testDefault() {
        Schema schema = new Schema(new Field("field", Type.INT32, "doc", 42));
        Struct struct = new Struct(schema);
        assertEquals(42, struct.get("field"), "Should get the default value");
        struct.validate(); // should be valid even with missing value
    }

    @Test
    public void testNullableDefault() {
        checkNullableDefault(Type.NULLABLE_BYTES, ByteBuffer.allocate(0));
        checkNullableDefault(Type.COMPACT_NULLABLE_BYTES, ByteBuffer.allocate(0));
        checkNullableDefault(Type.NULLABLE_STRING, "default");
        checkNullableDefault(Type.COMPACT_NULLABLE_STRING, "default");
    }

    private void checkNullableDefault(Type type, Object defaultValue) {
        // Should use default even if the field allows null values
        Schema schema = new Schema(new Field("field", type, "doc", defaultValue));
        Struct struct = new Struct(schema);
        assertEquals(defaultValue, struct.get("field"), "Should get the default value");
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
    public void testReadCompactArraySizeTooLarge() {
        Type type = new CompactArrayOf(Type.INT8);
        int size = 10;
        ByteBuffer invalidBuffer = ByteBuffer.allocate(
            ByteUtils.sizeOfUnsignedVarint(Integer.MAX_VALUE) + size);
        ByteUtils.writeUnsignedVarint(Integer.MAX_VALUE, invalidBuffer);
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
    public void testReadTaggedFieldsSizeTooLarge() {
        int tag = 1;
        Type type = TaggedFields.of(tag, new Field("field", Type.NULLABLE_STRING));
        int size = 10;
        ByteBuffer buffer = ByteBuffer.allocate(size);
        int numTaggedFields = 1;
        // write the number of tagged fields
        ByteUtils.writeUnsignedVarint(numTaggedFields, buffer);
        // write the tag of the first tagged fields
        ByteUtils.writeUnsignedVarint(tag, buffer);
        // write the size of tagged fields for this tag, using a large number for testing
        ByteUtils.writeUnsignedVarint(Integer.MAX_VALUE, buffer);
        int expectedRemaining = buffer.remaining();
        buffer.rewind();

        // should throw SchemaException while reading the buffer, instead of OOM
        Throwable e = assertThrows(SchemaException.class, () -> type.read(buffer));
        assertEquals("Error reading field of size " + Integer.MAX_VALUE + ", only " + expectedRemaining + " bytes available",
            e.getMessage());
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
    public void testReadZeroCompactArraySize() {
        Type type = new CompactArrayOf(Type.INT8);
        int size = 10;
        ByteBuffer invalidBuffer = ByteBuffer.allocate(
            ByteUtils.sizeOfUnsignedVarint(0) + size);
        ByteUtils.writeUnsignedVarint(0, invalidBuffer);
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
        assertNotNull(structStr, "Struct string should not be null.");
        assertFalse(structStr.isEmpty(), "Struct string should not be empty.");
    }

    private Object roundtrip(Type type, Object obj) {
        ByteBuffer buffer = ByteBuffer.allocate(type.sizeOf(obj));
        type.write(buffer, obj);
        assertFalse(buffer.hasRemaining(), "The buffer should now be full.");
        buffer.rewind();
        Object read = type.read(buffer);
        assertFalse(buffer.hasRemaining(), "All bytes should have been read.");
        return read;
    }

    private void check(Type type, Object obj, String expectedTypeName) {
        Object result = roundtrip(type, obj);
        if (obj instanceof Object[]) {
            obj = Arrays.asList((Object[]) obj);
            result = Arrays.asList((Object[]) result);
        }
        assertEquals(expectedTypeName, type.toString());
        assertEquals(obj, result, "The object read back should be the same as what was written.");
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

    @Test
    public void testReadIgnoringExtraDataAtTheEnd() {
        Schema oldSchema = new Schema(new Field("field1", Type.NULLABLE_STRING), new Field("field2", Type.NULLABLE_STRING));
        Schema newSchema = new Schema(new Field("field1", Type.NULLABLE_STRING));
        String value = "foo bar baz";
        Struct oldFormat = new Struct(oldSchema).set("field1", value).set("field2", "fine to ignore");
        ByteBuffer buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat));
        oldFormat.writeTo(buffer);
        buffer.flip();
        Struct newFormat = newSchema.read(buffer);
        assertEquals(value, newFormat.get("field1"));
    }

    @Test
    public void testReadWhenOptionalDataMissingAtTheEndIsTolerated() {
        Schema oldSchema = new Schema(new Field("field1", Type.NULLABLE_STRING));
        Schema newSchema = new Schema(
                true,
                new Field("field1", Type.NULLABLE_STRING),
                new Field("field2", Type.NULLABLE_STRING, "", true, "default"),
                new Field("field3", Type.NULLABLE_STRING, "", true, null),
                new Field("field4", Type.NULLABLE_BYTES, "", true, ByteBuffer.allocate(0)),
                new Field("field5", Type.INT64, "doc", true, Long.MAX_VALUE));
        String value = "foo bar baz";
        Struct oldFormat = new Struct(oldSchema).set("field1", value);
        ByteBuffer buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat));
        oldFormat.writeTo(buffer);
        buffer.flip();
        Struct newFormat = newSchema.read(buffer);
        assertEquals(value, newFormat.get("field1"));
        assertEquals("default", newFormat.get("field2"));
        assertNull(newFormat.get("field3"));
        assertEquals(ByteBuffer.allocate(0), newFormat.get("field4"));
        assertEquals(Long.MAX_VALUE, newFormat.get("field5"));
    }

    @Test
    public void testReadWhenOptionalDataMissingAtTheEndIsNotTolerated() {
        Schema oldSchema = new Schema(new Field("field1", Type.NULLABLE_STRING));
        Schema newSchema = new Schema(
                new Field("field1", Type.NULLABLE_STRING),
                new Field("field2", Type.NULLABLE_STRING, "", true, "default"));
        String value = "foo bar baz";
        Struct oldFormat = new Struct(oldSchema).set("field1", value);
        ByteBuffer buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat));
        oldFormat.writeTo(buffer);
        buffer.flip();
        SchemaException e = assertThrows(SchemaException.class, () -> newSchema.read(buffer));
        assertTrue(e.getMessage().contains("Error reading field 'field2':"));
    }

    @Test
    public void testReadWithMissingNonOptionalExtraDataAtTheEnd() {
        Schema oldSchema = new Schema(new Field("field1", Type.NULLABLE_STRING));
        Schema newSchema = new Schema(
                true,
                new Field("field1", Type.NULLABLE_STRING),
                new Field("field2", Type.NULLABLE_STRING));
        String value = "foo bar baz";
        Struct oldFormat = new Struct(oldSchema).set("field1", value);
        ByteBuffer buffer = ByteBuffer.allocate(oldSchema.sizeOf(oldFormat));
        oldFormat.writeTo(buffer);
        buffer.flip();
        SchemaException e = assertThrows(SchemaException.class, () -> newSchema.read(buffer));
        assertTrue(e.getMessage().contains("Missing value for field 'field2' which has no default value"));
    }

    @Test
    public void testReadBytesBeyondItsSize() {
        Type[] types = new Type[]{
            Type.BYTES,
            Type.COMPACT_BYTES,
            Type.NULLABLE_BYTES,
            Type.COMPACT_NULLABLE_BYTES
        };
        for (Type type : types) {
            ByteBuffer buffer = ByteBuffer.allocate(20);
            type.write(buffer, ByteBuffer.allocate(4));
            buffer.rewind();
            ByteBuffer bytes = (ByteBuffer) type.read(buffer);
            assertThrows(IllegalArgumentException.class, () -> bytes.limit(bytes.limit() + 1));
        }
    }
}
