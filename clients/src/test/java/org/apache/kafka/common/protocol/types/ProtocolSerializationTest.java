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
 */
package org.apache.kafka.common.protocol.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

public class ProtocolSerializationTest {

    private Schema schema;
    private Struct struct;

    @Before
    public void setup() {
        this.schema = new Schema(new Field("int8", Type.INT8),
                                 new Field("int16", Type.INT16),
                                 new Field("int32", Type.INT32),
                                 new Field("int64", Type.INT64),
                                 new Field("string", Type.STRING),
                                 new Field("bytes", Type.BYTES),
                                 new Field("nullable_bytes", Type.NULLABLE_BYTES),
                                 new Field("array", new ArrayOf(Type.INT32)),
                                 new Field("struct", new Schema(new Field("field", new ArrayOf(Type.INT32)))));
        this.struct = new Struct(this.schema).set("int8", (byte) 1)
                                             .set("int16", (short) 1)
                                             .set("int32", 1)
                                             .set("int64", 1L)
                                             .set("string", "1")
                                             .set("bytes", ByteBuffer.wrap("1".getBytes()))
                                             .set("nullable_bytes", null)
                                             .set("array", new Object[] {1});
        this.struct.set("struct", this.struct.instance("struct").set("field", new Object[] {1, 2, 3}));
    }

    @Test
    public void testSimple() {
        check(Type.INT8, (byte) -111);
        check(Type.INT16, (short) -11111);
        check(Type.INT32, -11111111);
        check(Type.INT64, -11111111111L);
        check(Type.STRING, "");
        check(Type.STRING, "hello");
        check(Type.STRING, "A\u00ea\u00f1\u00fcC");
        check(Type.BYTES, ByteBuffer.allocate(0));
        check(Type.BYTES, ByteBuffer.wrap("abcd".getBytes()));
        check(Type.NULLABLE_BYTES, null);
        check(Type.NULLABLE_BYTES, ByteBuffer.allocate(0));
        check(Type.NULLABLE_BYTES, ByteBuffer.wrap("abcd".getBytes()));
        check(new ArrayOf(Type.INT32), new Object[] {1, 2, 3, 4});
        check(new ArrayOf(Type.STRING), new Object[] {});
        check(new ArrayOf(Type.STRING), new Object[] {"hello", "there", "beautiful"});
    }

    @Test
    public void testNulls() {
        for (Field f : this.schema.fields()) {
            Object o = this.struct.get(f);
            try {
                this.struct.set(f, null);
                this.struct.validate();
                if (!f.type.isNullable())
                    fail("Should not allow serialization of null value.");
            } catch (SchemaException e) {
                assertFalse(f.type.isNullable());
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
        // Should use default even if the field allows null values
        ByteBuffer empty = ByteBuffer.allocate(0);
        Schema schema = new Schema(new Field("field", Type.NULLABLE_BYTES, "doc", empty));
        Struct struct = new Struct(schema);
        assertEquals("Should get the default value", empty, struct.get("field"));
        struct.validate(); // should be valid even with missing value
    }

    @Test
    public void testArray() {
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

}
