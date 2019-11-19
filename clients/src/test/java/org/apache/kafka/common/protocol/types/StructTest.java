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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StructTest {
    private static final Schema FLAT_STRUCT_SCHEMA = new Schema(
        new Field.Int8("int8", ""),
        new Field.Int16("int16", ""),
        new Field.Int32("int32", ""),
        new Field.Int64("int64", ""),
        new Field.Bool("boolean", ""),
        new Field.Str("string", ""));

    private static final Schema ARRAY_SCHEMA = new Schema(new Field.Array("array", new ArrayOf(Type.INT8), ""));
    private static final Schema NESTED_CHILD_SCHEMA = new Schema(
            new Field.Int8("int8", ""));
    private static final Schema NESTED_SCHEMA = new Schema(
            new Field.Array("array", ARRAY_SCHEMA, ""),
            new Field("nested", NESTED_CHILD_SCHEMA, ""));

    @Test
    public void testEquals() {
        Struct struct1 = new Struct(FLAT_STRUCT_SCHEMA)
                .set("int8", (byte) 12)
                .set("int16", (short) 12)
                .set("int32", 12)
                .set("int64", (long) 12)
                .set("boolean", true)
                .set("string", "foobar");
        Struct struct2 = new Struct(FLAT_STRUCT_SCHEMA)
                .set("int8", (byte) 12)
                .set("int16", (short) 12)
                .set("int32", 12)
                .set("int64", (long) 12)
                .set("boolean", true)
                .set("string", "foobar");
        Struct struct3 = new Struct(FLAT_STRUCT_SCHEMA)
                .set("int8", (byte) 12)
                .set("int16", (short) 12)
                .set("int32", 12)
                .set("int64", (long) 12)
                .set("boolean", true)
                .set("string", "mismatching string");

        assertEquals(struct1, struct2);
        assertNotEquals(struct1, struct3);

        Object[] array = {(byte) 1, (byte) 2};
        struct1 = new Struct(NESTED_SCHEMA)
                .set("array", array)
                .set("nested", new Struct(NESTED_CHILD_SCHEMA).set("int8", (byte) 12));
        Object[] array2 = {(byte) 1, (byte) 2};
        struct2 = new Struct(NESTED_SCHEMA)
                .set("array", array2)
                .set("nested", new Struct(NESTED_CHILD_SCHEMA).set("int8", (byte) 12));
        Object[] array3 = {(byte) 1, (byte) 2, (byte) 3};
        struct3 = new Struct(NESTED_SCHEMA)
                .set("array", array3)
                .set("nested", new Struct(NESTED_CHILD_SCHEMA).set("int8", (byte) 13));

        assertEquals(struct1, struct2);
        assertNotEquals(struct1, struct3);
    }
}
