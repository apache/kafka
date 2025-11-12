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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class StructTest {
    private static final Schema FLAT_STRUCT_SCHEMA = new Schema(
        new Field("int8", Type.INT8, ""),
        new Field("int16", Type.INT16, ""),
        new Field("int32", Type.INT32, ""),
        new Field("int64", Type.INT64, ""),
        new Field("boolean", Type.BOOLEAN, ""),
        new Field("float64", Type.FLOAT64, ""),
        new Field("string", Type.STRING, ""));

    private static final Schema ARRAY_SCHEMA = new Schema(new Field("array", new ArrayOf(new ArrayOf(Type.INT8)), ""));
    private static final Schema NESTED_CHILD_SCHEMA = new Schema(
            new Field("int8", Type.INT8, ""));
    private static final Schema NESTED_SCHEMA = new Schema(
            new Field("array", new ArrayOf(ARRAY_SCHEMA), ""),
            new Field("nested", NESTED_CHILD_SCHEMA, ""));

    @Test
    public void testEquals() {
        Struct struct1 = new Struct(FLAT_STRUCT_SCHEMA)
                .set("int8", (byte) 12)
                .set("int16", (short) 12)
                .set("int32", 12)
                .set("int64", (long) 12)
                .set("boolean", true)
                .set("float64", 0.5)
                .set("string", "foobar");
        Struct struct2 = new Struct(FLAT_STRUCT_SCHEMA)
                .set("int8", (byte) 12)
                .set("int16", (short) 12)
                .set("int32", 12)
                .set("int64", (long) 12)
                .set("boolean", true)
                .set("float64", 0.5)
                .set("string", "foobar");
        Struct struct3 = new Struct(FLAT_STRUCT_SCHEMA)
                .set("int8", (byte) 12)
                .set("int16", (short) 12)
                .set("int32", 12)
                .set("int64", (long) 12)
                .set("boolean", true)
                .set("float64", 0.5)
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
