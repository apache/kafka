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
package org.apache.kafka.connect.transforms.field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SingleFieldPathTest {

    @Test void shouldFindField() {
        Schema barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();

        assertEquals(barSchema.field("bar"), pathV2("foo.bar").fieldFrom(schema));
        assertEquals(schema.field("foo"), pathV2("foo").fieldFrom(schema));
    }

    @Test void shouldReturnNullFieldWhenFieldNotFound() {
        Schema barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();

        assertNull(pathV2("un.known").fieldFrom(schema));
        assertNull(pathV2("foo.unknown").fieldFrom(schema));
        assertNull(pathV2("unknown").fieldFrom(schema));
        assertNull(pathV2("test").fieldFrom(null));
    }

    @Test void shouldFindValueInMap() {
        Map<String, Object> foo = new HashMap<>();
        foo.put("bar", 42);
        foo.put("baz", null);
        Map<String, Object> map = new HashMap<>();
        map.put("foo", foo);

        assertEquals(42, pathV2("foo.bar").valueFrom(map));
        assertNull(pathV2("foo.baz").valueFrom(map));
    }

    @Test void shouldReturnNullValueWhenFieldNotFoundInMap() {
        Map<String, Object> foo = new HashMap<>();
        foo.put("bar", 42);
        foo.put("baz", null);
        Map<String, Object> map = new HashMap<>();
        map.put("foo", foo);

        assertNull(pathV2("un.known").valueFrom(map));
        assertNull(pathV2("foo.unknown").valueFrom(map));
        assertNull(pathV2("unknown").valueFrom(map));
        assertNull(pathV2("foo.baz.inner").valueFrom(map));
    }

    @Test void shouldFindValueInStruct() {
        Schema bazSchema = SchemaBuilder.struct()
            .field("inner", Schema.STRING_SCHEMA)
            .optional()
            .build();
        Schema barSchema = SchemaBuilder.struct()
            .field("bar", Schema.INT32_SCHEMA)
            .field("baz", bazSchema)
            .build();
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct foo = new Struct(barSchema)
            .put("bar", 42)
            .put("baz", null);
        Struct struct = new Struct(schema).put("foo", foo);

        assertEquals(42, pathV2("foo.bar").valueFrom(struct));
        assertNull(pathV2("foo.baz").valueFrom(struct));
    }

    @Test void shouldReturnNullValueWhenFieldNotFoundInStruct() {
        Schema bazSchema = SchemaBuilder.struct()
            .field("inner", Schema.STRING_SCHEMA)
            .optional()
            .build();
        Schema barSchema = SchemaBuilder.struct()
            .field("bar", Schema.INT32_SCHEMA)
            .field("baz", bazSchema)
            .build();
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct foo = new Struct(barSchema)
            .put("bar", 42)
            .put("baz", null);
        Struct struct = new Struct(schema).put("foo", foo);

        assertNull(pathV2("un.known").valueFrom(struct));
        assertNull(pathV2("foo.unknown").valueFrom(struct));
        assertNull(pathV2("unknown").valueFrom(struct));
        assertNull(pathV2("foo.baz.inner").valueFrom(struct));
    }

    private static SingleFieldPath pathV2(String path) {
        return new SingleFieldPath(path, FieldSyntaxVersion.V2);
    }
}