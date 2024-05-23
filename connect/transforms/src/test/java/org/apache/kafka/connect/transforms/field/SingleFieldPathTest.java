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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

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

    private static Integer update(Object field) {
        return field != null ? ((Integer) field) * 2 : null;
    }

    static Stream<Arguments> updateMapParams() {
        return Stream.of(
            Arguments.of(Collections.singletonMap("foo", 42), pathV1("foo"), Collections.singletonMap("foo", 84)),
            Arguments.of(Collections.singletonMap("foo", 42), pathV1("bar"), Collections.singletonMap("foo", 42)),
            Arguments.of(
                Collections.singletonMap("foo", Collections.singletonMap("bar", 42)),
                pathV2("foo.bar"),
                Collections.singletonMap("foo", Collections.singletonMap("bar", 84))
            ),
            Arguments.of(
                Collections.singletonMap("foo", Collections.singletonMap("bar", 42)),
                pathV2("foo.baz"),
                Collections.singletonMap("foo", Collections.singletonMap("bar", 42))
            )
        );
    }

    @ParameterizedTest
    @MethodSource("updateMapParams")
    void shouldUpdateMap(Map<String, Object> input, SingleFieldPath path, Map<String, Object> output) {
        Map<String, Object> updated = path.updateMap(
            input,
            SingleFieldPathTest::update
        );
        assertEquals(output, updated);
    }

    static Stream<Arguments> updateStructParams() {
        Schema barV2Schema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA).build();
        Schema fooV2Schema = SchemaBuilder.struct().field("foo", barV2Schema).build();
        Schema fooV1Schema = SchemaBuilder.struct()
            .field("foo", Schema.INT32_SCHEMA)
            .build();
        return Stream.of(
            Arguments.of(
                new Struct(fooV1Schema).put("foo", 42),
                pathV1("foo"),
                new Struct(fooV1Schema).put("foo", 84)
            ),
            Arguments.of(
                new Struct(fooV1Schema).put("foo", 42),
                pathV1("bar"),
                new Struct(fooV1Schema).put("foo", 42)
            ),
            Arguments.of(
                new Struct(fooV2Schema).put("foo", new Struct(barV2Schema).put("bar", 42)),
                pathV2("foo.bar"),
                new Struct(fooV2Schema).put("foo", new Struct(barV2Schema).put("bar", 84))
            ),
            Arguments.of(
                new Struct(fooV2Schema).put("foo", new Struct(barV2Schema).put("bar", 42)),
                pathV2("foo.baz"),
                new Struct(fooV2Schema).put("foo", new Struct(barV2Schema).put("bar", 42))
            )
        );
    }

    @ParameterizedTest
    @MethodSource("updateStructParams")
    void shouldUpdateStruct(Struct input, SingleFieldPath path, Struct output) {
        Struct updated = path.updateStruct(
            input,
            input.schema(),
            (fieldValue, fieldSchema) -> update(fieldValue)
        );
        assertEquals(output, updated);
    }

    static Stream<Arguments> updateSchemaParams() {
        Schema fooV1Schema = SchemaBuilder.struct().field("foo", Schema.INT32_SCHEMA).build();
        Schema resultFooV1Schema = SchemaBuilder.struct().field("foo", Schema.STRING_SCHEMA).build();
        Schema barV2Schema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA).build();
        Schema fooV2Schema = SchemaBuilder.struct().field("foo", barV2Schema).build();
        Schema resultBarV2Schema = SchemaBuilder.struct().field("bar", Schema.STRING_SCHEMA).build();
        Schema resultFooV2Schema = SchemaBuilder.struct().field("foo", resultBarV2Schema).build();
        return Stream.of(
            Arguments.of(fooV1Schema, pathV1("foo"), resultFooV1Schema),
            Arguments.of(fooV1Schema, pathV1("bar"), fooV1Schema),
            Arguments.of(fooV2Schema, pathV2("foo.bar"), resultFooV2Schema),
            Arguments.of(fooV2Schema, pathV2("foo.baz"), fooV2Schema)
        );
    }

    @ParameterizedTest
    @MethodSource("updateSchemaParams")
    void shouldUpdateSchema(Schema input, SingleFieldPath path, Schema output) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        Schema updated = path.updateSchema(
            input,
            schemaBuilder,
            field -> field != null ? Schema.STRING_SCHEMA : null
        );
        assertEquals(output, updated);
    }

    private static SingleFieldPath pathV1(String path) {
        return new SingleFieldPath(path, FieldSyntaxVersion.V1);
    }

    private static SingleFieldPath pathV2(String path) {
        return new SingleFieldPath(path, FieldSyntaxVersion.V2);
    }
}