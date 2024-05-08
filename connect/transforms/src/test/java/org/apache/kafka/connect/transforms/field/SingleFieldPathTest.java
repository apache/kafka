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
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.Test;

import java.util.Collections;
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

    @Test
    void shouldFilterSchemaV1Fields() {
        Schema schema = SchemaBuilder.struct().field("foo", Schema.STRING_SCHEMA)
            .field("bar", Schema.STRING_SCHEMA)
            .field("baz", Schema.INT32_SCHEMA)
            .build();

        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Schema result = pathV1("foo").updateSchemaFrom(
            schema,
            updated,
            StructSchemaUpdater.FILTER_OUT_FIELD,
            StructSchemaUpdater.FILTER_OUT_FIELD,
            StructSchemaUpdater.PASS_THROUGH_FIELD
        );

        assertEquals(2, result.fields().size());
        assertEquals("bar", result.fields().get(0).name());
        assertEquals("baz", result.fields().get(1).name());
    }

    @Test
    void shouldFilterSchemaV2Fields() {
        Schema schema = SchemaBuilder.struct().field("foo",
                SchemaBuilder.struct().field("bar", Schema.STRING_SCHEMA)
                    .field("baz", Schema.INT32_SCHEMA))
            .build();

        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Schema result = pathV2("foo.baz").updateSchemaFrom(
            schema,
            updated,
            StructSchemaUpdater.FILTER_OUT_FIELD,
            StructSchemaUpdater.FILTER_OUT_FIELD,
            StructSchemaUpdater.PASS_THROUGH_FIELD
        );

        assertEquals(1, result.fields().size());
        assertEquals(1, result.field("foo").schema().fields().size());
        assertEquals("bar", result.field("foo").schema().fields().get(0).name());
    }

    @Test
    void shouldRenameSchemaV1Fields() {
        Schema schema = SchemaBuilder.struct()
            .field("foo", Schema.STRING_SCHEMA)
            .field("bar", Schema.STRING_SCHEMA)
            .field("baz", Schema.INT32_SCHEMA)
            .build();

        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Schema result = pathV1("foo").updateSchemaFrom(
            schema,
            updated,
            (builder, field, path) -> builder.field("other", field.schema()),
            StructSchemaUpdater.FILTER_OUT_FIELD,
            StructSchemaUpdater.PASS_THROUGH_FIELD
        );

        assertEquals(3, result.fields().size());
        assertEquals("other", result.fields().get(0).name());
        assertEquals("bar", result.fields().get(1).name());
        assertEquals("baz", result.fields().get(2).name());
    }

    @Test
    void shouldRenameSchemaV2Fields() {
        SchemaBuilder nested = SchemaBuilder.struct()
            .field("bar", Schema.STRING_SCHEMA)
            .field("baz", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct()
            .field("foo", nested)
            .build();

        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Schema result = pathV2("foo.baz").updateSchemaFrom(
            schema,
            updated,
            (builder, field, path) -> builder.field("other", field.schema()),
            StructSchemaUpdater.FILTER_OUT_FIELD,
            StructSchemaUpdater.PASS_THROUGH_FIELD
        );

        assertEquals(1, result.fields().size());
        assertEquals(2, result.field("foo").schema().fields().size());
        assertEquals("bar", result.field("foo").schema().fields().get(0).name());
        assertEquals("other", result.field("foo").schema().fields().get(1).name());
    }

    @Test
    void shouldUpdateValueV1FromSchemaless() {
        Map<String, Object> value = Collections.singletonMap("foo", 42);

        SingleFieldPath fieldPath = pathV1("foo");
        Map<String, Object> updated = fieldPath.updateValueFrom(
            value,
            (originalParent, updatedParent, field, name) ->
                updatedParent.put(name, ((Integer) originalParent.get(name)) * 2),
            MapValueUpdater.FILTER_OUT_FIELD,
            MapValueUpdater.PASS_THROUGH_FIELD
        );

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test
    void shouldUpdateNestedValueV2FromSchemaless() {
        Map<String, Object> value = Collections.singletonMap("foo", Collections.singletonMap("bar", 42));

        SingleFieldPath fieldPath = pathV2("foo.bar");
        Map<String, Object> updated = fieldPath.updateValueFrom(
            value,
            (originalParent, updatedParent, field, name) ->
                updatedParent.put(name, ((Integer) originalParent.get(name)) * 2),
            MapValueUpdater.FILTER_OUT_FIELD,
            MapValueUpdater.PASS_THROUGH_FIELD
        );

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test
    void shouldUpdateValueV1WithSchema() {
        Schema schema = SchemaBuilder.struct().field("foo", Schema.INT32_SCHEMA).build();
        Struct value = new Struct(schema).put("foo", 42);

        SingleFieldPath fieldPath = pathV1("foo");
        Struct updated = fieldPath.updateValueFrom(
            schema, value, schema,
            (origParent, oldField, updatedParent, updatedField, f) ->
                updatedParent.put(updatedField, ((Integer) origParent.get(oldField)) * 2),
            StructValueUpdater.FILTER_OUT_VALUE,
            StructValueUpdater.PASS_THROUGH_VALUE
        );

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test
    void shouldUpdateNestedValueV2WithSchema() {
        SchemaBuilder barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct value = new Struct(schema).put("foo", new Struct(barSchema).put("bar", 42));

        SingleFieldPath fieldPath = pathV2("foo.bar");
        Struct updated = fieldPath.updateValueFrom(
            schema, value, schema,
            (origParent, oldField, updatedParent, updatedField, f) ->
                updatedParent.put(updatedField, ((Integer) origParent.get(oldField)) * 2),
            StructValueUpdater.FILTER_OUT_VALUE,
            StructValueUpdater.PASS_THROUGH_VALUE
        );

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    private static SingleFieldPath pathV1(String path) {
        return new SingleFieldPath(path, FieldSyntaxVersion.V1);
    }

    private static SingleFieldPath pathV2(String path) {
        return new SingleFieldPath(path, FieldSyntaxVersion.V2);
    }
}