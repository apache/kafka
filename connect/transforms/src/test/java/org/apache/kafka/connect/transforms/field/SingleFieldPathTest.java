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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.Test;

class SingleFieldPathTest {
    final static String[] EMPTY_PATH = new String[]{};

    @Test void shouldBuildV1WithDotsAndBacktickPair() {
        assertArrayEquals(
                new String[] {"foo.bar.baz"},
                new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V1).path());
        assertArrayEquals(
                new String[] {"foo.`bar.baz`"},
                new SingleFieldPath("foo.`bar.baz`", FieldSyntaxVersion.V1).path());
    }

    @Test void shouldBuildV2WithEmptyPath() {
        assertArrayEquals(EMPTY_PATH, new SingleFieldPath("", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldBuildV2WithoutDots() {
        assertArrayEquals(new String[] {"foobarbaz"}, new SingleFieldPath("foobarbaz", FieldSyntaxVersion.V2).path());
    }
    @Test void shouldBuildV2WithoutWrappingBackticks() {
        assertArrayEquals(new String[] {"foo`bar`baz"}, new SingleFieldPath("foo`bar`baz", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldBuildV2WhenIncludesDots() {
        assertArrayEquals(new String[] {"foo", "bar", "baz"}, new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldBuildV2WhenIncludesDotsAndBacktickPair() {
        assertArrayEquals(new String[] {"foo", "bar.baz"}, new SingleFieldPath("foo.`bar.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar", "baz"}, new SingleFieldPath("foo.`bar`.baz", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldBuildV2AndIgnoreBackticksThatAreNotWrapping() {
        assertArrayEquals(new String[] {"foo", "`bar.baz"}, new SingleFieldPath("foo.``bar.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar.baz`"}, new SingleFieldPath("foo.`bar.baz``", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "ba`r.baz"}, new SingleFieldPath("foo.`ba`r.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "ba`r", "baz"}, new SingleFieldPath("foo.ba`r.baz", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldBuildV2AndEscapeBackticks() {
        assertArrayEquals(new String[] {"foo", "bar`.baz"}, new SingleFieldPath("foo.`bar\\`.baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar.`baz"}, new SingleFieldPath("foo.`bar.`baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar`.`baz"}, new SingleFieldPath("foo.`bar\\`.`baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar\\`.`baz"}, new SingleFieldPath("foo.`bar\\\\`.`baz`", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"foo", "bar`.\\`baz"}, new SingleFieldPath("foo.`bar\\`.\\`baz`", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldBuildV2WithBackticksWrappingBackticks() {
        assertArrayEquals(new String[] {"foo", "`bar`", "baz"}, new SingleFieldPath("foo.``bar``.baz", FieldSyntaxVersion.V2).path());
        assertArrayEquals(new String[] {"`foo.bar.baz`"}, new SingleFieldPath("``foo.bar.baz``", FieldSyntaxVersion.V2).path());
    }

    @Test void shouldFilterSchemaV1Fields() {
        Schema schema = SchemaBuilder.struct().field("foo", Schema.STRING_SCHEMA)
            .field("bar", Schema.STRING_SCHEMA)
            .field("baz", Schema.INT32_SCHEMA)
            .build();

        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Schema result = new SingleFieldPath("foo", FieldSyntaxVersion.V1)
            .updateSchemaFrom(schema, updated, (builder, field, path) -> {
                // ignore field
            });

        assertEquals(2, result.fields().size());
        assertEquals("bar", result.fields().get(0).name());
        assertEquals("baz", result.fields().get(1).name());
    }
    @Test void shouldFilterSchemaV2Fields() {
        Schema schema = SchemaBuilder.struct().field("foo",
            SchemaBuilder.struct().field("bar", Schema.STRING_SCHEMA)
                .field("baz", Schema.INT32_SCHEMA))
            .build();

        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        SingleFieldPath fieldPath = new SingleFieldPath("foo.baz", FieldSyntaxVersion.V2);
        Schema result = fieldPath.updateSchemaFrom(
                schema,
                updated, (builder, field, path) -> {
                        // ignore field
                });

        assertEquals(1, result.fields().size());
        assertEquals(1, result.field("foo").schema().fields().size());
        assertEquals("bar", result.field("foo").schema().fields().get(0).name());
    }

    @Test void shouldRenameSchemaV1Fields() {
        Schema schema = SchemaBuilder.struct()
                .field("foo", Schema.STRING_SCHEMA)
                .field("bar", Schema.STRING_SCHEMA)
                .field("baz", Schema.INT32_SCHEMA)
                .build();

        SingleFieldPath fieldPath = new SingleFieldPath("foo", FieldSyntaxVersion.V1);
        Schema result = fieldPath.updateSchemaFrom(
                schema,
                (builder, field, path) -> builder.field("other", field.schema())
        );

        assertEquals(3, result.fields().size());
        assertEquals("other", result.fields().get(0).name());
        assertEquals("bar", result.fields().get(1).name());
        assertEquals("baz", result.fields().get(2).name());
    }

    @Test void shouldRenameSchemaV2Fields() {
        SchemaBuilder nested = SchemaBuilder.struct()
                .field("bar", Schema.STRING_SCHEMA)
                .field("baz", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct()
                .field("foo", nested)
                .build();

        SingleFieldPath fieldPath = new SingleFieldPath("foo.baz", FieldSyntaxVersion.V2);
        Schema result = fieldPath.updateSchemaFrom(
                schema,
                (builder, field, path) -> builder.field("other", field.schema())
        );

        assertEquals(1, result.fields().size());
        assertEquals(2, result.field("foo").schema().fields().size());
        assertEquals("bar", result.field("foo").schema().fields().get(0).name());
        assertEquals("other", result.field("foo").schema().fields().get(1).name());
    }

    @Test void shouldUpdateValueV1FromSchemaless() {
        Map<String, Object> value = Collections.singletonMap("foo", 42);

        SingleFieldPath fieldPath = new SingleFieldPath("foo", FieldSyntaxVersion.V1);
        Map<String, Object> updated = fieldPath
            .updateValueFrom(value, (orig, map, f, k) -> map.put(k, ((Integer) orig.get(k)) * 2));

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test void shouldUpdateNestedValueV2FromSchemaless() {
        Map<String, Object> value = Collections.singletonMap("foo", Collections.singletonMap("bar", 42));

        SingleFieldPath fieldPath = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2);
        Map<String, Object> updated = fieldPath.updateValueFrom(
                value,
                (original, map, f, k) -> map.put(k, ((Integer) original.get(k)) * 2)
        );

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test void shouldUpdateValueV1WithSchema() {
        Schema schema = SchemaBuilder.struct().field("foo", Schema.INT32_SCHEMA).build();
        Struct value = new Struct(schema).put("foo", 42);

        SingleFieldPath fieldPath = new SingleFieldPath("foo", FieldSyntaxVersion.V1);
        Struct updated = fieldPath.updateValueFrom(schema, value, schema,
                (orig, oldField, s, updatedField, f) -> s.put(updatedField, ((Integer) orig.get(oldField)) * 2));

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test void shouldUpdateNestedValueV2WithSchema() {
        SchemaBuilder barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct value = new Struct(schema).put("foo", new Struct(barSchema).put("bar", 42));

        SingleFieldPath fieldPath = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2);
        Struct updated = fieldPath.updateValueFrom(schema, value, schema,
                (orig, oldField, s, updatedField, f) -> s.put(updatedField, ((Integer) orig.get(oldField)) * 2));

        assertEquals(84, fieldPath.valueFrom(updated));
    }

    @Test
    void shouldIncludeEmptyFieldNames() {
        assertArrayEquals(
            new String[] {"", "", ""},
            new SingleFieldPath("..", FieldSyntaxVersion.V2).path()
        );
        assertArrayEquals(
            new String[] {"foo", "", ""},
            new SingleFieldPath("foo..", FieldSyntaxVersion.V2).path()
        );
        assertArrayEquals(
            new String[] {"", "bar", ""},
            new SingleFieldPath(".bar.", FieldSyntaxVersion.V2).path()
        );
        assertArrayEquals(
            new String[] {"", "", "baz"},
            new SingleFieldPath("..baz", FieldSyntaxVersion.V2).path()
        );
    }

    @Test void shouldReturnNullFieldWhenFieldNotFound() {
        SchemaBuilder barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();

        assertNull(new SingleFieldPath("un.known", FieldSyntaxVersion.V2).fieldFrom(schema));
        assertNull(new SingleFieldPath("foo.unknown", FieldSyntaxVersion.V2).fieldFrom(schema));
        assertNull(new SingleFieldPath("unknown", FieldSyntaxVersion.V2).fieldFrom(schema));
    }

    @Test void shouldReturnNullValueWhenFieldNotFound() {
        SchemaBuilder barSchema = SchemaBuilder.struct().field("bar", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct().field("foo", barSchema).build();
        Struct struct = new Struct(schema).put("foo", new Struct(barSchema).put("bar", 42));

        assertNull(new SingleFieldPath("un.known", FieldSyntaxVersion.V2).valueFrom(struct));
        assertNull(new SingleFieldPath("foo.unknown", FieldSyntaxVersion.V2).valueFrom(struct));
        assertNull(new SingleFieldPath("unknown", FieldSyntaxVersion.V2).valueFrom(struct));
    }
}