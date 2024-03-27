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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MultiFieldPathsTest {

    @Test void shouldBuildEmptyTrie() {
        MultiFieldPaths.Trie trie = new MultiFieldPaths.Trie();
        assertTrue(trie.isEmpty());
    }

    @Test void shouldBuildMultiPathWithSinglePathV1() {
        SingleFieldPath path = new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V1);
        MultiFieldPaths paths = createMultiFieldPaths(path);
        assertFalse(paths.trie.isEmpty());
        assertEquals(1, paths.trie.size());

        final Optional<MultiFieldPaths.TrieNode> maybeFoo = paths.trie.find("foo.bar.baz");
        assertTrue(maybeFoo.isPresent());
        assertEquals(path, maybeFoo.get().path);
    }

    @Test void shouldBuildMultiPathWithSinglePathV2() {
        SingleFieldPath path = new SingleFieldPath("foo.bar.baz", FieldSyntaxVersion.V2);
        MultiFieldPaths paths = createMultiFieldPaths(path);
        assertFalse(paths.trie.isEmpty());
        assertEquals(1, paths.trie.size());

        final Optional<MultiFieldPaths.TrieNode> maybeV1 = paths.trie.find("foo.bar.baz");
        assertFalse(maybeV1.isPresent());
        final Optional<MultiFieldPaths.TrieNode> maybeFoo = paths.trie.find("foo");
        assertTrue(maybeFoo.isPresent());
        assertFalse(maybeFoo.get().isLeaf());
        final Optional<MultiFieldPaths.TrieNode> maybeBar = maybeFoo.get().find("bar");
        assertTrue(maybeBar.isPresent());
        assertFalse(maybeBar.get().isLeaf());
        final Optional<MultiFieldPaths.TrieNode> maybeBaz = maybeBar.get().find("baz");
        assertTrue(maybeBaz.isPresent());
        assertTrue(maybeBaz.get().isLeaf());
        assertEquals(path, maybeBaz.get().path);
    }


    @Test void shouldBuildMultiPathWithMultipleSinglePathV2() {
        MultiFieldPaths paths = createMultiFieldPaths(
            new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2),
            new SingleFieldPath("foo.baz", FieldSyntaxVersion.V2),
            new SingleFieldPath("test", FieldSyntaxVersion.V2));
        assertFalse(paths.trie.isEmpty());
        assertEquals(3, paths.trie.size());

        final Optional<MultiFieldPaths.TrieNode> maybeFoo = paths.trie.find("foo");
        assertTrue(maybeFoo.isPresent());
        assertFalse(maybeFoo.get().isLeaf());
        final Optional<MultiFieldPaths.TrieNode> maybeBar = maybeFoo.get().find("bar");
        assertTrue(maybeBar.isPresent());
        assertTrue(maybeBar.get().isLeaf());
        final Optional<MultiFieldPaths.TrieNode> maybeBaz = maybeFoo.get().find("baz");
        assertTrue(maybeBaz.isPresent());
        assertTrue(maybeBaz.get().isLeaf());
        final Optional<MultiFieldPaths.TrieNode> maybeTest = paths.trie.find("test");
        assertTrue(maybeTest.isPresent());
        assertTrue(maybeTest.get().isLeaf());
    }

    @Test void shouldFlatOverlappingPaths() {
        final SingleFieldPath foo = new SingleFieldPath("foo", FieldSyntaxVersion.V2);
        final SingleFieldPath fooBar = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2);

        MultiFieldPaths.Trie trie1 = new MultiFieldPaths.Trie();
        trie1.insert(foo);
        trie1.insert(fooBar);
        assertFalse(trie1.isEmpty());
        assertEquals(1, trie1.size());

        final Optional<MultiFieldPaths.TrieNode> maybeFoo1 = trie1.find("foo");
        assertTrue(maybeFoo1.isPresent());
        final MultiFieldPaths.TrieNode fooNode1 = maybeFoo1.get();
        assertFalse(fooNode1.isLeaf());
        final Optional<MultiFieldPaths.TrieNode> maybeBar1 = fooNode1.find("bar");
        assertTrue(maybeBar1.isPresent());
        final MultiFieldPaths.TrieNode barNode = maybeBar1.get();
        assertTrue(barNode.isLeaf());
        assertEquals(fooBar, barNode.path);

        MultiFieldPaths.Trie trie2 = new MultiFieldPaths.Trie();
        trie2.insert(fooBar);
        trie2.insert(foo);
        assertFalse(trie2.isEmpty());
        assertEquals(1, trie2.size());

        final Optional<MultiFieldPaths.TrieNode> maybeFoo2 = trie2.find("foo");
        assertTrue(maybeFoo2.isPresent());
        final MultiFieldPaths.TrieNode fooNode2 = maybeFoo2.get();
        assertFalse(fooNode2.isLeaf());
        final Optional<MultiFieldPaths.TrieNode> barNode2 = fooNode2.find("bar");
        assertTrue(barNode2.isPresent());
        assertTrue(barNode2.get().isLeaf());
        assertEquals(fooBar, barNode2.get().path);

        assertEquals(trie1, trie2);
    }

    @Test void shouldRenameSchemaV1Fields() {
        Schema schema = SchemaBuilder.struct()
                .field("foo", Schema.STRING_SCHEMA)
                .field("bar", Schema.STRING_SCHEMA)
                .field("baz", Schema.INT32_SCHEMA)
                .build();

        MultiFieldPaths fieldPath = MultiFieldPaths.of(Arrays.asList("foo", "bar"), FieldSyntaxVersion.V1);
        SchemaBuilder updated = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        Schema result = fieldPath.updateSchemaFrom(
                schema,
                updated,
                (builder, field, path) -> builder.field(field.name() + "_other", field.schema())
        );

        assertEquals(3, result.fields().size());
        assertEquals("foo_other", result.fields().get(0).name());
        assertEquals("bar_other", result.fields().get(1).name());
        assertEquals("baz", result.fields().get(2).name());
    }

    @Test void shouldRenameSchemaV2Fields() {
        SchemaBuilder nested = SchemaBuilder.struct()
                .field("bar", Schema.STRING_SCHEMA)
                .field("baz", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct()
                .field("foo", nested)
                .build();

        MultiFieldPaths fieldPath = MultiFieldPaths.of(Arrays.asList("foo.baz", "foo.bar"), FieldSyntaxVersion.V2);
        Schema result = fieldPath.updateSchemaFrom(
                schema,
                (builder, field, path) -> builder.field(field.name() + "_other", field.schema())
        );

        assertEquals(1, result.fields().size());
        assertEquals(2, result.field("foo").schema().fields().size());
        assertEquals("bar_other", result.field("foo").schema().fields().get(0).name());
        assertEquals("baz_other", result.field("foo").schema().fields().get(1).name());
    }

    @Test void shouldUpdateValuesV1FromSchemaless() {
        Map<String, Object> value = new HashMap<>();
        value.put("foo", 42);
        value.put("bar", 21);

        SingleFieldPath fooPath = new SingleFieldPath("foo", FieldSyntaxVersion.V1);
        SingleFieldPath barPath = new SingleFieldPath("bar", FieldSyntaxVersion.V1);
        MultiFieldPaths fieldPaths = createMultiFieldPaths(fooPath, barPath);
        Map<String, Object> updated = fieldPaths.updateValuesFrom(
                value,
                (orig, map, f, k) -> map.put(k, ((Integer) orig.get(k)) * 2)
        );

        Map<SingleFieldPath, Map.Entry<String, Object>> actual = fieldPaths.fieldAndValuesFrom(updated);
        assertEquals(84, actual.get(fooPath).getValue());
        assertEquals(42, actual.get(barPath).getValue());
    }

    @Test void shouldUpdateNestedValuesV2FromSchemaless() {
        Map<String, Object> nested = new HashMap<>();
        nested.put("bar", 21);
        nested.put("baz", 42);
        Map<String, Object> value = Collections.singletonMap("foo", nested);

        SingleFieldPath barPath = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2);
        SingleFieldPath bazPath = new SingleFieldPath("foo.baz", FieldSyntaxVersion.V2);
        MultiFieldPaths fieldPaths = createMultiFieldPaths(bazPath, barPath);
        Map<String, Object> updated = fieldPaths.updateValuesFrom(
                value,
                (orig, map, f, k) -> map.put(k, ((Integer) orig.get(k)) * 2)
        );

        Map<SingleFieldPath, Map.Entry<String, Object>> actual = fieldPaths.fieldAndValuesFrom(updated);
        assertEquals(84, actual.get(bazPath).getValue());
        assertEquals(42, actual.get(barPath).getValue());
    }

    @Test void shouldUpdateValueV1WithSchema() {
        Schema schema = SchemaBuilder.struct()
                .field("foo.bar", Schema.INT32_SCHEMA)
                .field("foo.baz", Schema.INT32_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("foo.bar", 21)
                .put("foo.baz", 42);

        SingleFieldPath bazPath = new SingleFieldPath("foo.baz", FieldSyntaxVersion.V1);
        SingleFieldPath barPath = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V1);
        MultiFieldPaths fieldPaths = createMultiFieldPaths(bazPath, barPath);
        Struct updated = fieldPaths.updateValuesFrom(schema, value, schema,
                (orig, oldField, s, updatedField, f) -> s.put(updatedField, ((Integer) orig.get(oldField)) * 2));

        Map<SingleFieldPath, Map.Entry<Field, Object>> actual = fieldPaths.fieldAndValuesFrom(updated);
        assertEquals(84, actual.get(bazPath).getValue());
        assertEquals(42, actual.get(barPath).getValue());
    }

    @Test void shouldUpdateNestedValueV2WithSchema() {
        SchemaBuilder nestedSchema = SchemaBuilder.struct()
                .field("bar", Schema.INT32_SCHEMA)
                .field("baz", Schema.INT32_SCHEMA);
        Schema schema = SchemaBuilder.struct()
                .field("foo", nestedSchema)
                .build();
        Struct nested = new Struct(nestedSchema)
                .put("bar", 21)
                .put("baz", 42);
        Struct value = new Struct(schema).put("foo", nested);

        SingleFieldPath bazPath = new SingleFieldPath("foo.baz", FieldSyntaxVersion.V2);
        SingleFieldPath barPath = new SingleFieldPath("foo.bar", FieldSyntaxVersion.V2);
        MultiFieldPaths fieldPaths = createMultiFieldPaths(bazPath, barPath);
        Struct updated = fieldPaths.updateValuesFrom(schema, value, schema,
                (orig, oldField, s, updatedField, f) -> s.put(updatedField, ((Integer) orig.get(oldField)) * 2));

        Map<SingleFieldPath, Map.Entry<Field, Object>> actual = fieldPaths.fieldAndValuesFrom(updated);
        assertEquals(84, actual.get(bazPath).getValue());
        assertEquals(42, actual.get(barPath).getValue());
    }

    static MultiFieldPaths createMultiFieldPaths(SingleFieldPath... fields) {
        return new MultiFieldPaths(new HashSet<>(Arrays.asList(fields)));
    }
}