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
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Multiple field paths to access data objects ({@code Struct} or {@code Map}) efficiently,
 * instead of multiple individual {@link SingleFieldPath single-field paths}.
 *
 * <p>If the SMT requires accessing a single field on the same data object,
 * use {@link SingleFieldPath} instead.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-821%3A+Connect+Transforms+support+for+nested+structures">KIP-821</a>
 * @see SingleFieldPath
 * @see FieldSyntaxVersion
 */
public class MultiFieldPaths {
    final Trie trie = new Trie();

    MultiFieldPaths(Set<SingleFieldPath> paths) {
        paths.forEach(trie::insert);
    }

    public static MultiFieldPaths of(List<String> fields, FieldSyntaxVersion syntaxVersion) {
        return new MultiFieldPaths(fields.stream()
            .map(f -> new SingleFieldPath(f, syntaxVersion))
            .collect(Collectors.toSet()));
    }

    /**
     * Find values at the field paths
     *
     * @param struct data value
     * @return map of field paths and field/values
     */
    public Map<SingleFieldPath, Map.Entry<Field, Object>> fieldAndValuesFrom(Struct struct) {
        if (trie.isEmpty()) return Collections.emptyMap();
        return findFieldAndValues(struct, trie.root, new HashMap<>());
    }

    private Map<SingleFieldPath, Map.Entry<Field, Object>> findFieldAndValues(
        Struct originalValue,
        TrieNode trieAt,
        Map<SingleFieldPath, Map.Entry<Field, Object>> fieldAndValueMap
    ) {
        for (Map.Entry<String, TrieNode> step : trieAt.steps().entrySet()) {
            Field field = originalValue.schema().field(step.getKey());
            if (step.getValue().isLeaf()) {
                Map.Entry<Field, Object> fieldAndValue =
                    field != null
                        ? new AbstractMap.SimpleImmutableEntry<>(field, originalValue.get(field))
                        : null;
                fieldAndValueMap.put(step.getValue().path, fieldAndValue);
            } else {
                if (field.schema().type() == Type.STRUCT) {
                    findFieldAndValues(
                        originalValue.getStruct(field.name()),
                        step.getValue(),
                        fieldAndValueMap
                    );
                }
            }
        }
        return fieldAndValueMap;
    }

    /**
     * Find values at the field paths
     *
     * @param value data value
     * @return map of field paths and field/values
     */
    public Map<SingleFieldPath, Map.Entry<String, Object>> fieldAndValuesFrom(Map<String, Object> value) {
        if (trie.isEmpty()) return Collections.emptyMap();
        return findFieldAndValues(value, trie.root, new HashMap<>());
    }

    @SuppressWarnings("unchecked")
    private Map<SingleFieldPath, Map.Entry<String, Object>> findFieldAndValues(
        Map<String, Object> value,
        TrieNode trieAt,
        Map<SingleFieldPath, Map.Entry<String, Object>> fieldAndValueMap
    ) {
        for (Map.Entry<String, TrieNode> step : trieAt.steps().entrySet()) {
            Object fieldValue = value.get(step.getKey());
            if (step.getValue().isLeaf()) {
                fieldAndValueMap.put(
                    step.getValue().path,
                    new AbstractMap.SimpleImmutableEntry<>(step.getKey(), fieldValue)
                );
            } else {
                if (fieldValue instanceof Map) {
                    findFieldAndValues(
                        (Map<String, Object>) fieldValue,
                        step.getValue(),
                        fieldAndValueMap
                    );
                }
            }
        }
        return fieldAndValueMap;
    }

    /**
     * Access {@code Map} fields and apply functions to update field values.
     *
     * <p>If path is not found, no function is applied, and the path is ignored.
     *
     * <p>Other fields keep values from original struct.
     *
     * @param originalValue schema-based data value
     * @param whenFound     function to apply when current path(s) is/are found
     * @return updated data value
     */
    public Map<String, Object> updateValuesFrom(
        Map<String, Object> originalValue,
        MapValueUpdater whenFound
    ) {
        if (trie.isEmpty()) return originalValue;
        return updateValues(originalValue, trie.root, whenFound,
            (originalParent, updatedParent, fieldPath, fieldName) -> {
                // filter out
            },
            (originalParent, updatedParent, fieldPath, fieldName) ->
                updatedParent.put(fieldName, originalParent.get(fieldName)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> updateValues(
        Map<String, Object> originalValue,
        TrieNode trieAt,
        MapValueUpdater matching,
        MapValueUpdater notFound,
        MapValueUpdater others
    ) {
        if (originalValue == null) return null;
        Map<String, Object> updatedValue = new HashMap<>(originalValue.size());
        Map<String, TrieNode> notFoundFields = new HashMap<>(trieAt.steps);
        for (Map.Entry<String, Object> entry : originalValue.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            if (!trieAt.isEmpty()) {
                if (trieAt.contains(fieldName)) {
                    notFoundFields.remove(fieldName);
                    TrieNode trieValue = trieAt.get(fieldName);
                    if (trieValue.isLeaf()) {
                        matching.apply(originalValue, updatedValue, trieValue.path, fieldName);
                    } else {
                        if (fieldValue instanceof Map) {
                            Map<String, Object> updatedField = updateValues(
                                (Map<String, Object>) fieldValue,
                                trieValue,
                                matching, notFound, others);
                            updatedValue.put(fieldName, updatedField);
                        } else {
                            // add back to not found and apply others, as only leaf values are updated
                            notFoundFields.put(fieldName, trieValue);
                            others.apply(originalValue, updatedValue, null, fieldName);
                        }
                    }
                } else {
                    others.apply(originalValue, updatedValue, null, fieldName);
                }
            } else {
                others.apply(originalValue, updatedValue, null, fieldName);
            }
        }

        for (Map.Entry<String, TrieNode> entry : notFoundFields.entrySet()) {
            String fieldName = entry.getKey();
            TrieNode trieValue = entry.getValue();
            if (trieValue.isLeaf()) {
                notFound.apply(originalValue, updatedValue, trieValue.path, fieldName);
            } else {
                Map<String, Object> updatedField = updateValues(
                    new HashMap<>(),
                    trieValue,
                    matching, notFound, others);
                updatedValue.put(fieldName, updatedField);
            }
        }

        return updatedValue;
    }

    /**
     * Access {@code Struct} fields and apply functions to update field values.
     *
     * <p>If path is not found, no function is applied, and the path is ignored.
     *
     * <p>Other fields keep values from original struct.
     *
     * @param originalSchema original struct schema
     * @param originalValue  schema-based data value
     * @param updatedSchema  updated struct schema
     * @param whenFound      function to apply when current path(s) is/are found
     * @return updated data value
     */
    public Struct updateValuesFrom(
        Schema originalSchema,
        Struct originalValue,
        Schema updatedSchema,
        StructValueUpdater whenFound
    ) {
        if (trie.isEmpty()) return originalValue;
        return updateValues(originalSchema, originalValue, updatedSchema, trie.root, whenFound,
            (originalParent, originalField, updatedParent, updatedField, fieldPath) -> {
                // filter out
            },
            (originalParent, originalField, updatedParent, nullUpdatedField, nullFieldPath) ->
                updatedParent.put(originalField.name(), originalParent.get(originalField)));
    }

    private Struct updateValues(
        Schema originalSchema,
        Struct originalValue,
        Schema updateSchema,
        TrieNode trieAt,
        StructValueUpdater matching,
        StructValueUpdater notFound,
        StructValueUpdater others
    ) {
        Struct updatedValue = new Struct(updateSchema);
        Map<String, TrieNode> notFoundFields = trieAt.steps();
        for (Field field : originalSchema.fields()) {
            if (!trieAt.isEmpty()) {
                if (trieAt.contains(field.name())) {
                    notFoundFields.remove(field.name());
                    final TrieNode trieValue = trieAt.get(field.name());
                    if (trieValue.isLeaf()) {
                        matching.apply(
                            originalValue,
                            originalSchema.field(field.name()),
                            updatedValue,
                            updateSchema.field(field.name()),
                            trieValue.path
                        );
                    } else {
                        if (field.schema().type() == Type.STRUCT) {
                            Struct fieldValue = updateValues(
                                field.schema(),
                                originalValue.getStruct(field.name()),
                                updateSchema.field(field.name()).schema(),
                                trieValue,
                                matching, notFound, others
                            );
                            updatedValue.put(updateSchema.field(field.name()), fieldValue);
                        } else {
                            // add back to not found and apply others, as only leaf values are updated
                            notFoundFields.put(field.name(), trieValue);
                            others.apply(originalValue, field, updatedValue, null, null);
                        }
                    }
                } else {
                    others.apply(originalValue, field, updatedValue, null, null);
                }
            } else {
                others.apply(originalValue, field, updatedValue, null, null);
            }
        }
        for (Map.Entry<String, TrieNode> entry : notFoundFields.entrySet()) {
            String fieldName = entry.getKey();
            TrieNode trieValue = entry.getValue();
            if (trieValue.isLeaf()) {
                notFound.apply(
                    originalValue,
                    null,
                    updatedValue,
                    updateSchema.field(fieldName),
                    trieValue.path
                );
            } else {
                Struct fieldValue = updateValues(
                    SchemaBuilder.struct().build(),
                    null,
                    updateSchema.field(fieldName).schema(),
                    trieValue,
                    matching, notFound, others
                );
                updatedValue.put(updateSchema.field(fieldName), fieldValue);
            }
        }
        return updatedValue;
    }

    /**
     * Prepares a new schema based on an original one, and applies an update function
     * when the current path(s) is found.
     *
     * <p>If path is not found, no function is applied, and the path is ignored.
     *
     * <p>Other fields are copied from original schema.
     *
     * <p>A copy of the {@code Schema} is used as a base for the updated schema.
     *
     * @param originalSchema baseline schema
     * @param whenFound      function to apply when current path(s) is/are found
     * @return an updated schema. Resulting schemas are usually cached for further access
     */
    public Schema updateSchemaFrom(
        Schema originalSchema,
        StructSchemaUpdater whenFound
    ) {
        if (trie.isEmpty()) return originalSchema;
        SchemaBuilder updated = SchemaUtil.copySchemaBasics(originalSchema, SchemaBuilder.struct());
        return updateSchema(originalSchema, updated, trie.root, whenFound,
            (schemaBuilder, field, fieldPath) -> { /* ignore */ },
            (schemaBuilder, field, fieldPath) -> schemaBuilder.field(field.name(), field.schema()));
    }


    /**
     * Prepares a new schema based on an original one, and applies an update function
     * when the current path(s) is found.
     *
     * <p>If path is not found, no function is applied, and the path is ignored.
     *
     * <p>Other fields are copied from original schema.
     *
     * @param originalSchema        baseline schema
     * @param baselineSchemaBuilder baseline schema build, if changes to the baseline
     *                              are required before copying original
     * @param whenFound             function to apply when current path(s) is/are found.
     * @return an updated schema. Resulting schemas are usually cached for further access.
     */
    public Schema updateSchemaFrom(
        Schema originalSchema,
        SchemaBuilder baselineSchemaBuilder,
        StructSchemaUpdater whenFound
    ) {
        if (trie.isEmpty()) return originalSchema;
        return updateSchema(originalSchema, baselineSchemaBuilder, trie.root, whenFound,
            (schemaBuilder, field, fieldPath) -> { /* ignore */ },
            (schemaBuilder, field, fieldPath) -> schemaBuilder.field(field.name(), field.schema()));
    }

    private Schema updateSchema(
        Schema originalSchema,
        SchemaBuilder baseSchemaBuilder,
        TrieNode trieAt,
        StructSchemaUpdater whenFound,
        StructSchemaUpdater whenNotFound,
        StructSchemaUpdater toOtherFields
    ) {
        if (originalSchema.isOptional()) {
            baseSchemaBuilder.optional();
        }
        Map<String, TrieNode> notFoundFields = trieAt.steps();
        for (Field field : originalSchema.fields()) {
            if (!trieAt.isEmpty()) {
                if (!trieAt.contains(field.name())) {
                    toOtherFields.apply(baseSchemaBuilder, field, null);
                } else {
                    notFoundFields.remove(field.name());
                    TrieNode trieNode = trieAt.get(field.name());
                    if (trieNode.isLeaf()) {
                        whenFound.apply(baseSchemaBuilder, field, trieNode.path);
                    } else {
                        if (field.schema().type() == Type.STRUCT) {
                            Schema fieldSchema = updateSchema(
                                field.schema(),
                                SchemaBuilder.struct(),
                                trieNode,
                                whenFound, whenNotFound, toOtherFields);
                            baseSchemaBuilder.field(field.name(), fieldSchema);
                        } else {
                            toOtherFields.apply(baseSchemaBuilder, field, null);
                        }
                    }
                }
            } else {
                toOtherFields.apply(baseSchemaBuilder, field, null);
            }
        }
        for (Map.Entry<String, TrieNode> entry : notFoundFields.entrySet()) {
            String fieldName = entry.getKey();
            TrieNode trieValue = entry.getValue();
            if (trieValue.isLeaf()) {
                whenNotFound.apply(baseSchemaBuilder, null, trieValue.path);
            } else {
                Schema fieldSchema = updateSchema(
                    SchemaBuilder.struct().build(),
                    SchemaBuilder.struct(),
                    trieValue,
                    whenFound, whenNotFound, toOtherFields);
                baseSchemaBuilder.field(fieldName, fieldSchema);
            }
        }
        return baseSchemaBuilder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MultiFieldPaths that = (MultiFieldPaths) o;
        return Objects.equals(trie, that.trie);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trie);
    }

    @Override
    public String toString() {
        return "FieldPaths(trie = " + trie + ")";
    }

    // Invariants:
    // - Trie values contain either a nested trie or a field path when it is a leaf.
    // - A trie flattens overlapping paths (e.g. `foo` and `foo.bar` in V2, only `foo.bar` would be kept)
    static class Trie {
        TrieNode root;

        Trie() {
            root = new TrieNode();
        }


        public void insert(SingleFieldPath path) {
            TrieNode current = root;

            for (String step : path.stepsWithoutLast()) {
                if (!current.contains(step)) {
                    current.addStep(step);
                }

                current = current.get(step);
            }

            final String step = path.lastStep();
            if (!current.contains(step)) {
                current.addLeaf(step, path);
            }
        }

        public boolean isEmpty() {
            return root.isEmpty();
        }

        public Optional<TrieNode> find(String step) {
            return root.find(step);
        }

        public int size() {
            if (root.isEmpty()) return 0;
            return root.size();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Trie trie = (Trie) o;
            return Objects.equals(root, trie.root);
        }

        @Override
        public int hashCode() {
            return Objects.hash(root);
        }

        @Override
        public String toString() {
            return "Trie(" +
                "root = " + root +
                ')';
        }
    }

    static class TrieNode {
        Map<String, TrieNode> steps = new HashMap<>();
        SingleFieldPath path;

        TrieNode() {
        }

        private TrieNode(SingleFieldPath path) {
            this.path = path;
        }

        public boolean contains(String step) {
            return steps.containsKey(step);
        }

        public void addStep(String step) {
            if (path != null) path = null;
            steps.put(step, new TrieNode());
        }

        public void addLeaf(String step, SingleFieldPath path) {
            if (this.path != null) this.path = null;
            steps.put(step, new TrieNode(path));
        }

        public TrieNode get(String step) {
            return steps.get(step);
        }

        public Optional<TrieNode> find(String step) {
            return Optional.ofNullable(steps.get(step));
        }

        public boolean isEmpty() {
            return steps.isEmpty() && path == null;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrieNode trieNode = (TrieNode) o;
            return Objects.equals(steps, trieNode.steps) && Objects.equals(path, trieNode.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(steps, path);
        }

        @Override
        public String toString() {
            return "TrieNode(" +
                "steps = " + steps +
                (path != null ? (", path = " + path) : "") +
                ')';
        }

        public boolean isLeaf() {
            return path != null;
        }

        public Map<String, TrieNode> steps() {
            return new HashMap<>(steps);
        }

        public int size() {
            if (isLeaf()) return 1;
            int size = 0;
            for (TrieNode child : steps.values()) {
                size = size + child.size();
            }
            return size;
        }
    }
}
