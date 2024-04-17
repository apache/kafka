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
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    public MultiFieldPaths(List<SingleFieldPath> paths) {
        paths.forEach(trie::insert);
    }

    public MultiFieldPaths(List<String> fields, FieldSyntaxVersion syntaxVersion) {
        this(fields.stream()
            .map(f -> new SingleFieldPath(f, syntaxVersion))
            .collect(Collectors.toList()));
    }

    /**
     * Find values at the field paths
     *
     * @param struct data value
     * @return map of field paths and field/values
     */
    public Map<SingleFieldPath, Map.Entry<Field, Object>> fieldAndValuesFrom(Struct struct) {
        if (trie.isEmpty()) return Collections.emptyMap();
        return findFieldAndValues(struct, trie.root, new LinkedHashMap<>());
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
        return findFieldAndValues(value, trie.root, new LinkedHashMap<>());
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

    public int size() {
        return trie.size();
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


        void insert(SingleFieldPath path) {
            TrieNode current = root;

            for (String step : path.stepsWithoutLast()) {
                current = current.addStep(step);
            }

            final String step = path.lastStep();
            current.addLeaf(step, path);
        }

        boolean isEmpty() {
            return root.isEmpty();
        }

        TrieNode get(String step) {
            return root.get(step);
        }

        int size() {
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

        private TrieNode() {
        }

        private TrieNode(SingleFieldPath path) {
            this.path = path;
        }

        boolean contains(String step) {
            return steps.containsKey(step);
        }

        private TrieNode addStep(String step) {
            return steps.computeIfAbsent(step, ignored -> {
                if (path != null) path = null;
                return new TrieNode();
            });
        }

        private void addLeaf(String step, SingleFieldPath path) {
            steps.computeIfAbsent(step, ignored -> {
                if (this.path != null) this.path = null;
                return new TrieNode(path);
            });
        }

        TrieNode get(String step) {
            return steps.get(step);
        }

        boolean isEmpty() {
            return steps.isEmpty() && path == null;
        }

        public boolean isLeaf() {
            return path != null;
        }

        Map<String, TrieNode> steps() {
            return new HashMap<>(steps);
        }

        int size() {
            if (isLeaf()) return 1;
            int size = 0;
            for (TrieNode child : steps.values()) {
                size = size + child.size();
            }
            return size;
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
    }
}
