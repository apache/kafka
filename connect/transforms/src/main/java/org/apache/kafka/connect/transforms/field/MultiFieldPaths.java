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
    private final Trie trie = new Trie();

    public MultiFieldPaths(List<SingleFieldPath> paths) {
        Objects.requireNonNull(paths, "paths cannot be null");
        paths.forEach(trie::insert);
    }

    public MultiFieldPaths(List<String> fields, FieldSyntaxVersion syntaxVersion) {
        this(Objects.requireNonNull(fields, "fields cannot be null").stream()
            .map(f -> new SingleFieldPath(f, Objects.requireNonNull(syntaxVersion, "syntaxVersion cannot be null")))
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
        Trie.Node trieAt,
        Map<SingleFieldPath, Map.Entry<Field, Object>> fieldAndValueMap
    ) {
        for (Map.Entry<String, Trie.Node> step : trieAt.steps().entrySet()) {
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
        Trie.Node trieAt,
        Map<SingleFieldPath, Map.Entry<String, Object>> fieldAndValueMap
    ) {
        for (Map.Entry<String, Trie.Node> step : trieAt.steps().entrySet()) {
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

}
