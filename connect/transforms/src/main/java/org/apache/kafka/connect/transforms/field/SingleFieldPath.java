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

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;

/**
 * A SingleFieldPath is composed of one or more field names, known as path steps,
 * to access values within a data object (either {@code Struct} or {@code Map<String, Object>}).
 *
 * <p>The field path semantics are defined by the {@link FieldSyntaxVersion syntax version}.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-821%3A+Connect+Transforms+support+for+nested+structures">KIP-821</a>
 * @see FieldSyntaxVersion
 */
@InterfaceStability.Evolving
public class SingleFieldPath {
    // Invariants:
    // - A field path can contain one or more steps
    private static final char BACKTICK = '`';
    private static final char DOT = '.';
    private static final char BACKSLASH = '\\';

    private final FieldSyntaxVersion version;
    private final List<String> steps;

    public SingleFieldPath(String pathText, FieldSyntaxVersion version) {
        this.version = version;
        switch (version) {
            case V1: // backward compatibility
                this.steps = Collections.singletonList(pathText);
                break;
            case V2:
                this.steps = buildFieldPathV2(pathText);
                break;
            default:
                throw new IllegalArgumentException("Unknown syntax version: " + version);
        }
    }

    private static List<String> buildFieldPathV2(String path) {
        final List<String> steps = new ArrayList<>();
        // path character index to track backticks and dots and break path into steps
        int idx = 0;
        while (idx < path.length() && idx >= 0) {
            if (path.charAt(idx) != BACKTICK) {
                final int start = idx;
                idx = path.indexOf(String.valueOf(DOT), idx);
                if (idx >= 0) { // get path step and move forward
                    String field = path.substring(start, idx);
                    steps.add(field);
                    idx++;
                } else { // add all
                    String field = path.substring(start);
                    steps.add(field);
                }
            } else { // has backtick
                int backtickAt = idx;
                idx++;
                StringBuilder field = new StringBuilder();
                int start = idx;
                while (true) {
                    // find closing backtick
                    idx = path.indexOf(String.valueOf(BACKTICK), idx);
                    if (idx == -1) { // if not found, then fail
                        failWhenIncompleteBacktickPair(path, backtickAt);
                    }

                    // backtick escaped if right after backslash
                    boolean escaped = path.charAt(idx - 1) == BACKSLASH;

                    if (idx >= path.length() - 1) { // at the end of path
                        if (escaped) { // but escaped, then fail
                            failWhenIncompleteBacktickPair(path, backtickAt);
                        }
                        field.append(path, start, idx);
                        // we've reached the end of the path, and the last character is the backtick
                        steps.add(field.toString());
                        idx++;
                        break;
                    }

                    if (path.charAt(idx + 1) != DOT) { // not followed by a dot
                        // this backtick isn't followed by a dot; include it in the field name, but continue
                        // looking for a matching backtick that is followed by a dot
                        idx++;
                        continue;
                    }

                    if (escaped) {
                        // this backtick was escaped; include it in the field name, but continue
                        // looking for an unescaped matching backtick
                        field.append(path, start, idx - 1)
                            .append(BACKTICK);

                        idx++;
                        start = idx;
                        continue;
                    }

                    // we've found our matching backtick
                    field.append(path, start, idx);
                    steps.add(field.toString());
                    idx += 2; // increment by two to include the backtick and the dot after it
                    break;
                }
            }
        }
        // add last step if last char is a dot
        if (!path.isEmpty() && path.charAt(path.length() - 1) == DOT)
            steps.add("");
        return Collections.unmodifiableList(steps);
    }

    private static void failWhenIncompleteBacktickPair(String path, int backtickAt) {
        throw new ConfigException("Incomplete backtick pair in path: [" + path + "],"
                + " consider adding a backslash before backtick at position " + backtickAt
                + " to escape it");
    }

    /**
     * Access a {@code Field} at the current path within a schema {@code Schema}
     * If field is not found, then {@code null} is returned.
     */
    public Field fieldFrom(Schema schema) {
        if (schema == null) return null;

        Schema current = schema;
        for (String pathSegment : stepsWithoutLast()) {
            final Field field = current.field(pathSegment);
            if (field != null) {
                current = field.schema();
            } else {
                return null;
            }
        }
        return current.field(lastStep());
    }

    /**
     * Access a value at the current path within a schema-based {@code Struct}
     * If object is not found, then {@code null} is returned.
     */
    public Object valueFrom(Struct struct) {
        if (struct == null) return null;

        Struct current = struct;
        for (String pathSegment : stepsWithoutLast()) {
            // Check to see if the field actually exists
            if (current.schema().field(pathSegment) == null) {
                return null;
            }
            Object subValue = current.get(pathSegment);
            current = requireStructOrNull(subValue, "nested field access");
            if (current == null) return null;
        }

        if (current.schema().field(lastStep()) != null) {
            return current.get(lastStep());
        } else {
            return null;
        }
    }

    /**
     * Access a value at the current path within a schemaless {@code Map<String, Object>}.
     * If object is not found, then {@code null} is returned.
     */
    public Object valueFrom(Map<String, Object> map) {
        if (map == null) return null;

        Map<String, Object> current = map;
        for (String step : stepsWithoutLast()) {
            current = requireMapOrNull(current.get(step), "nested field access");
            if (current == null) return null;
        }
        return current.get(lastStep());
    }

    /**
     * Updates the matching field value if found
     * @param map original root value
     * @param update function to apply to existing value
     * @return the original value with the matching field updated if found
     */
    public Map<String, Object> updateMap(
        Map<String, Object> map,
        Function<Object, Object> update
    ) {
        if (map == null) return null;
        Map<String, Object> result = new HashMap<>(map);

        Map<String, Object> parent = result;
        Map<String, Object> child;
        for (String step : stepsWithoutLast()) {
            child = requireMapOrNull(parent.get(step), "nested field access");
            if (child == null) return map;
            child = new HashMap<>(child);
            parent.put(step, child);
            parent = child;
        }

        Object original = parent.get(lastStep());
        Object updated = update.apply(original);
        if (updated != null) {
            parent.put(lastStep(), updated);
        }
        return result;
    }

    /**
     * Updates the matching field value if found
     * @param struct original root value
     * @param update function to apply to existing value, input may be null
     * @return the original value with the matching field updated if found
     */
    public Struct updateStruct(
        Struct struct,
        Schema updatedSchema,
        BiFunction<Object, Schema, Object> update
    ) {
        return updateStruct(
            struct,
            updatedSchema,
            update,
            steps.get(0),
            steps.subList(1, steps.size())
        );
    }

    private static Struct updateStruct(
        Struct original,
        Schema updatedSchema,
        BiFunction<Object, Schema, Object> update,
        String currentStep,
        List<String> nextSteps
    ) {
        if (original == null)
            return null;

        Struct result = new Struct(updatedSchema);
        for (Field field : updatedSchema.fields()) {
            String fieldName = field.name();

            if (fieldName.equals(currentStep)) {
                final Object updatedField;

                // Modify this field
                if (nextSteps.isEmpty()) {
                    // This is a leaf node
                    Object originalField = original.get(fieldName);
                    updatedField = update.apply(
                        originalField,
                        original.schema().field(fieldName).schema()
                    );
                } else {
                    // We have to go deeper
                    Struct originalField = requireStructOrNull(original.get(fieldName), "nested field access");
                    updatedField = updateStruct(
                        originalField,
                        field.schema(),
                        update,
                        nextSteps.get(0),
                        nextSteps.subList(1, nextSteps.size())
                    );
                }

                result.put(fieldName, updatedField);
            } else {
                // Copy over all other fields from the original to the result
                result.put(fieldName, original.get(fieldName));
            }
        }

        return result;
    }

    /**
     * Updates the matching field schema if found
     * @param originalSchema original schema
     * @param baselineSchemaBuilder baseline schema to build
     * @param update function to apply to existing field, input may be null
     * @return the original schema with the matching field updated if found
     */
    public Schema updateSchema(
        Schema originalSchema,
        SchemaBuilder baselineSchemaBuilder,
        Function<Field, Schema> update
    ) {
        return updateSchema(originalSchema, baselineSchemaBuilder, update, steps.get(0), steps.subList(1, steps.size()));
    }

    // Recursive implementation to update schema at different steps.
    // Consider that resulting schemas are usually cached.
    private Schema updateSchema(
        Schema operatingSchema,
        SchemaBuilder builder,
        Function<Field, Schema> update,
        String currentStep,
        List<String> nextSteps
    ) {
        if (operatingSchema.isOptional()) {
            builder.optional();
        }
        for (Field field : operatingSchema.fields()) {
            if (field.name().equals(currentStep)) {
                final Schema updatedSchema;
                if (nextSteps.isEmpty()) {
                    // This is a leaf node
                    updatedSchema = update.apply(field);
                } else {
                    updatedSchema = updateSchema(
                        field.schema(),
                        SchemaBuilder.struct(),
                        update,
                        nextSteps.get(0),
                        nextSteps.subList(1, nextSteps.size()));
                }
                builder.field(field.name(), updatedSchema);
            } else {
                // Copy over all other fields from the original to the result
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    public boolean isEmpty() {
        for (String step: steps) {
            if (!step.isEmpty()) return false;
        }
        return true;
    }

    // For testing
    String[] path() {
        return steps.toArray(new String[0]);
    }

    private String lastStep() {
        return steps.get(lastStepIndex());
    }

    private int lastStepIndex() {
        return steps.size() - 1;
    }

    private List<String> stepsWithoutLast() {
        return steps.subList(0, lastStepIndex());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SingleFieldPath that = (SingleFieldPath) o;
        return Objects.equals(steps, that.steps);
    }

    @Override
    public int hashCode() {
        return Objects.hash(steps);
    }

    @Override
    public String toString() {
        return "SingleFieldPath{" +
            "version=" + version +
            ", path=" + String.join(".", steps) +
            '}';
    }
}
