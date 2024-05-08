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
     * Access {@code Map} fields and apply functions to update field values.
     *
     * @param originalValue schema-based data value
     * @param whenFound     function to apply when path is found
     * @param whenNotFound  function to apply when path is not found
     * @param whenOther     function to apply on fields not matched by path
     * @return updated data value
     */
    public Map<String, Object> updateValueFrom(
        Map<String, Object> originalValue,
        MapValueUpdater whenFound,
        MapValueUpdater whenNotFound,
        MapValueUpdater whenOther
    ) {
        return updateValue(originalValue, 0, whenFound, whenNotFound, whenOther);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> updateValue(
        Map<String, Object> originalValue,
        int step,
        MapValueUpdater whenFound,
        MapValueUpdater whenNotFound,
        MapValueUpdater whenOther
    ) {
        if (originalValue == null) return null;
        Map<String, Object> updatedParent = new HashMap<>(originalValue.size());
        boolean found = false;
        for (Map.Entry<String, Object> entry : originalValue.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            if (steps.get(step).equals(fieldName)) {
                found = true;
                if (step < lastStepIndex()) {
                    if (fieldValue instanceof Map) {
                        Map<String, Object> updatedField = updateValue(
                            (Map<String, Object>) fieldValue,
                            step + 1,
                            whenFound,
                            whenNotFound,
                            whenOther);
                        updatedParent.put(fieldName, updatedField);
                    } else {
                        // add back to not found and apply others, as only leaf values are updated
                        found = false;
                        whenOther.apply(originalValue, updatedParent, null, fieldName);
                    }
                } else {
                    whenFound.apply(originalValue, updatedParent, this, fieldName);
                }
            } else {
                whenOther.apply(originalValue, updatedParent, null, fieldName);
            }
        }

        if (!found) {
            whenNotFound.apply(originalValue, updatedParent, this, steps.get(step));
        }

        return updatedParent;
    }

    /**
     * Access {@code Struct} fields and apply functions to update field values.
     *
     * @param originalSchema original struct schema
     * @param originalValue  schema-based data value
     * @param updatedSchema  updated struct schema
     * @param whenFound      function to apply when path is found
     * @param whenNotFound   function to apply when path is not found
     * @param whenOther      function to apply on fields not matched by path
     * @return updated data value
     */
    public Struct updateValueFrom(
        Schema originalSchema,
        Struct originalValue,
        Schema updatedSchema,
        StructValueUpdater whenFound,
        StructValueUpdater whenNotFound,
        StructValueUpdater whenOther
    ) {
        return updateValue(originalSchema, originalValue, updatedSchema, 0, whenFound, whenNotFound, whenOther);
    }

    private Struct updateValue(
        Schema originalSchema,
        Struct originalValue,
        Schema updateSchema,
        int step,
        StructValueUpdater whenFound,
        StructValueUpdater whenNotFound,
        StructValueUpdater whenOther
    ) {
        Struct updated = new Struct(updateSchema);
        boolean found = false;
        for (Field field : originalSchema.fields()) {
            if (step < steps.size()) {
                if (steps.get(step).equals(field.name())) {
                    found = true;
                    if (step == lastStepIndex()) {
                        whenFound.apply(
                            originalValue,
                            field,
                            updated,
                            updateSchema.field(field.name()),
                            this
                        );
                    } else {
                        if (field.schema().type() == Schema.Type.STRUCT) {
                            Struct fieldValue = updateValue(
                                field.schema(),
                                originalValue.getStruct(field.name()),
                                updateSchema.field(field.name()).schema(),
                                step + 1,
                                whenFound,
                                whenNotFound,
                                whenOther
                            );
                            updated.put(field.name(), fieldValue);
                        } else {
                            // add back to not found and apply others, as only leaf values are updated
                            found = false;
                            whenOther.apply(originalValue, field, updated, null, this);
                        }
                    }
                } else {
                    whenOther.apply(originalValue, field, updated, null, this);
                }
            }
        }
        if (!found) {
            whenNotFound.apply(
                originalValue,
                null,
                updated,
                updateSchema.field(steps.get(step)),
                this);
        }
        return updated;
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
        StructSchemaUpdater whenFound,
        StructSchemaUpdater whenNotFound,
        StructSchemaUpdater whenOther
    ) {
        return updateSchema(originalSchema, baselineSchemaBuilder, 0, whenFound, whenNotFound, whenOther);
    }

    // Recursive implementation to update schema at different steps.
    // Consider that resulting schemas are usually cached.
    private Schema updateSchema(
        Schema operatingSchema,
        SchemaBuilder builder,
        int step,
        StructSchemaUpdater matching,
        StructSchemaUpdater notFound,
        StructSchemaUpdater others
    ) {
        if (operatingSchema.isOptional()) {
            builder.optional();
        }
        if (operatingSchema.defaultValue() != null) {
            builder.defaultValue(operatingSchema.defaultValue());
        }
        boolean matched = false;
        for (Field field : operatingSchema.fields()) {
            if (step < steps.size()) {
                if (steps.get(step).equals(field.name())) {
                    matched = true;
                    if (step == lastStepIndex()) {
                        matching.apply(builder, field, this);
                    } else {
                        Schema fieldSchema = updateSchema(
                            field.schema(),
                            SchemaBuilder.struct(),
                            step + 1,
                            matching,
                            notFound,
                            others);
                        builder.field(field.name(), fieldSchema);
                    }
                } else {
                    others.apply(builder, field, null);
                }
            } else {
                others.apply(builder, field, null);
            }
        }
        if (!matched) {
            notFound.apply(builder, null, this);
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
