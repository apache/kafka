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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A SingleFieldPath is composed of one or more field names, known as steps,
 * to access values within a data object (either {@code Struct} or {@code Map<String, Object>}).
 *
 * <p>If the SMT requires accessing multiple fields on the same data object,
 * use {@link MultiFieldPaths} instead.
 *
 * <p>The field path semantics are defined by the {@link FieldSyntaxVersion syntax version}.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-821%3A+Connect+Transforms+support+for+nested+structures">KIP-821</a>
 * @see FieldSyntaxVersion
 * @see MultiFieldPaths
 */
public class SingleFieldPath {
    // Invariants:
    // - A field path can contain one or more steps
    private static final char BACKTICK = '`';
    private static final char DOT = '.';
    private static final char BACKSLASH = '\\';

    private final String[] path;

    public SingleFieldPath(String pathText, FieldSyntaxVersion version) {
        Objects.requireNonNull(pathText, "Field path cannot be null");
        switch (version) {
            case V1: // backward compatibility
                this.path = new String[] {pathText};
                break;
            case V2:
                this.path = buildFieldPathV2(pathText);
                break;
            default:
                throw new IllegalArgumentException("Unknown syntax version: " + version);
        }
    }

    private String[] buildFieldPathV2(String pathText) {
        final List<String> steps = new ArrayList<>();
        int idx = 0;
        while (idx < pathText.length() && idx >= 0) {
            if (pathText.charAt(idx) != BACKTICK) {
                final int start = idx;
                idx = pathText.indexOf(String.valueOf(DOT), idx);
                if (idx >= 0) { // get path step and move forward
                    String field = pathText.substring(start, idx);
                    steps.add(field);
                    idx++;
                } else { // add all
                    String field = pathText.substring(start);
                    steps.add(field);
                }
            } else {
                StringBuilder field = new StringBuilder();
                idx++;
                int start = idx;
                while (true) {
                    idx = pathText.indexOf(String.valueOf(BACKTICK), idx);
                    if (idx == -1) { // if not found, fail
                        throw new IllegalArgumentException("Incomplete backtick pair in path: " + pathText);
                    }

                    if (idx >= pathText.length() - 1) { // at the end of path
                        field.append(pathText, start, idx);
                        // we've reached the end of the path, and the last character is the backtick
                        steps.add(field.toString());
                        idx++;
                        break;
                    }

                    boolean notFollowedByDot = pathText.charAt(idx + 1) != DOT;
                    if (notFollowedByDot) {
                        boolean afterABackslash = pathText.charAt(idx - 1) == BACKSLASH;
                        if (afterABackslash) {
                            // this backtick was escaped; include it in the field name, but continue
                            // looking for an unescaped matching backtick
                            field.append(pathText, start, idx - 1)
                                .append(BACKTICK);

                            idx++;
                            start = idx;
                        } else {
                            // this backtick isn't followed by a dot; include it in the field name, but continue
                            // looking for a matching backtick that is followed by a dot
                            idx++;
                        }
                        continue;
                    }

                    boolean afterABackslash = pathText.charAt(idx - 1) == BACKSLASH;
                    if (afterABackslash) {
                        // this backtick was escaped; include it in the field name, but continue
                        // looking for an unescaped matching backtick
                        field.append(pathText, start, idx - 1)
                            .append(BACKTICK);

                        idx++;
                        start = idx;
                        continue;
                    }
                    // we've found our matching backtick
                    field.append(pathText, start, idx);
                    steps.add(field.toString());
                    idx += 2; // increment by two to include the backtick and the dot after it
                    break;
                }
            }
        }
        if (!pathText.isEmpty() && pathText.charAt(pathText.length() - 1) == DOT)
            steps.add("");
        return steps.toArray(new String[0]);
    }


    /**
     * Access a {@code Field} at the current path within a schema {@code Schema}
     * If field is not found, then {@code null} is returned.
     */
    public Field fieldFrom(Schema schema) {
        if (path.length == 1) {
            return schema.field(path[0]);
        } else {
            Schema current = schema;
            for (int i = 0; i < path.length; i++) {
                if (current == null) {
                    return null;
                }
                if (i == path.length - 1) { // get value
                    return current.field(path[i]);
                } else { // iterate
                    final Field field = current.field(path[i]);
                    if (field != null) {
                        current = field.schema();
                    } else {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Access a value at the current path within a schema-based {@code Struct}
     * If object is not found, then {@code null} is returned.
     */
    public Object valueFrom(Struct struct) {
        if (path.length == 1) {
            final Field field = struct.schema().field(path[0]);
            if (field != null) {
                return struct.get(path[0]);
            } else {
                return null;
            }
        } else {
            Struct current = struct;
            for (int i = 0; i < path.length; i++) {
                if (current == null) {
                    return null;
                }
                final Field field = current.schema().field(path[i]);
                if (i == path.length - 1) { // get value
                    if (field != null) {
                        return current.get(path[i]);
                    } else {
                        return null;
                    }
                } else { // iterate
                    if (field != null && field.schema().type() == Type.STRUCT) {
                        current = current.getStruct(path[i]);
                    } else {
                        return null;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Access a value at the current path within a schemaless {@code Map<String, Object>}.
     * If object is not found, then {@code null} is returned.
     */
    @SuppressWarnings("unchecked")
    public Object valueFrom(Map<String, Object> map) {
        if (path.length == 1) {
            return map.get(path[0]);
        } else {
            Map<String, Object> current = map;
            for (int i = 0; i < path.length; i++) {
                if (current == null) {
                    return null;
                }
                if (i == path.length - 1) {
                    return current.get(path[i]);
                } else {
                    current = (Map<String, Object>) current.get(path[i]);
                }
            }
        }
        return null;
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
    public Map<String, Object> updateValueFrom(
        Map<String, Object> originalValue,
        MapValueUpdater whenFound
    ) {
        return updateValue(originalValue, 0, whenFound,
            (originalParent, updatedParent, fieldPath, fieldName) -> {
                // filter out
            },
            (originalParent, updatedParent, fieldPath, fieldName) ->
                updatedParent.put(fieldName, originalParent.get(fieldName)));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> updateValue(
        Map<String, Object> originalValue,
        int step,
        MapValueUpdater update,
        MapValueUpdater notFound,
        MapValueUpdater others
    ) {
        if (originalValue == null) return null;
        Map<String, Object> updatedParent = new HashMap<>(originalValue.size());
        boolean found = false;
        for (Map.Entry<String, Object> entry : originalValue.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldValue = entry.getValue();
            if (path[step].equals(fieldName)) {
                found = true;
                if (step < path.length - 1) {
                    if (fieldValue instanceof Map) {
                        Map<String, Object> updatedField = updateValue(
                            (Map<String, Object>) fieldValue,
                            step + 1,
                            update,
                            notFound,
                            others);
                        updatedParent.put(fieldName, updatedField);
                    } else {
                        // add back to not found and apply others, as only leaf values are updated
                        found = false;
                        others.apply(originalValue, updatedParent, null, fieldName);
                    }
                } else {
                    update.apply(originalValue, updatedParent, this, fieldName);
                }
            } else {
                others.apply(originalValue, updatedParent, null, fieldName);
            }
        }

        if (!found) {
            notFound.apply(originalValue, updatedParent, this, stepAt(step));
        }

        return updatedParent;
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
    public Struct updateValueFrom(
        Schema originalSchema,
        Struct originalValue,
        Schema updatedSchema,
        StructValueUpdater whenFound
    ) {
        return updateValue(originalSchema, originalValue, updatedSchema, 0, whenFound,
            (originalParent, originalField, updatedParent, updatedField, fieldPath) -> {
                // filter out
            },
            (originalParent, originalField, updatedParent, nullUpdatedField, nullFieldPath) ->
                updatedParent.put(originalField.name(), originalParent.get(originalField)));
    }

    private Struct updateValue(
        Schema originalSchema,
        Struct originalValue,
        Schema updateSchema,
        int step,
        StructValueUpdater update,
        StructValueUpdater notFound,
        StructValueUpdater others
    ) {
        Struct updated = new Struct(updateSchema);
        boolean found = false;
        for (Field field : originalSchema.fields()) {
            if (step < path.length) {
                if (path[step].equals(field.name())) {
                    found = true;
                    if (step == path.length - 1) {
                        update.apply(
                            originalValue,
                            field,
                            updated,
                            updateSchema.field(field.name()),
                            this
                        );
                    } else {
                        if (field.schema().type() == Type.STRUCT) {
                            Struct fieldValue = updateValue(
                                field.schema(),
                                originalValue.getStruct(field.name()),
                                updateSchema.field(field.name()).schema(),
                                step + 1,
                                update,
                                notFound,
                                others
                            );
                            updated.put(field.name(), fieldValue);
                        } else {
                            // add back to not found and apply others, as only leaf values are updated
                            found = false;
                            others.apply(originalValue, field, updated, null, this);
                        }
                    }
                } else {
                    others.apply(originalValue, field, updated, null, this);
                }
            }
        }
        if (!found) {
            notFound.apply(
                originalValue,
                null,
                updated,
                updateSchema.field(stepAt(step)),
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
     * <p>A copy of the {@code Schema} is used as a base for the updated schema.
     *
     * @param originalSchema baseline schema
     * @param whenFound      function to apply when current path(s) is/are found
     * @return an updated schema. Resulting schemas are usually cached for further access
     */
    public Schema updateSchemaFrom(Schema originalSchema, StructSchemaUpdater whenFound) {
        SchemaBuilder updated = SchemaUtil.copySchemaBasics(originalSchema, SchemaBuilder.struct());
        return updateSchema(originalSchema, updated, 0, whenFound,
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
        return updateSchema(originalSchema, baselineSchemaBuilder, 0, whenFound,
            (schemaBuilder, field, fieldPath) -> { /* ignore */ },
            (schemaBuilder, field, fieldPath) -> schemaBuilder.field(field.name(), field.schema()));
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
            if (step < path.length) {
                if (path[step].equals(field.name())) {
                    matched = true;
                    if (step == path.length - 1) {
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

    public String last() {
        return path[path.length - 1];
    }

    public boolean isEmpty() {
        return path.length == 0;
    }

    public String stepAt(int i) {
        return i < path.length ? path[i] : "";
    }

    // For testing
    String[] path() {
        return Arrays.copyOf(path, path.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SingleFieldPath fieldPath = (SingleFieldPath) o;
        return Arrays.equals(path, fieldPath.path);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(path);
    }

    @Override
    public String toString() {
        return "FieldPath(path = " + Arrays.toString(path) + ")";
    }
}
