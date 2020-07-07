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
package org.apache.kafka.common.message;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.message.FieldSpec;
import org.apache.kafka.message.FieldType;
import org.apache.kafka.message.MessageSpec;
import org.apache.kafka.message.StructSpec;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AbstractMessageCompatibilityTest {

    static final ObjectMapper MAPPER = new ObjectMapper()
            .enable(JsonParser.Feature.ALLOW_COMMENTS)
            .enable(JsonParser.Feature.AUTO_CLOSE_SOURCE);

    /**
     * Returns a collection of arrays suitable for use with Junit @Parameterized.Parameters methods.
     * Each array has elements:
     * <ol>
     *     <li>Message name</li>
     *     <li>{@link KafkaVersion} from git (e.g. from a git tag for a given kafka release, or HEAD)</li>
     *     <li>{@link MessageSpec} for that version in git</li>
     *     <li>{@link KafkaVersion} for the local code (i.e in the working tree)</li>
     *     <li>{@link MessageSpec} for the local version</li>
     * </ol>
     * @param path The path where the messages reside (e.g. "clients/src/main/resources/common/message/").
     * @param pathSuffix The suffix for files (e.g. ".json").
     * @return collection of arrays.
     * @throws IOException
     */
    protected static Collection<Object[]> messagePairs(String path, String pathSuffix) throws IOException {
        Map<String, Map<KafkaVersion, MessageSpec>> apiToVersionToSpec = new TreeMap<>();
        Stream.concat(Git.kafkaReleaseTags(), Git.head())
                .flatMap(ref -> Git.parseFiles(ref,
                        path, pathSuffix,
                        (kafkaVersion, input) -> MAPPER.readValue(input, MessageSpec.class)))
                .forEach(pair -> apiToVersionToSpec.computeIfAbsent(
                        pair.model().name(),
                        key -> new TreeMap<>()).put(pair.kafkaVersion(), pair.model()));
        List<Object[]> result = new ArrayList<>();
        for (Map.Entry<String, Map<KafkaVersion, MessageSpec>> nameEntry : apiToVersionToSpec.entrySet()) {
            String messageName = nameEntry.getKey();
            KafkaVersion localKafkaVersion = KafkaVersion.parse("local");
            File localFile = Git.findGitDir().toPath().getParent().resolve(path).resolve(messageName + pathSuffix).toFile();
            for (Map.Entry<KafkaVersion, MessageSpec> versionEntry : nameEntry.getValue().entrySet()) {
                KafkaVersion gitKafkaVersion = versionEntry.getKey();
                MessageSpec gitSpec = versionEntry.getValue();
                if (localFile.exists()) {
                    MessageSpec localSpec = MAPPER.readValue(localFile, MessageSpec.class);
                    result.add(new Object[]{messageName, gitKafkaVersion, gitSpec, localKafkaVersion, localSpec});
                }
            }
        }
        return result;
    }

    public void validateCompatibility(String messageName, KafkaVersion oldKafkaVersion, MessageSpec oldSpec, KafkaVersion newKafkaVersion, MessageSpec newSpec) {
        assertEquals(format("%s: API key changed from %s in %s to %s in %s", messageName, oldSpec.apiKey(), oldKafkaVersion, newSpec.apiKey(), newKafkaVersion),
                oldSpec.apiKey(), newSpec.apiKey());
        assertEquals(format("%s: Lowest supported version changed", messageName),
                oldSpec.validVersions().lowest(), newSpec.validVersions().lowest());
        Map<Short, List<FieldSpec>> oldModelsByVersion = modelsByVersion(oldSpec);
        Map<Short, List<FieldSpec>> newModelsByVersion = modelsByVersion(newSpec);
        Map<String, Map<Integer, String>> tagsByPath = new HashMap<>();
        validate(messageName, oldKafkaVersion.toString(), oldModelsByVersion,
                newKafkaVersion.toString(), newModelsByVersion, tagsByPath);
    }

    private void validate(String messageName, String oldKafkaVersion, Map<Short, List<FieldSpec>> oldModelsByVersion, String newKafkaVersion,
                          Map<Short, List<FieldSpec>> newModelsByVersion,
                          Map<String, Map<Integer, String>> tagsByPath) {
        HashSet<Short> commonVersions = new HashSet<>(oldModelsByVersion.keySet());
        commonVersions.retainAll(newModelsByVersion.keySet());
        for (Short apiVersion : commonVersions) {
            List<FieldSpec> oldFields = oldModelsByVersion.get(apiVersion);
            List<FieldSpec> newFields = newModelsByVersion.get(apiVersion);
            validateFields(messageName, apiVersion,
                    null,
                    oldKafkaVersion, oldFields,
                    null,
                    newKafkaVersion, newFields, "", tagsByPath);
        }
    }

    private void validateFields(String messageName, Short apiVersion,
                                FieldSpec parentOldField,
                                String oldKafkaVersion, List<FieldSpec> oldFields,
                                FieldSpec parentNewField,
                                String newKafkaVersion, List<FieldSpec> newFields,
                                String path,
                                Map<String, Map<Integer, String>> tags) {
        // Changed field order
        String[] reorderedField = findFirstChangedOrder(oldFields, newFields);
        if (reorderedField != null) {
            fail(format("%s: Field '%s' was reordered in API version %s: " +
                            "it was after '%s' in %s but it is before '%s' in %s",
                    messageName,
                    reorderedField[0], apiVersion, reorderedField[1], oldKafkaVersion, reorderedField[1], newKafkaVersion));
        }
        Map<String, FieldSpec> oldMap = fieldMap(oldFields);
        Map<String, FieldSpec> newMap = fieldMap(newFields);
        Diff diff = new Diff(oldMap, newMap);
        for (String fieldName : diff.commonNames()) {
            FieldSpec oldField = oldMap.get(fieldName);
            FieldSpec newField = newMap.get(fieldName);
            // Changed type
            validateFieldType(messageName, apiVersion, oldKafkaVersion, newKafkaVersion, fieldName, oldField, newField);
            String fieldPath = (path.isEmpty() ? "" : path + ".") + oldField.name();
            //validateFields(messageName, apiVersion, oldKafkaVersion, oldField.fields(), newKafkaVersion, newField.fields(), fieldPath, tags);
            // Changed default
            validateFieldDefault(messageName, apiVersion, oldKafkaVersion, newKafkaVersion, fieldName, oldField, newField);
            // Changed nullabilty
            validateFieldNullability(messageName, apiVersion, oldKafkaVersion, newKafkaVersion, fieldName, oldField, newField);
            // Check for reused tags
            validateTagReuse(messageName, path, tags, fieldName, oldField);
            validateTagReuse(messageName, path, tags, fieldName, newField);
            // Validate child fields
            validateFields(messageName, apiVersion,
                    oldField, oldKafkaVersion, oldField.fields(),
                    newField, newKafkaVersion, newField.fields(),
                    fieldPath, tags);
        }
        for (String fieldName : diff.addedNames()) {
            // Added a non-tagged, non-ignorable field
            FieldSpec field = newMap.get(fieldName);
            assertTrue(format("%s: Field '%s' added in API version %d/Kafka version %s is not tagged and not ignorable",
                    messageName,
                    fieldName, apiVersion, newKafkaVersion),
                    !field.taggedVersions().empty() // it's OK to add tagged fields
                            || field.ignorable() // or ignorable fields
                            || isArrayOfSingletonPrimitiveStructReplacingArrayOfPrimitive(parentOldField, newFields, field));
            validateTagReuse(messageName, path, tags, fieldName, field);
        }
        // Field removed within a API version
        assertTrue(format("%s: Kafka version %s removed fields %s from API version %d",
                messageName,
                newKafkaVersion, diff.removedNames(), apiVersion),
                diff.removedNames().isEmpty());
        // TODO non-nullable field with a null default (or is that checked by the generator?)
    }

    private boolean isArrayOfSingletonPrimitiveStructReplacingArrayOfPrimitive(FieldSpec parentOldField, List<FieldSpec> newFields, FieldSpec field) {
        return newFields.size() == 1 && isPrimitive(field.type())
            && parentOldField.type().isArray() && ((FieldType.ArrayType) parentOldField.type()).elementType().equals(field.type());
    }

    private void validateFieldNullability(String messageName, Short apiVersion, String oldKafkaVersion, String newKafkaVersion, String fieldName, FieldSpec oldField, FieldSpec newField) {
        assertEquals(format("%s: Field '%s' in API version %d has a different nullability in %s than in %s",
                messageName,
                fieldName, apiVersion, oldKafkaVersion, newKafkaVersion),
                oldField.nullableVersionsString(), newField.nullableVersionsString());
    }

    private void validateFieldDefault(String messageName, Short apiVersion, String oldKafkaVersion, String newKafkaVersion, String fieldName, FieldSpec oldField, FieldSpec newField) {
        assertEquals(format("%s: Field '%s' in API version %d has a different default in %s than in %s",
                messageName,
                fieldName, apiVersion, oldKafkaVersion, newKafkaVersion),
                defaultValid(oldField), defaultValid(newField));
    }

    private String defaultValid(FieldSpec oldField) {
        String def = oldField.defaultString();
        if (def == null || def.isEmpty()) {
            if (oldField.type().isFloat() ||
                    isIntegral(oldField.type())) {
                return "0";
            } else if (oldField.type().isString()) {
                return "";
            } else if (oldField.type() instanceof FieldType.BoolFieldType) {
                return "false";
            }
        }
        return def;
    }

    private void validateFieldType(String messageName, Short apiVersion, String oldKafkaVersion, String newKafkaVersion, String fieldName, FieldSpec oldField, FieldSpec newField) {
        if (oldField.type().isArray() && newField.type().isArray()) {
            // Messages effectively use structural typing, rather than nominal typing
            // i.e. the name of the type doesn't matter, so long as the structure of the types is the same
            // it's the same if the elements are compatible, but that's exactly what validateFields()
            // does, so normally there's nothing to test.
            // Exception: array of primitive and compatible with array of struct of just that primitive
            FieldType oldElementType = ((FieldType.ArrayType) oldField.type()).elementType();
            FieldType newElementType = ((FieldType.ArrayType) newField.type()).elementType();
            String msg = format("%s: Field '%s' cannot change from type %s to %s in API version %d",
                    messageName, fieldName, oldElementType, newElementType, apiVersion);
            if (isPrimitive(oldElementType)
                    && newElementType.isStruct()
                    && !newField.fields().get(0).flexibleVersions().isPresent()) {
                assertTrue(msg,
                        newField.fields().size() == 1 &&
                        newField.fields().get(0).type().equals(oldElementType));
            } else if (isPrimitive(newElementType)
                    && oldElementType.isStruct()
                    && !oldField.fields().get(0).flexibleVersions().isPresent()) {
                assertTrue(msg,
                        oldField.fields().size() == 1 &&
                        oldField.fields().get(0).type().equals(newElementType));
            }

        } else {
            assertEquals(format("%s: Field '%s' in API version %d has a different type in %s than in %s",
                    messageName,
                    fieldName, apiVersion, oldKafkaVersion, newKafkaVersion),
                    oldField.typeString(), newField.typeString());
        }
    }

    private boolean isPrimitive(FieldType oldElementType) {
        return isIntegral(oldElementType)
                || oldElementType instanceof FieldType.Float64FieldType
                || oldElementType instanceof FieldType.BoolFieldType;
    }

    private boolean isIntegral(FieldType oldElementType) {
        return oldElementType instanceof FieldType.Int8FieldType
                || oldElementType instanceof FieldType.Int16FieldType
                || oldElementType instanceof FieldType.Int32FieldType
                || oldElementType instanceof FieldType.Int64FieldType;
    }

    private void validateTagReuse(String messageName, String path, Map<String, Map<Integer, String>> tags, String fieldName, FieldSpec field) {
        if (field.tag().isPresent()) {
            Integer tag = field.tag().get();
            String oldFieldName = tags.computeIfAbsent(path, key -> new HashMap<>()).put(tag, fieldName);
            assertTrue(format("%s: Field '%s' uses tag %d but this was previously used for field '%s'",
                    messageName,
                    fieldName, tag, oldFieldName),
                    oldFieldName == null || fieldName.equals(oldFieldName));
        }
    }

    static class Diff {
        private final Map<String, FieldSpec> commonOld;
        private final Map<String, FieldSpec> commonNew;
        private final Map<String, FieldSpec> onlyOld;
        private final Map<String, FieldSpec> onlyNew;

        Diff(Map<String, FieldSpec> oldMap, Map<String, FieldSpec> newMap) {
            commonOld = new HashMap<>(oldMap);
            commonOld.keySet().retainAll(newMap.keySet());
            commonNew = new HashMap<>(newMap);
            commonNew.keySet().retainAll(oldMap.keySet());

            onlyOld = new HashMap<>(oldMap);
            onlyOld.keySet().removeAll(newMap.keySet());
            onlyNew = new HashMap<>(newMap);
            onlyNew.keySet().removeAll(oldMap.keySet());
        }

        Set<String> commonNames() {
            return commonNew.keySet();
        }

        Set<String> addedNames() {
            return onlyNew.keySet();
        }

        Set<String> removedNames() {
            return onlyOld.keySet();
        }
    }

    /**
     * Validates that, while fields may have been added or removed,
     * the order of common fields in the given {@code oldFields} and {@code newFields} is the same.
     */
    private String[] findFirstChangedOrder(List<FieldSpec> oldFields, List<FieldSpec> newFields) {
        int index = 0;
        String name = null;
        for (FieldSpec oldField : oldFields) {
            int newFieldIndex = indexOfField(newFields, oldField.name());
            if (newFieldIndex != -1) {
                if (newFieldIndex < index) {
                    return new String[]{oldField.name(), name};
                }
                index = newFieldIndex;
                name = newFields.get(newFieldIndex).name();
            }
        }
        return null;
    }

    private Map<Short, List<FieldSpec>> modelsByVersion(MessageSpec newSpec) {
        Map<Short, List<FieldSpec>> perVersionModels = new TreeMap<>();
        for (short messageVersion = newSpec.validVersions().lowest(); messageVersion <= newSpec.validVersions().highest(); messageVersion++) {
            StructSpec newStruct = newSpec.struct();
            perVersionModels.put(messageVersion, buildVersionModel(messageVersion, "", newStruct.fields()));
        }
        return perVersionModels;
    }

    private int indexOfField(List<FieldSpec> newFields, String fieldName) {
        for (int i = 0; i < newFields.size(); i++) {
            FieldSpec newField = newFields.get(i);
            if (fieldName.equals(newField.name())) {
                return i;
            }
        }
        return -1;
    }

    private List<FieldSpec> buildVersionModel(short messageVersion, String path, List<FieldSpec> fields) {
        List<FieldSpec> result = new ArrayList<>();
        for (FieldSpec field : fields) {
            String fieldPath = (path.isEmpty() ? "" : path + ".") + field.name();
            if (field.versions().contains(messageVersion)) {
                boolean isNullable = field.nullableVersions().contains(messageVersion);
                boolean isTagged = field.taggedVersions().contains(messageVersion);
                boolean isFlexible = field.flexibleVersions().flatMap(x -> Optional.of(x.contains(messageVersion))).orElse(false);
                //System.out.printf("Message version %s has field %s nullable=%s, tagged=%s, flexible=%s%n", messageVersion, field.name(), isNullable, isTagged, isFlexible);
                List<FieldSpec> childFields = buildVersionModel(messageVersion, fieldPath, field.fields());
                String versionStr = Short.toString(messageVersion);
                result.add(new FieldSpec(field.name(),
                        isTagged ? versionStr + "+" : versionStr, // versions must == tagged versions, so the open-ended rule applies to version
                        childFields, field.typeString(), field.mapKey(),
                        isNullable ? (isTagged ? versionStr + "+" : versionStr) : null,// nullableVersions == taggedVersions, so the open-ended rule applies to nullableVersion
                        field.defaultString(),
                        field.ignorable(),
                        field.entityType(),
                        field.about(),
                        isTagged ? versionStr + "+" : null, // tagged versions must be open-ended
                        isFlexible ? versionStr : null,
                        field.tagInteger(),
                        field.zeroCopy()));
            }
        }
        return result;
    }

    private static Map<String, FieldSpec> fieldMap(List<FieldSpec> fields) {
        return Collections.unmodifiableMap(fields.stream().collect(Collectors.toMap(e -> e.name(), Function.identity(),
            (k1, k2) -> {
                throw new RuntimeException();
            },
            () -> new LinkedHashMap<>())));
    }

}
