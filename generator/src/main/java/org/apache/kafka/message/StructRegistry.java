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

package org.apache.kafka.message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Contains structure data for Kafka MessageData classes.
 */
final class StructRegistry {
    private final Map<String, StructInfo> structs;
    private final Set<String> commonStructNames;

    static class StructInfo {
        /**
         * The specification for this structure.
         */
        private final StructSpec spec;

        /**
         * The versions which the parent(s) of this structure can have.  If this is a
         * top-level structure, this will be equal to the versions which the
         * overall message can have.
         */
        private final Versions parentVersions;

        StructInfo(StructSpec spec, Versions parentVersions) {
            this.spec = spec;
            this.parentVersions = parentVersions;
        }

        public StructSpec spec() {
            return spec;
        }

        public Versions parentVersions() {
            return parentVersions;
        }
    }

    StructRegistry() {
        this.structs = new TreeMap<>();
        this.commonStructNames = new TreeSet<>();
    }

    /**
     * Register all the structures contained a message spec.
     */
    void register(MessageSpec message) throws Exception {
        // Register common structures.
        for (StructSpec struct : message.commonStructs()) {
            if (!MessageGenerator.firstIsCapitalized(struct.name())) {
                throw new RuntimeException("Can't process structure " + struct.name() +
                        ": the first letter of structure names must be capitalized.");
            }
            if (structs.containsKey(struct.name())) {
                throw new RuntimeException("Common struct " + struct.name() + " was specified twice.");
            }
            structs.put(struct.name(), new StructInfo(struct, struct.versions()));
            commonStructNames.add(struct.name());
        }
        // Register inline structures.
        addStructSpecs(message.validVersions(), message.fields());
    }

    @SuppressWarnings("unchecked")
    private void addStructSpecs(Versions parentVersions, List<FieldSpec> fields) {
        for (FieldSpec field : fields) {
            String elementName = null;
            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                elementName = arrayType.elementName();
            } else if (field.type().isStruct()) {
                elementName = field.name();
            }
            if (elementName != null) {
                if (commonStructNames.contains(elementName)) {
                    // If we're using a common structure, we can't specify its fields.
                    // The fields should be specified in the commonStructs area.
                    if (!field.fields().isEmpty()) {
                        throw new RuntimeException("Can't re-specify the common struct " +
                                elementName + " as an inline struct.");
                    }
                } else if (structs.containsKey(elementName)) {
                    // Inline structures should only appear once.
                    throw new RuntimeException("Struct " + elementName +
                        " was specified twice.");
                } else {
                    // Synthesize a StructSpec object out of the fields.
                    StructSpec spec = new StructSpec(elementName,
                            field.versions().toString(),
                            field.fields());
                    structs.put(elementName, new StructInfo(spec, parentVersions));
                }
                addStructSpecs(parentVersions.intersect(field.versions()), field.fields());
            }
        }
    }

    /**
     * Locate the struct corresponding to a field.
     */
    @SuppressWarnings("unchecked")
    StructSpec findStruct(FieldSpec field) {
        String structFieldName;
        if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            structFieldName = arrayType.elementName();
        } else if (field.type().isStruct()) {
            structFieldName = field.name();
        } else {
            throw new RuntimeException("Field " + field.name() +
                    " cannot be treated as a structure.");
        }
        StructInfo structInfo = structs.get(structFieldName);
        if (structInfo == null) {
            throw new RuntimeException("Unable to locate a specification for the structure " +
                    structFieldName);
        }
        return structInfo.spec;
    }

    /**
     * Return true if the field is a struct array with keys.
     */
    @SuppressWarnings("unchecked")
    boolean isStructArrayWithKeys(FieldSpec field) {
        if (!field.type().isArray()) {
            return false;
        }
        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
        if (!arrayType.isStructArray()) {
            return false;
        }
        StructInfo structInfo = structs.get(arrayType.elementName());
        if (structInfo == null) {
            throw new RuntimeException("Unable to locate a specification for the structure " +
                    arrayType.elementName());
        }
        return structInfo.spec.hasKeys();
    }

    Set<String> commonStructNames() {
        return commonStructNames;
    }

    /**
     * Returns an iterator that will step through all the common structures.
     */
    Iterator<StructSpec> commonStructs() {
        return new Iterator<StructSpec>() {
            private final Iterator<String> iter = commonStructNames.iterator();

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public StructSpec next() {
                return structs.get(iter.next()).spec;
            }
        };
    }

    Iterator<StructInfo> structs() {
        return structs.values().iterator();
    }
}
