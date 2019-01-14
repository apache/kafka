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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Generates Schemas for Kafka MessageData classes.
 */
final class SchemaGenerator {
    /**
     * Schema information for a particular message.
     */
    static class MessageInfo {
        /**
         * The versions of this message that we want to generate a schema for.
         * This will be constrained by the valid versions for the parent objects.
         * For example, if the parent message is valid for versions 0 and 1,
         * we will only generate a version 0 and version 1 schema for child classes,
         * even if their valid versions are "0+".
         */
        private final Versions versions;

        /**
         * Maps versions to schema declaration code.  If the schema for a
         * particular version is the same as that of a previous version,
         * there will be no entry in the map for it.
         */
        private final TreeMap<Short, CodeBuffer> schemaForVersion;

        MessageInfo(Versions versions) {
            this.versions = versions;
            this.schemaForVersion = new TreeMap<>();
        }
    }

    /**
     * The header file generator.  This is shared with the MessageDataGenerator
     * instance that owns this SchemaGenerator.
     */
    private final HeaderGenerator headerGenerator;

    /**
     * Maps message names to message information.
     */
    private final Map<String, MessageInfo> messages;

    SchemaGenerator(HeaderGenerator headerGenerator) {
        this.headerGenerator = headerGenerator;
        this.messages = new HashMap<>();
    }

    void generateSchemas(MessageSpec message) throws Exception {
        generateSchemas(message.generatedClassName(), message.struct(),
            message.struct().versions());
    }

    void generateSchemas(String className, StructSpec struct,
                         Versions parentVersions) throws Exception {
        Versions versions = parentVersions.intersect(struct.versions());
        MessageInfo messageInfo = messages.get(className);
        if (messageInfo != null) {
            return;
        }
        messageInfo = new MessageInfo(versions);
        messages.put(className, messageInfo);
        // Process the leaf classes first.
        for (FieldSpec field : struct.fields()) {
            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                generateSchemas(arrayType.elementType().toString(), field.toStruct(), versions);
            } else if (field.type().isStruct()) {
                generateSchemas(field.type().toString(), field.toStruct(), versions);
            }
        }
        CodeBuffer prev = null;
        for (short v = versions.lowest(); v <= versions.highest(); v++) {
            CodeBuffer cur = new CodeBuffer();
            generateSchemaForVersion(struct, v, cur);
            // If this schema version is different from the previous one,
            // create a new map entry.
            if (!cur.equals(prev)) {
                messageInfo.schemaForVersion.put(v, cur);
            }
            prev = cur;
        }
    }

    private void generateSchemaForVersion(StructSpec struct, short version,
                                          CodeBuffer buffer) throws Exception {
        // Find the last valid field index.
        int lastValidIndex = struct.fields().size() - 1;
        while (true) {
            if (lastValidIndex < 0) {
                break;
            }
            FieldSpec field = struct.fields().get(lastValidIndex);
            if (field.versions().contains(version)) {
                break;
            }
            lastValidIndex--;
        }

        headerGenerator.addImport(MessageGenerator.SCHEMA_CLASS);
        buffer.printf("new Schema(%n");
        buffer.incrementIndent();
        for (int i = 0; i <= lastValidIndex; i++) {
            FieldSpec field = struct.fields().get(i);
            if (!field.versions().contains(version)) {
                continue;
            }
            headerGenerator.addImport(MessageGenerator.FIELD_CLASS);
            buffer.printf("new Field(\"%s\", %s, \"%s\")%s%n",
                field.snakeCaseName(),
                fieldTypeToSchemaType(field, version),
                field.about(),
                i == lastValidIndex ? "" : ",");
        }
        buffer.decrementIndent();
        buffer.printf(");%n");
    }

    private String fieldTypeToSchemaType(FieldSpec field, short version) {
        return fieldTypeToSchemaType(field.type(),
            field.nullableVersions().contains(version),
            version);
    }

    private String fieldTypeToSchemaType(FieldType type, boolean nullable, short version) {
        if (type instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.BOOLEAN";
        } else if (type instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT8";
        } else if (type instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT16";
        } else if (type instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT32";
        } else if (type instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return "Type.INT64";
        } else if (type instanceof FieldType.StringFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            return nullable ? "Type.NULLABLE_STRING" : "Type.STRING";
        } else if (type instanceof FieldType.BytesFieldType) {
            headerGenerator.addImport(MessageGenerator.TYPE_CLASS);
            return nullable ? "Type.NULLABLE_BYTES" : "Type.BYTES";
        } else if (type.isArray()) {
            headerGenerator.addImport(MessageGenerator.ARRAYOF_CLASS);
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            String prefix = nullable ? "ArrayOf.nullable" : "new ArrayOf";
            return String.format("%s(%s)", prefix,
                fieldTypeToSchemaType(arrayType.elementType(), false, version));
        } else if (type.isStruct()) {
            if (nullable) {
                throw new RuntimeException("Type " + type + " cannot be nullable.");
            }
            return String.format("%s.SCHEMA_%d", type.toString(),
                floorVersion(type.toString(), version));
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
    }

    /**
     * Find the lowest schema version for a given class that is the same as the
     * given version.
     */
    private short floorVersion(String className, short v) {
        MessageInfo message = messages.get(className);
        return message.schemaForVersion.floorKey(v);
    }

    /**
     * Write the message schema to the provided buffer.
     *
     * @param className     The class name.
     * @param buffer        The destination buffer.
     */
    void writeSchema(String className, CodeBuffer buffer) throws Exception {
        MessageInfo messageInfo = messages.get(className);
        Versions versions = messageInfo.versions;

        for (short v = versions.lowest(); v <= versions.highest(); v++) {
            CodeBuffer declaration = messageInfo.schemaForVersion.get(v);
            if (declaration == null) {
                buffer.printf("public static final Schema SCHEMA_%d = SCHEMA_%d;%n", v, v - 1);
            } else {
                buffer.printf("public static final Schema SCHEMA_%d =%n", v);
                buffer.incrementIndent();
                declaration.write(buffer);
                buffer.decrementIndent();
            }
            buffer.printf("%n");
        }
        buffer.printf("public static final Schema[] SCHEMAS = new Schema[] {%n");
        buffer.incrementIndent();
        for (short v = 0; v < versions.lowest(); v++) {
            buffer.printf("null%s%n", (v == versions.highest()) ? "" : ",");
        }
        for (short v = versions.lowest(); v <= versions.highest(); v++) {
            buffer.printf("SCHEMA_%d%s%n", v, (v == versions.highest()) ? "" : ",");
        }
        buffer.decrementIndent();
        buffer.printf("};%n");
        buffer.printf("%n");
    }
}
