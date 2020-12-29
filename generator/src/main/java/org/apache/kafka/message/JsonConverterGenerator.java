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

import java.io.BufferedWriter;
import java.util.Iterator;

/**
 * Generates Kafka MessageData classes.
 */
public final class JsonConverterGenerator implements MessageClassGenerator {
    private final static String SUFFIX = "JsonConverter";
    private final String packageName;
    private final StructRegistry structRegistry;
    private final HeaderGenerator headerGenerator;
    private final CodeBuffer buffer;

    JsonConverterGenerator(String packageName) {
        this.packageName = packageName;
        this.structRegistry = new StructRegistry();
        this.headerGenerator = new HeaderGenerator(packageName);
        this.buffer = new CodeBuffer();
    }

    @Override
    public String outputName(MessageSpec spec) {
        return spec.dataClassName() + SUFFIX;
    }

    @Override
    public void generateAndWrite(MessageSpec message, BufferedWriter writer)
            throws Exception {
        structRegistry.register(message);
        headerGenerator.addStaticImport(String.format("%s.%s.*",
            packageName, message.dataClassName()));
        buffer.printf("public class %s {%n",
            MessageGenerator.capitalizeFirst(outputName(message)));
        buffer.incrementIndent();
        generateConverters(message.dataClassName(), message.struct(),
            message.validVersions());
        for (Iterator<StructRegistry.StructInfo> iter = structRegistry.structs();
                iter.hasNext(); ) {
            StructRegistry.StructInfo info = iter.next();
            buffer.printf("%n");
            buffer.printf("public static class %s {%n",
                MessageGenerator.capitalizeFirst(info.spec().name() + SUFFIX));
            buffer.incrementIndent();
            generateConverters(MessageGenerator.capitalizeFirst(info.spec().name()),
                info.spec(), info.parentVersions());
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
        headerGenerator.generate();
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }

    private void generateConverters(String name,
                                    StructSpec spec,
                                    Versions parentVersions) {
        generateRead(name, spec, parentVersions);
        generateWrite(name, spec, parentVersions);
        generateOverloadWrite(name);
    }

    private void generateRead(String className,
                              StructSpec struct,
                              Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        buffer.printf("public static %s read(JsonNode _node, short _version) {%n",
            className);
        buffer.incrementIndent();
        buffer.printf("%s _object = new %s();%n", className, className);
        VersionConditional.forVersions(struct.versions(), parentVersions).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        for (FieldSpec field : struct.fields()) {
            String sourceVariable = String.format("_%sNode", field.camelCaseName());
            buffer.printf("JsonNode %s = _node.get(\"%s\");%n",
                sourceVariable,
                field.camelCaseName());
            buffer.printf("if (%s == null) {%n", sourceVariable);
            buffer.incrementIndent();
            Versions mandatoryVersions = field.versions().subtract(field.taggedVersions());
            VersionConditional.forVersions(mandatoryVersions, curVersions).
                ifMember(__ -> {
                    buffer.printf("throw new RuntimeException(\"%s: unable to locate " +
                            "field \'%s\', which is mandatory in version \" + _version);%n",
                        className, field.camelCaseName());
                }).
                ifNotMember(__ -> {
                    buffer.printf("_object.%s = %s;%n", field.camelCaseName(),
                        field.fieldDefault(headerGenerator, structRegistry));
                }).
                generate(buffer);
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
            VersionConditional.forVersions(struct.versions(), curVersions).
                ifMember(presentVersions -> {
                    generateTargetFromJson(new Target(field,
                            sourceVariable,
                            className,
                        input -> String.format("_object.%s = %s", field.camelCaseName(), input)),
                        curVersions);
                }).ifNotMember(__ -> {
                    buffer.printf("throw new RuntimeException(\"%s: field \'%s\' is not " +
                        "supported in version \" + _version);%n",
                        className, field.camelCaseName());
                }).generate(buffer);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        buffer.printf("return _object;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateTargetFromJson(Target target, Versions curVersions) {
        if (target.field().type() instanceof FieldType.BoolFieldType) {
            buffer.printf("if (!%s.isBoolean()) {%n", target.sourceVariable());
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"%s expected Boolean type, " +
                "but got \" + _node.getNodeType());%n", target.humanReadableName());
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("%s;%n", target.assignmentStatement(
                target.sourceVariable() + ".asBoolean()"));
        } else if (target.field().type() instanceof FieldType.Int8FieldType) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("MessageUtil.jsonNodeToByte(%s, \"%s\")",
                    target.sourceVariable(), target.humanReadableName())));
        } else if (target.field().type() instanceof FieldType.Int16FieldType) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("MessageUtil.jsonNodeToShort(%s, \"%s\")",
                    target.sourceVariable(), target.humanReadableName())));
        } else if (target.field().type() instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("MessageUtil.jsonNodeToInt(%s, \"%s\")",
                    target.sourceVariable(), target.humanReadableName())));
        } else if (target.field().type() instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("MessageUtil.jsonNodeToLong(%s, \"%s\")",
                    target.sourceVariable(), target.humanReadableName())));
        } else if (target.field().type() instanceof FieldType.UUIDFieldType) {
            buffer.printf("if (!%s.isTextual()) {%n", target.sourceVariable());
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"%s expected a JSON string " +
                "type, but got \" + _node.getNodeType());%n", target.humanReadableName());
            buffer.decrementIndent();
            buffer.printf("}%n");
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(String.format(
                "Uuid.fromString(%s.asText())", target.sourceVariable())));
        } else if (target.field().type() instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("MessageUtil.jsonNodeToDouble(%s, \"%s\")",
                    target.sourceVariable(), target.humanReadableName())));
        } else {
            // Handle the variable length types.  All of them are potentially
            // nullable, so handle that here.
            IsNullConditional.forName(target.sourceVariable()).
                nullableVersions(target.field().nullableVersions()).
                possibleVersions(curVersions).
                conditionalGenerator((name, negated) ->
                    String.format("%s%s.isNull()", negated ? "!" : "", name)).
                ifNull(() -> {
                    buffer.printf("%s;%n", target.assignmentStatement("null"));
                }).
                ifShouldNotBeNull(() -> {
                    generateVariableLengthTargetFromJson(target, curVersions);
                }).
                generate(buffer);
        }
    }

    private void generateVariableLengthTargetFromJson(Target target, Versions curVersions) {
        if (target.field().type().isString()) {
            buffer.printf("if (!%s.isTextual()) {%n", target.sourceVariable());
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"%s expected a string " +
                "type, but got \" + _node.getNodeType());%n", target.humanReadableName());
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("%s.asText()", target.sourceVariable())));
        } else if (target.field().type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            if (target.field().zeroCopy()) {
                headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
                buffer.printf("%s;%n", target.assignmentStatement(
                    String.format("ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(%s, \"%s\"))",
                        target.sourceVariable(), target.humanReadableName())));
            } else {
                buffer.printf("%s;%n", target.assignmentStatement(
                    String.format("MessageUtil.jsonNodeToBinary(%s, \"%s\")",
                        target.sourceVariable(), target.humanReadableName())));
            }
        } else if (target.field().type().isRecords()) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
            headerGenerator.addImport(MessageGenerator.MEMORY_RECORDS_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("MemoryRecords.readableRecords(ByteBuffer.wrap(MessageUtil.jsonNodeToBinary(%s, \"%s\")))",
                    target.sourceVariable(), target.humanReadableName())));
        } else if (target.field().type().isArray()) {
            buffer.printf("if (!%s.isArray()) {%n", target.sourceVariable());
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"%s expected a JSON " +
                "array, but got \" + _node.getNodeType());%n", target.humanReadableName());
            buffer.decrementIndent();
            buffer.printf("}%n");
            String type = target.field().concreteJavaType(headerGenerator, structRegistry);
            buffer.printf("%s _collection = new %s();%n", type, type);
            buffer.printf("%s;%n", target.assignmentStatement("_collection"));
            headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
            buffer.printf("for (JsonNode _element : %s) {%n", target.sourceVariable());
            buffer.incrementIndent();
            generateTargetFromJson(target.arrayElementTarget(
                input -> String.format("_collection.add(%s)", input)),
                curVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else if (target.field().type().isStruct()) {
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("%s%s.read(%s, _version)",
                target.field().type().toString(), SUFFIX, target.sourceVariable())));
        } else {
            throw new RuntimeException("Unexpected type " + target.field().type());
        }
    }

    private void generateOverloadWrite(String className) {
        buffer.printf("public static JsonNode write(%s _object, short _version) {%n",
                className);
        buffer.incrementIndent();
        buffer.printf("return write(_object, _version, true);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateWrite(String className,
                               StructSpec struct,
                               Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        buffer.printf("public static JsonNode write(%s _object, short _version, boolean _serializeRecords) {%n",
            className);
        buffer.incrementIndent();
        VersionConditional.forVersions(struct.versions(), parentVersions).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        headerGenerator.addImport(MessageGenerator.OBJECT_NODE_CLASS);
        headerGenerator.addImport(MessageGenerator.JSON_NODE_FACTORY_CLASS);
        buffer.printf("ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);%n");
        for (FieldSpec field : struct.fields()) {
            Target target = new Target(field,
                String.format("_object.%s", field.camelCaseName()),
                field.camelCaseName(),
                input -> String.format("_node.set(\"%s\", %s)", field.camelCaseName(), input));
            VersionConditional cond = VersionConditional.forVersions(field.versions(), curVersions).
                ifMember(presentVersions -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifMember(presentAndTaggedVersions -> {
                            field.generateNonDefaultValueCheck(headerGenerator,
                                structRegistry, buffer, "_object.", field.nullableVersions());
                            buffer.incrementIndent();
                            if (field.defaultString().equals("null")) {
                                // If the default was null, and we already checked that this field was not
                                // the default, we can omit further null checks.
                                generateTargetToJson(target.nonNullableCopy(), presentAndTaggedVersions);
                            } else {
                                generateTargetToJson(target, presentAndTaggedVersions);
                            }
                            buffer.decrementIndent();
                            buffer.printf("}%n");
                        }).
                        ifNotMember(presentAndNotTaggedVersions -> {
                            generateTargetToJson(target, presentAndNotTaggedVersions);
                        }).
                        generate(buffer);
                });
            if (!field.ignorable()) {
                cond.ifNotMember(__ -> {
                    field.generateNonIgnorableFieldCheck(headerGenerator,
                        structRegistry, "_object.", buffer);
                });
            }
            cond.generate(buffer);
        }
        buffer.printf("return _node;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateTargetToJson(Target target, Versions versions) {
        if (target.field().type() instanceof FieldType.BoolFieldType) {
            headerGenerator.addImport(MessageGenerator.BOOLEAN_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("BooleanNode.valueOf(%s)", target.sourceVariable())));
        } else if ((target.field().type() instanceof FieldType.Int8FieldType) ||
            (target.field().type() instanceof FieldType.Int16FieldType)) {
            headerGenerator.addImport(MessageGenerator.SHORT_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new ShortNode(%s)", target.sourceVariable())));
        } else if (target.field().type() instanceof FieldType.Int32FieldType) {
            headerGenerator.addImport(MessageGenerator.INT_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new IntNode(%s)", target.sourceVariable())));
        } else if (target.field().type() instanceof FieldType.Int64FieldType) {
            headerGenerator.addImport(MessageGenerator.LONG_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new LongNode(%s)", target.sourceVariable())));
        } else if (target.field().type() instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.TEXT_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new TextNode(%s.toString())", target.sourceVariable())));
        } else if (target.field().type() instanceof FieldType.Float64FieldType) {
            headerGenerator.addImport(MessageGenerator.DOUBLE_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new DoubleNode(%s)", target.sourceVariable())));
        } else {
            // Handle the variable length types.  All of them are potentially
            // nullable, so handle that here.
            IsNullConditional.forName(target.sourceVariable()).
                nullableVersions(target.field().nullableVersions()).
                possibleVersions(versions).
                conditionalGenerator((name, negated) ->
                    String.format("%s %s= null", name, negated ? "!" : "=")).
                ifNull(() -> {
                    headerGenerator.addImport(MessageGenerator.NULL_NODE_CLASS);
                    buffer.printf("%s;%n", target.assignmentStatement("NullNode.instance"));
                }).
                ifShouldNotBeNull(() -> {
                    generateVariableLengthTargetToJson(target, versions);
                }).
                generate(buffer);
        }
    }

    private void generateVariableLengthTargetToJson(Target target, Versions versions) {
        if (target.field().type().isString()) {
            headerGenerator.addImport(MessageGenerator.TEXT_NODE_CLASS);
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new TextNode(%s)", target.sourceVariable())));
        } else if (target.field().type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.BINARY_NODE_CLASS);
            if (target.field().zeroCopy()) {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
                buffer.printf("%s;%n", target.assignmentStatement(
                    String.format("new BinaryNode(MessageUtil.byteBufferToArray(%s))",
                        target.sourceVariable())));
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
                buffer.printf("%s;%n", target.assignmentStatement(
                    String.format("new BinaryNode(Arrays.copyOf(%s, %s.length))",
                            target.sourceVariable(), target.sourceVariable())));
            }
        } else if (target.field().type().isRecords()) {
            headerGenerator.addImport(MessageGenerator.BINARY_NODE_CLASS);
            headerGenerator.addImport(MessageGenerator.INT_NODE_CLASS);
            // KIP-673: When logging requests/responses, we do not serialize the record, instead we
            // output its sizeInBytes, because outputting the bytes is not very useful and can be
            // quite expensive. Otherwise, we will serialize the record.
            buffer.printf("if (_serializeRecords) {%n");
            buffer.incrementIndent();
            buffer.printf("%s;%n", target.assignmentStatement("new BinaryNode(new byte[]{})"));
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
            buffer.printf("_node.set(\"%sSizeInBytes\", new IntNode(%s.sizeInBytes()));%n",
                    target.field().camelCaseName(),
                    target.sourceVariable());
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else if (target.field().type().isArray()) {
            headerGenerator.addImport(MessageGenerator.ARRAY_NODE_CLASS);
            headerGenerator.addImport(MessageGenerator.JSON_NODE_FACTORY_CLASS);
            FieldType.ArrayType arrayType = (FieldType.ArrayType) target.field().type();
            FieldType elementType = arrayType.elementType();
            String arrayInstanceName = String.format("_%sArray",
                target.field().camelCaseName());
            buffer.printf("ArrayNode %s = new ArrayNode(JsonNodeFactory.instance);%n",
                arrayInstanceName);
            buffer.printf("for (%s _element : %s) {%n",
                elementType.getBoxedJavaType(headerGenerator), target.sourceVariable());
            buffer.incrementIndent();
            generateTargetToJson(target.arrayElementTarget(
                input -> String.format("%s.add(%s)", arrayInstanceName, input)),
                versions);
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("%s;%n", target.assignmentStatement(arrayInstanceName));
        } else if (target.field().type().isStruct()) {
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("%sJsonConverter.write(%s, _version, _serializeRecords)",
                    target.field().type().toString(), target.sourceVariable())));
        } else {
            throw new RuntimeException("unknown type " + target.field().type());
        }
    }

}
