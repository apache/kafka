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

import java.io.Writer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Generates Kafka MessageData classes.
 */
public final class MessageDataGenerator {
    private final static String TAGGED_FIELDS_SECTION_NAME = "_tagged_fields";

    private final StructRegistry structRegistry;
    private final HeaderGenerator headerGenerator;
    private final SchemaGenerator schemaGenerator;
    private final CodeBuffer buffer;
    private Versions messageFlexibleVersions;

    MessageDataGenerator(String packageName) {
        this.structRegistry = new StructRegistry();
        this.headerGenerator = new HeaderGenerator(packageName);
        this.schemaGenerator = new SchemaGenerator(headerGenerator, structRegistry);
        this.buffer = new CodeBuffer();
    }

    void generate(MessageSpec message) throws Exception {
        if (message.struct().versions().contains(Short.MAX_VALUE)) {
            throw new RuntimeException("Message " + message.name() + " does " +
                "not specify a maximum version.");
        }
        structRegistry.register(message);
        schemaGenerator.generateSchemas(message);
        messageFlexibleVersions = message.flexibleVersions();
        generateClass(Optional.of(message),
            message.generatedClassName(),
            message.struct(),
            message.struct().versions());
        headerGenerator.generate();
    }

    void write(Writer writer) throws Exception {
        headerGenerator.buffer().write(writer);
        buffer.write(writer);
    }

    private void generateClass(Optional<MessageSpec> topLevelMessageSpec,
                               String className,
                               StructSpec struct,
                               Versions parentVersions) throws Exception {
        buffer.printf("%n");
        boolean isTopLevel = topLevelMessageSpec.isPresent();
        boolean isSetElement = struct.hasKeys(); // Check if the class is inside a set.
        if (isTopLevel && isSetElement) {
            throw new RuntimeException("Cannot set mapKey on top level fields.");
        }
        generateClassHeader(className, isTopLevel, isSetElement);
        buffer.incrementIndent();
        generateFieldDeclarations(struct, isSetElement);
        buffer.printf("%n");
        schemaGenerator.writeSchema(className, buffer);
        generateClassConstructors(className, struct, isSetElement);
        buffer.printf("%n");
        if (isTopLevel) {
            generateShortAccessor("apiKey", topLevelMessageSpec.get().apiKey().orElse((short) -1));
        }
        buffer.printf("%n");
        generateShortAccessor("lowestSupportedVersion", parentVersions.lowest());
        buffer.printf("%n");
        generateShortAccessor("highestSupportedVersion", parentVersions.highest());
        buffer.printf("%n");
        generateClassReader(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassWriter(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassFromStruct(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassToStruct(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassFromJson(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassToJson(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassSize(className, struct, parentVersions);
        if (isSetElement) {
            buffer.printf("%n");
            generateClassEquals(className, struct, true);
        }
        buffer.printf("%n");
        generateClassEquals(className, struct, false);
        buffer.printf("%n");
        generateClassHashCode(struct, isSetElement);
        buffer.printf("%n");
        generateClassDuplicate(className, struct);
        buffer.printf("%n");
        generateClassToString(className, struct);
        generateFieldAccessors(struct, isSetElement);
        buffer.printf("%n");
        generateUnknownTaggedFieldsAccessor(struct);
        generateFieldMutators(struct, className, isSetElement);

        if (!isTopLevel) {
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        generateSubclasses(className, struct, parentVersions, isSetElement);
        if (isTopLevel) {
            for (Iterator<StructSpec> iter = structRegistry.commonStructs(); iter.hasNext(); ) {
                StructSpec commonStruct = iter.next();
                generateClass(Optional.empty(),
                        commonStruct.name(),
                        commonStruct,
                        commonStruct.versions());
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
    }

    private void generateClassHeader(String className, boolean isTopLevel,
                                     boolean isSetElement) {
        Set<String> implementedInterfaces = new HashSet<>();
        if (isTopLevel) {
            implementedInterfaces.add("ApiMessage");
            headerGenerator.addImport(MessageGenerator.API_MESSAGE_CLASS);
        } else {
            implementedInterfaces.add("Message");
            headerGenerator.addImport(MessageGenerator.MESSAGE_CLASS);
        }
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
            implementedInterfaces.add("ImplicitLinkedHashMultiCollection.Element");
        }
        Set<String> classModifiers = new HashSet<>();
        classModifiers.add("public");
        if (!isTopLevel) {
            classModifiers.add("static");
        }
        buffer.printf("%s class %s implements %s {%n",
            String.join(" ", classModifiers),
            className,
            String.join(", ", implementedInterfaces));
    }

    private void generateSubclasses(String className, StructSpec struct,
            Versions parentVersions, boolean isSetElement) throws Exception {
        for (FieldSpec field : struct.fields()) {
            if (field.type().isStructArray()) {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                if (!structRegistry.commonStructNames().contains(arrayType.elementName())) {
                    generateClass(Optional.empty(),
                            arrayType.elementType().toString(),
                            structRegistry.findStruct(field),
                            parentVersions.intersect(struct.versions()));
                }
            } else if (field.type().isStruct()) {
                if (!structRegistry.commonStructNames().contains(field.name())) {
                    generateClass(Optional.empty(),
                            field.type().toString(),
                            structRegistry.findStruct(field),
                            parentVersions.intersect(struct.versions()));
                }
            }
        }
        if (isSetElement) {
            generateHashSet(className, struct);
        }
    }

    private void generateHashSet(String className, StructSpec struct) {
        buffer.printf("%n");
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("public static class %s extends ImplicitLinkedHashMultiCollection<%s> {%n",
            collectionType(className), className);
        buffer.incrementIndent();
        generateHashSetZeroArgConstructor(className);
        buffer.printf("%n");
        generateHashSetSizeArgConstructor(className);
        buffer.printf("%n");
        generateHashSetIteratorConstructor(className);
        buffer.printf("%n");
        generateHashSetFindMethod(className, struct);
        buffer.printf("%n");
        generateHashSetFindAllMethod(className, struct);
        buffer.printf("%n");
        generateCollectionDuplicateMethod(className, struct);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetZeroArgConstructor(String className) {
        buffer.printf("public %s() {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super();%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetSizeArgConstructor(String className) {
        buffer.printf("public %s(int expectedNumElements) {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super(expectedNumElements);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetIteratorConstructor(String className) {
        headerGenerator.addImport(MessageGenerator.ITERATOR_CLASS);
        buffer.printf("public %s(Iterator<%s> iterator) {%n", collectionType(className), className);
        buffer.incrementIndent();
        buffer.printf("super(iterator);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetFindMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public %s find(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct));
        buffer.incrementIndent();
        generateKeyElement(className, struct);
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("return find(_key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetFindAllMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public List<%s> findAll(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct));
        buffer.incrementIndent();
        generateKeyElement(className, struct);
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("return findAll(_key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateKeyElement(String className, StructSpec struct) {
        buffer.printf("%s _key = new %s();%n", className, className);
        for (FieldSpec field : struct.fields()) {
            if (field.mapKey()) {
                buffer.printf("_key.set%s(%s);%n",
                    field.capitalizedCamelCaseName(),
                    field.camelCaseName());
            }
        }
    }

    private String commaSeparatedHashSetFieldAndTypes(StructSpec struct) {
        return struct.fields().stream().
            filter(f -> f.mapKey()).
            map(f -> String.format("%s %s", fieldConcreteJavaType(f), f.camelCaseName())).
            collect(Collectors.joining(", "));
    }

    private void generateCollectionDuplicateMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public %s duplicate() {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("%s _duplicate = new %s(size());%n",
            collectionType(className), collectionType(className));
        buffer.printf("for (%s _element : this) {%n", className);
        buffer.incrementIndent();
        buffer.printf("_duplicate.add(_element.duplicate());%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("return _duplicate;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldDeclarations(StructSpec struct, boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldDeclaration(field);
        }
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS);
        buffer.printf("private List<RawTaggedField> _unknownTaggedFields;%n");
        if (isSetElement) {
            buffer.printf("private int next;%n");
            buffer.printf("private int prev;%n");
        }
    }

    private void generateFieldDeclaration(FieldSpec field) {
        buffer.printf("private %s %s;%n",
            fieldAbstractJavaType(field), field.camelCaseName());
    }

    private void generateFieldAccessors(StructSpec struct, boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldAccessor(field);
        }
        if (isSetElement) {
            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateAccessor("int", "next", "next");

            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateAccessor("int", "prev", "prev");
        }
    }

    private void generateUnknownTaggedFieldsAccessor(StructSpec struct) {
        buffer.printf("@Override%n");
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS);
        buffer.printf("public List<RawTaggedField> unknownTaggedFields() {%n");
        buffer.incrementIndent();
        // Optimize _unknownTaggedFields by not creating a new list object
        // unless we need it.
        buffer.printf("if (_unknownTaggedFields == null) {%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
        buffer.printf("_unknownTaggedFields = new ArrayList<>(0);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("return _unknownTaggedFields;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");

    }

    private void generateFieldMutators(StructSpec struct, String className,
                                       boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldMutator(className, field);
        }
        if (isSetElement) {
            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateSetter("int", "setNext", "next");

            buffer.printf("%n");
            buffer.printf("@Override%n");
            generateSetter("int", "setPrev", "prev");
        }
    }

    private static String collectionType(String baseType) {
        return baseType + "Collection";
    }

    private String fieldAbstractJavaType(FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            return "boolean";
        } else if (field.type() instanceof FieldType.Int8FieldType) {
            return "byte";
        } else if (field.type() instanceof FieldType.Int16FieldType) {
            return "short";
        } else if (field.type() instanceof FieldType.Int32FieldType) {
            return "int";
        } else if (field.type() instanceof FieldType.Int64FieldType) {
            return "long";
        } else if (field.type() instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            return "UUID";
        } else if (field.type() instanceof FieldType.Float64FieldType) {
            return "double";
        } else if (field.type().isString()) {
            return "String";
        } else if (field.type().isBytes()) {
            if (field.zeroCopy()) {
                headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
                return "ByteBuffer";
            } else {
                return "byte[]";
            }
        } else if (field.type() instanceof FieldType.RecordsFieldType) {
            headerGenerator.addImport(MessageGenerator.BASE_RECORDS_CLASS);
            return "BaseRecords";
        } else if (field.type().isStruct()) {
            return MessageGenerator.capitalizeFirst(field.typeString());
        } else if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (structRegistry.isStructArrayWithKeys(field)) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                return collectionType(arrayType.elementType().toString());
            } else {
                headerGenerator.addImport(MessageGenerator.LIST_CLASS);
                return "List<" + getBoxedJavaType(arrayType.elementType()) + ">";
            }
        } else {
            throw new RuntimeException("Unknown field type " + field.type());
        }
    }

    private String fieldConcreteJavaType(FieldSpec field) {
        if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (structRegistry.isStructArrayWithKeys(field)) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                return collectionType(arrayType.elementType().toString());
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                return "ArrayList<" + getBoxedJavaType(arrayType.elementType()) + ">";
            }
        } else {
            return fieldAbstractJavaType(field);
        }
    }

    private void generateClassConstructors(String className, StructSpec struct, boolean isSetElement) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS);
        buffer.printf("public %s(Readable _readable, short _version) {%n", className);
        buffer.incrementIndent();
        buffer.printf("read(_readable, _version);%n");
        generateConstructorEpilogue(isSetElement);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");

        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("public %s(Struct _struct, short _version) {%n", className);
        buffer.incrementIndent();
        buffer.printf("fromStruct(_struct, _version);%n");
        generateConstructorEpilogue(isSetElement);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");

        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        buffer.printf("public %s(JsonNode _node, short _version) {%n", className);
        buffer.incrementIndent();
        buffer.printf("fromJson(_node, _version);%n");
        generateConstructorEpilogue(isSetElement);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");

        buffer.printf("public %s() {%n", className);
        buffer.incrementIndent();
        for (FieldSpec field : struct.fields()) {
            buffer.printf("this.%s = %s;%n",
                field.camelCaseName(), fieldDefault(field));
        }
        generateConstructorEpilogue(isSetElement);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateConstructorEpilogue(boolean isSetElement) {
        if (isSetElement) {
            headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_COLLECTION_CLASS);
            buffer.printf("this.prev = ImplicitLinkedHashCollection.INVALID_INDEX;%n");
            buffer.printf("this.next = ImplicitLinkedHashCollection.INVALID_INDEX;%n");
        }
    }

    private void generateShortAccessor(String name, short val) {
        buffer.printf("@Override%n");
        buffer.printf("public short %s() {%n", name);
        buffer.incrementIndent();
        buffer.printf("return %d;%n", val);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassReader(String className, StructSpec struct,
                                     Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void read(Readable _readable, short _version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        for (FieldSpec field : struct.fields()) {
            Versions fieldFlexibleVersions = fieldFlexibleVersions(field);
            if (!field.taggedVersions().intersect(fieldFlexibleVersions).equals(field.taggedVersions())) {
                throw new RuntimeException("Field " + field.name() + " specifies tagged " +
                    "versions " + field.taggedVersions() + " that are not a subset of the " +
                    "flexible versions " + fieldFlexibleVersions);
            }
            Versions mandatoryVersions = field.versions().subtract(field.taggedVersions());
            VersionConditional.forVersions(mandatoryVersions, curVersions).
                alwaysEmitBlockScope(field.type().isVariableLength()).
                ifNotMember(__ -> {
                    // If the field is not present, or is tagged, set it to its default here.
                    buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
                }).
                ifMember(presentAndUntaggedVersions -> {
                    if (field.type().isVariableLength() && !field.type().isStruct()) {
                        ClauseGenerator callGenerateVariableLengthReader = versions -> {
                            generateVariableLengthReader(fieldFlexibleVersions(field),
                                field.camelCaseName(),
                                field.type(),
                                versions,
                                field.nullableVersions(),
                                String.format("this.%s = ", field.camelCaseName()),
                                String.format(";%n"),
                                structRegistry.isStructArrayWithKeys(field),
                                field.zeroCopy());
                        };
                        // For arrays where the field type needs to be serialized differently in flexible
                        // versions, lift the flexible version check outside of the array.
                        // This may mean generating two separate 'for' loops-- one for flexible
                        // versions, and one for regular versions.
                        if (field.type().isArray() &&
                            ((FieldType.ArrayType) field.type()).elementType().
                                serializationIsDifferentInFlexibleVersions()) {
                            VersionConditional.forVersions(fieldFlexibleVersions(field),
                                    presentAndUntaggedVersions).
                                ifMember(callGenerateVariableLengthReader).
                                ifNotMember(callGenerateVariableLengthReader).
                                generate(buffer);
                        } else {
                            callGenerateVariableLengthReader.generate(presentAndUntaggedVersions);
                        }
                    } else {
                        buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                            primitiveReadExpression(field.type()));
                    }
                }).
                generate(buffer);
        }
        buffer.printf("this._unknownTaggedFields = null;%n");
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).
            ifMember(curFlexibleVersions -> {
                buffer.printf("int _numTaggedFields = _readable.readUnsignedVarint();%n");
                buffer.printf("for (int _i = 0; _i < _numTaggedFields; _i++) {%n");
                buffer.incrementIndent();
                buffer.printf("int _tag = _readable.readUnsignedVarint();%n");
                buffer.printf("int _size = _readable.readUnsignedVarint();%n");
                buffer.printf("switch (_tag) {%n");
                buffer.incrementIndent();
                for (FieldSpec field : struct.fields()) {
                    Versions validTaggedVersions = field.versions().intersect(field.taggedVersions());
                    if (!validTaggedVersions.empty()) {
                        if (!field.tag().isPresent()) {
                            throw new RuntimeException("Field " + field.name() + " has tagged versions, but no tag.");
                        }
                        buffer.printf("case %d: {%n", field.tag().get());
                        buffer.incrementIndent();
                        VersionConditional.forVersions(validTaggedVersions, curFlexibleVersions).
                            ifMember(presentAndTaggedVersions -> {
                                if (field.type().isVariableLength() && !field.type().isStruct()) {
                                    // All tagged fields are serialized using the new-style
                                    // flexible versions serialization.
                                    generateVariableLengthReader(fieldFlexibleVersions(field),
                                        field.camelCaseName(),
                                        field.type(),
                                        presentAndTaggedVersions,
                                        field.nullableVersions(),
                                        String.format("this.%s = ", field.camelCaseName()),
                                        String.format(";%n"),
                                        structRegistry.isStructArrayWithKeys(field),
                                        field.zeroCopy());
                                } else {
                                    buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                                        primitiveReadExpression(field.type()));
                                }
                                buffer.printf("break;%n");
                            }).
                            ifNotMember(__ -> {
                                buffer.printf("throw new RuntimeException(\"Tag %d is not " +
                                    "valid for version \" + _version);%n", field.tag().get());
                            }).
                            generate(buffer);
                        buffer.decrementIndent();
                        buffer.printf("}%n");
                    }
                }
                buffer.printf("default:%n");
                buffer.incrementIndent();
                buffer.printf("this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);%n");
                buffer.printf("break;%n");
                buffer.decrementIndent();
                buffer.decrementIndent();
                buffer.printf("}%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
            }).
            generate(buffer);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String primitiveReadExpression(FieldType type) {
        if (type instanceof FieldType.BoolFieldType) {
            return "_readable.readByte() != 0";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "_readable.readByte()";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "_readable.readShort()";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "_readable.readInt()";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "_readable.readLong()";
        } else if (type instanceof FieldType.UUIDFieldType) {
            return "_readable.readUUID()";
        } else if (type instanceof FieldType.Float64FieldType) {
            return "_readable.readDouble()";
        } else if (type.isStruct()) {
            return String.format("new %s(_readable, _version)", type.toString());
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateVariableLengthReader(Versions fieldFlexibleVersions,
                                              String name,
                                              FieldType type,
                                              Versions possibleVersions,
                                              Versions nullableVersions,
                                              String assignmentPrefix,
                                              String assignmentSuffix,
                                              boolean isStructArrayWithKeys,
                                              boolean zeroCopy) {
        String lengthVar = type.isArray() ? "arrayLength" : "length";
        buffer.printf("int %s;%n", lengthVar);
        VersionConditional.forVersions(fieldFlexibleVersions, possibleVersions).
            ifMember(__ -> {
                buffer.printf("%s = _readable.readUnsignedVarint() - 1;%n", lengthVar);
            }).
            ifNotMember(__ -> {
                if (type.isString()) {
                    buffer.printf("%s = _readable.readShort();%n", lengthVar);
                } else if (type.isBytes() || type.isArray() || type.isRecords()) {
                    buffer.printf("%s = _readable.readInt();%n", lengthVar);
                } else {
                    throw new RuntimeException("Can't handle variable length type " + type);
                }
            }).
            generate(buffer);
        buffer.printf("if (%s < 0) {%n", lengthVar);
        buffer.incrementIndent();
        VersionConditional.forVersions(nullableVersions, possibleVersions).
            ifNotMember(__ -> {
                buffer.printf("throw new RuntimeException(\"non-nullable field %s " +
                    "was serialized as null\");%n", name);
            }).
            ifMember(__ -> {
                buffer.printf("%snull%s", assignmentPrefix, assignmentSuffix);
            }).
            generate(buffer);
        buffer.decrementIndent();
        if (type.isString()) {
            buffer.printf("} else if (%s > 0x7fff) {%n", lengthVar);
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"string field %s " +
                "had invalid length \" + %s);%n", name, lengthVar);
            buffer.decrementIndent();
        }
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        if (type.isString()) {
            buffer.printf("%s_readable.readString(%s)%s",
                assignmentPrefix, lengthVar, assignmentSuffix);
        } else if (type.isBytes()) {
            if (zeroCopy) {
                buffer.printf("%s_readable.readByteBuffer(%s)%s", assignmentPrefix, lengthVar, assignmentSuffix);
            } else {
                buffer.printf("byte[] newBytes = new byte[%s];%n", lengthVar);
                buffer.printf("_readable.readArray(newBytes);%n");
                buffer.printf("%snewBytes%s", assignmentPrefix, assignmentSuffix);
            }
        } else if (type.isRecords()) {
            headerGenerator.addImport(MessageGenerator.RECORDS_READABLE_CLASS);
            buffer.printf("if (_readable instanceof RecordsReadable) {%n");
            buffer.incrementIndent();
            buffer.printf("%s((RecordsReadable) _readable).readRecords(%s)%s", assignmentPrefix, lengthVar, assignmentSuffix);
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"Cannot read records from reader of class: \" + _readable.getClass().getSimpleName());%n");
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            if (isStructArrayWithKeys) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                buffer.printf("%s newCollection = new %s(%s);%n",
                    collectionType(arrayType.elementType().toString()),
                        collectionType(arrayType.elementType().toString()), lengthVar);
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                buffer.printf("ArrayList<%s> newCollection = new ArrayList<%s>(%s);%n",
                    getBoxedJavaType(arrayType.elementType()),
                        getBoxedJavaType(arrayType.elementType()), lengthVar);
            }
            buffer.printf("for (int i = 0; i < %s; i++) {%n", lengthVar);
            buffer.incrementIndent();
            if (arrayType.elementType().isArray()) {
                throw new RuntimeException("Nested arrays are not supported.  " +
                    "Use an array of structures containing another array.");
            } else if (arrayType.elementType().isBytes() || arrayType.elementType().isString()) {
                generateVariableLengthReader(fieldFlexibleVersions,
                    name + " element",
                    arrayType.elementType(),
                    possibleVersions,
                    Versions.NONE,
                    "newCollection.add(",
                    String.format(");%n"),
                    false,
                    false);
            } else {
                buffer.printf("newCollection.add(%s);%n",
                    primitiveReadExpression(arrayType.elementType()));
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("%snewCollection%s", assignmentPrefix, assignmentSuffix);
        } else {
            throw new RuntimeException("Can't handle variable length type " + type);
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassFromStruct(String className, StructSpec struct,
                                         Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("@SuppressWarnings(\"unchecked\")%n");
        buffer.printf("@Override%n");
        buffer.printf("public void fromStruct(Struct struct, short _version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        if (!messageFlexibleVersions.intersect(struct.versions()).empty()) {
            buffer.printf("NavigableMap<Integer, Object> _taggedFields = null;%n");
        }
        buffer.printf("this._unknownTaggedFields = null;%n");
        VersionConditional.forVersions(messageFlexibleVersions, struct.versions()).
            ifMember(__ -> {
                headerGenerator.addImport(MessageGenerator.NAVIGABLE_MAP_CLASS);
                buffer.printf("_taggedFields = (NavigableMap<Integer, Object>) " +
                    "struct.get(\"%s\");%n", TAGGED_FIELDS_SECTION_NAME);
            }).
            generate(buffer);
        for (FieldSpec field : struct.fields()) {
            VersionConditional.forVersions(field.versions(), curVersions).
                alwaysEmitBlockScope(field.type().isArray()).
                ifNotMember(__ -> {
                    buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
                }).
                ifMember(presentVersions -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifNotMember(presentAndUntaggedVersions -> {
                            if (field.type().isArray()) {
                                buffer.printf("Object[] _nestedObjects = struct.getArray(\"%s\");%n",
                                    field.snakeCaseName());
                                generateArrayFromStruct(field, presentAndUntaggedVersions);
                            } else {
                                buffer.printf("this.%s = %s;%n",
                                    field.camelCaseName(),
                                    readFieldFromStruct(field.type(), field.snakeCaseName(), field.zeroCopy()));
                            }
                        }).
                        ifMember(presentAndTaggedVersions -> {
                            buffer.printf("if (_taggedFields.containsKey(%d)) {%n", field.tag().get());
                            buffer.incrementIndent();
                            if (field.type().isArray()) {
                                buffer.printf("Object[] _nestedObjects = " +
                                    "(Object[]) _taggedFields.remove(%d);%n", field.tag().get());
                                generateArrayFromStruct(field, presentAndTaggedVersions);
                            } else if (field.type().isBytes()) {
                                headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
                                buffer.printf("ByteBuffer _byteBuffer = (ByteBuffer) _taggedFields.remove(%d);%n",
                                    field.tag().get());

                                IsNullConditional.forName("_byteBuffer").
                                    nullableVersions(field.nullableVersions()).
                                    possibleVersions(field.versions()).
                                    ifNull(() -> {
                                        buffer.printf("this.%s = null;%n", field.camelCaseName());
                                    }).
                                    ifShouldNotBeNull(() -> {
                                        headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
                                        buffer.printf("this.%s = MessageUtil.byteBufferToArray(_byteBuffer);%n",
                                            field.camelCaseName());
                                    }).
                                    generate(buffer);
                            } else if (field.type().isStruct()) {
                                buffer.printf("this.%s = new %s((Struct) _taggedFields.remove(%d), _version);%n",
                                    field.camelCaseName(),
                                    getBoxedJavaType(field.type()),
                                    field.tag().get());
                            } else {
                                buffer.printf("this.%s = (%s) _taggedFields.remove(%d);%n",
                                    field.camelCaseName(),
                                    getBoxedJavaType(field.type()),
                                    field.tag().get());
                            }
                            buffer.decrementIndent();
                            buffer.printf("} else {%n");
                            buffer.incrementIndent();
                            buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
                            buffer.decrementIndent();
                            buffer.printf("}%n");
                        }).
                        generate(buffer);
                }).
                generate(buffer);
        }
        VersionConditional.forVersions(messageFlexibleVersions, struct.versions()).
            ifMember(__ -> {
                headerGenerator.addImport(MessageGenerator.NAVIGABLE_MAP_CLASS);
                buffer.printf("if (!_taggedFields.isEmpty()) {%n");
                buffer.incrementIndent();
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                buffer.printf("this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());%n");
                headerGenerator.addStaticImport(MessageGenerator.MAP_ENTRY_CLASS);
                buffer.printf("for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {%n");
                buffer.incrementIndent();
                headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_CLASS);
                buffer.printf("this._unknownTaggedFields.add((RawTaggedField) entry.getValue());%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
                buffer.decrementIndent();
                buffer.printf("}%n");
            }).
            generate(buffer);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassFromJson(String className, StructSpec struct,
                                       Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void fromJson(JsonNode _node, short _version) {%n");
        buffer.incrementIndent();
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
                    buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
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
                        input -> String.format("this.%s = %s", field.camelCaseName(), input)),
                        curVersions);
                }).ifNotMember(__ -> {
                    buffer.printf("throw new RuntimeException(\"%s: field \'%s\' is not " +
                        "supported in version \" + _version);%n",
                        className, field.camelCaseName());
                }).
                generate(buffer);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
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
                "UUID.fromString(%s.asText())", target.sourceVariable())));
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
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("new %s()", fieldConcreteJavaType(target.field()))));
            headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
            buffer.printf("for (JsonNode _element : %s) {%n", target.sourceVariable());
            buffer.incrementIndent();
            generateTargetFromJson(target.arrayElementTarget(
                input -> String.format("%s.add(%s)", target.field().camelCaseName(), input)),
                curVersions);
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else if (target.field().type().isStruct()) {
            buffer.printf("%s;%n", target.assignmentStatement(String.format("new %s(%s, _version)",
                target.field().type().toString(),
                target.sourceVariable())));
        } else {
            throw new RuntimeException("Unexpected type " + target.field().type());
        }
    }

    private void generateClassToJson(String className, StructSpec struct,
                                     Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.JSON_NODE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public JsonNode toJson(short _version) {%n");
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
                String.format("this.%s", field.camelCaseName()),
                field.camelCaseName(),
                input -> String.format("_node.set(\"%s\", %s)", field.camelCaseName(), input));
            VersionConditional cond = VersionConditional.forVersions(field.versions(), curVersions).
                ifMember(presentVersions -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifMember(presentAndTaggedVersions -> {
                            generateNonDefaultValueCheck(field, field.nullableVersions());
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
                    generateNonIgnorableFieldCheck(field);
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
            buffer.printf("%s;%n", target.assignmentStatement("new BinaryNode(new byte[]{})"));
        } else if (target.field().type().isArray()) {
            headerGenerator.addImport(MessageGenerator.ARRAY_NODE_CLASS);
            headerGenerator.addImport(MessageGenerator.JSON_NODE_FACTORY_CLASS);
            FieldType.ArrayType arrayType = (FieldType.ArrayType) target.field().type();
            FieldType elementType = arrayType.elementType();
            String arrayInstanceName = String.format("_%sArray", target.field().camelCaseName());
            buffer.printf("ArrayNode %s = new ArrayNode(JsonNodeFactory.instance);%n", arrayInstanceName);
            buffer.printf("for (%s _element : %s) {%n",
                getBoxedJavaType(elementType), target.sourceVariable());
            buffer.incrementIndent();
            generateTargetToJson(target.arrayElementTarget(
                input -> String.format("%s.add(%s)", arrayInstanceName, input)),
                versions);
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("%s;%n", target.assignmentStatement(arrayInstanceName));
        } else if (target.field().type().isStruct()) {
            buffer.printf("%s;%n", target.assignmentStatement(
                String.format("%s.toJson(_version)", target.sourceVariable())));
        } else {
            throw new RuntimeException("unknown type " + target.field().type());
        }
    }

    private void generateArrayFromStruct(FieldSpec field, Versions versions) {
        IsNullConditional.forName("_nestedObjects").
            possibleVersions(versions).
            nullableVersions(field.nullableVersions()).
            ifNull(() -> {
                buffer.printf("this.%s = null;%n", field.camelCaseName());
            }).
            ifShouldNotBeNull(() -> {
                FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                FieldType elementType = arrayType.elementType();
                buffer.printf("this.%s = new %s(_nestedObjects.length);%n",
                    field.camelCaseName(), fieldConcreteJavaType(field));
                buffer.printf("for (Object nestedObject : _nestedObjects) {%n");
                buffer.incrementIndent();
                if (elementType.isStruct()) {
                    headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
                    buffer.printf("this.%s.add(new %s((Struct) nestedObject, _version));%n",
                        field.camelCaseName(), elementType.toString());
                } else {
                    buffer.printf("this.%s.add((%s) nestedObject);%n",
                        field.camelCaseName(), getBoxedJavaType(elementType));
                }
                buffer.decrementIndent();
                buffer.printf("}%n");
            }).
            generate(buffer);
    }

    private String getBoxedJavaType(FieldType type) {
        if (type instanceof FieldType.BoolFieldType) {
            return "Boolean";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "Byte";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "Short";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "Integer";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "Long";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            return "UUID";
        } else if (type instanceof FieldType.Float64FieldType) {
            return "Double";
        } else if (type.isString()) {
            return "String";
        } else if (type.isStruct()) {
            return type.toString();
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private String readFieldFromStruct(FieldType type, String name, boolean zeroCopy) {
        if (type instanceof FieldType.BoolFieldType) {
            return String.format("struct.getBoolean(\"%s\")", name);
        } else if (type instanceof FieldType.Int8FieldType) {
            return String.format("struct.getByte(\"%s\")", name);
        } else if (type instanceof FieldType.Int16FieldType) {
            return String.format("struct.getShort(\"%s\")", name);
        } else if (type instanceof FieldType.Int32FieldType) {
            return String.format("struct.getInt(\"%s\")", name);
        } else if (type instanceof FieldType.Int64FieldType) {
            return String.format("struct.getLong(\"%s\")", name);
        } else if (type instanceof FieldType.UUIDFieldType) {
            return String.format("struct.getUUID(\"%s\")", name);
        } else if (type instanceof FieldType.Float64FieldType) {
            return String.format("struct.getDouble(\"%s\")", name);
        } else if (type.isString()) {
            return String.format("struct.getString(\"%s\")", name);
        } else if (type.isBytes()) {
            if (zeroCopy) {
                return String.format("struct.getBytes(\"%s\")", name);
            } else {
                return String.format("struct.getByteArray(\"%s\")", name);
            }
        } else if (type.isRecords()) {
            return String.format("struct.getRecords(\"%s\")", name);
        } else if (type.isStruct()) {
            return String.format("new %s((Struct) struct.get(\"%s\"), _version)",
                    type.toString(), name);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateNonIgnorableFieldCheck(FieldSpec field) {
        generateNonDefaultValueCheck(field, field.nullableVersions());
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(" +
                        "\"Attempted to write a non-default %s at version \" + _version);%n",
                field.camelCaseName());
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassWriter(String className, StructSpec struct,
            Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.WRITABLE_CLASS);
        headerGenerator.addImport(MessageGenerator.OBJECT_SERIALIZATION_CACHE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(struct.versions(), parentVersions).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        buffer.printf("int _numTaggedFields = 0;%n");
        Versions curVersions = parentVersions.intersect(struct.versions());
        TreeMap<Integer, FieldSpec> taggedFields = new TreeMap<>();
        for (FieldSpec field : struct.fields()) {
            VersionConditional cond = VersionConditional.forVersions(field.versions(), curVersions).
                ifMember(presentVersions -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifNotMember(presentAndUntaggedVersions -> {
                            if (field.type().isVariableLength() && !field.type().isStruct()) {
                                ClauseGenerator callGenerateVariableLengthWriter = versions -> {
                                    generateVariableLengthWriter(fieldFlexibleVersions(field),
                                        field.camelCaseName(),
                                        field.type(),
                                        versions,
                                        field.nullableVersions(),
                                        field.zeroCopy());
                                };
                                // For arrays where the field type needs to be serialized differently in flexible
                                // versions, lift the flexible version check outside of the array.
                                // This may mean generating two separate 'for' loops-- one for flexible
                                // versions, and one for regular versions.
                                if (field.type().isArray() &&
                                    ((FieldType.ArrayType) field.type()).elementType().
                                        serializationIsDifferentInFlexibleVersions()) {
                                    VersionConditional.forVersions(fieldFlexibleVersions(field),
                                            presentAndUntaggedVersions).
                                        ifMember(callGenerateVariableLengthWriter).
                                        ifNotMember(callGenerateVariableLengthWriter).
                                        generate(buffer);
                                } else {
                                    callGenerateVariableLengthWriter.generate(presentAndUntaggedVersions);
                                }
                            } else {
                                buffer.printf("%s;%n",
                                    primitiveWriteExpression(field.type(), field.camelCaseName()));
                            }
                        }).
                        ifMember(__ -> {
                            generateNonDefaultValueCheck(field, field.nullableVersions());
                            buffer.incrementIndent();
                            buffer.printf("_numTaggedFields++;%n");
                            buffer.decrementIndent();
                            buffer.printf("}%n");
                            if (taggedFields.put(field.tag().get(), field) != null) {
                                throw new RuntimeException("Field " + field.name() + " has tag " +
                                    field.tag() + ", but another field already used that tag.");
                            }
                        }).
                        generate(buffer);
                });
            if (!field.ignorable()) {
                cond.ifNotMember(__ -> {
                    generateNonIgnorableFieldCheck(field);
                });
            }
            cond.generate(buffer);
        }
        headerGenerator.addImport(MessageGenerator.RAW_TAGGED_FIELD_WRITER_CLASS);
        buffer.printf("RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);%n");
        buffer.printf("_numTaggedFields += _rawWriter.numFields();%n");
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).
            ifNotMember(__ -> {
                generateCheckForUnsupportedNumTaggedFields("_numTaggedFields > 0");
            }).
            ifMember(__ -> {
                buffer.printf("_writable.writeUnsignedVarint(_numTaggedFields);%n");
                int prevTag = -1;
                for (FieldSpec field : taggedFields.values()) {
                    if (prevTag + 1 != field.tag().get()) {
                        buffer.printf("_rawWriter.writeRawTags(_writable, %d);%n", field.tag().get());
                    }
                    VersionConditional.
                        forVersions(field.versions(), field.taggedVersions().intersect(field.versions())).
                        allowMembershipCheckAlwaysFalse(false).
                        ifMember(presentAndTaggedVersions -> {
                            IsNullConditional cond = IsNullConditional.forName(field.camelCaseName()).
                                nullableVersions(field.nullableVersions()).
                                possibleVersions(presentAndTaggedVersions).
                                alwaysEmitBlockScope(true).
                                ifShouldNotBeNull(() -> {
                                    if (!field.defaultString().equals("null")) {
                                        generateNonDefaultValueCheck(field, Versions.NONE);
                                        buffer.incrementIndent();
                                    }
                                    buffer.printf("_writable.writeUnsignedVarint(%d);%n", field.tag().get());
                                    if (field.type().isString()) {
                                        buffer.printf("byte[] _stringBytes = _cache.getSerializedValue(this.%s);%n",
                                            field.camelCaseName());
                                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                                        buffer.printf("_writable.writeUnsignedVarint(_stringBytes.length + " +
                                            "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));%n");
                                        buffer.printf("_writable.writeUnsignedVarint(_stringBytes.length + 1);%n");
                                        buffer.printf("_writable.writeByteArray(_stringBytes);%n");
                                    } else if (field.type().isBytes()) {
                                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                                        buffer.printf("_writable.writeUnsignedVarint(this.%s.length + " +
                                                "ByteUtils.sizeOfUnsignedVarint(this.%s.length + 1));%n",
                                            field.camelCaseName(), field.camelCaseName());
                                        buffer.printf("_writable.writeUnsignedVarint(this.%s.length + 1);%n",
                                            field.camelCaseName());
                                        buffer.printf("_writable.writeByteArray(this.%s);%n",
                                            field.camelCaseName());
                                    } else if (field.type().isArray()) {
                                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                                        buffer.printf("_writable.writeUnsignedVarint(_cache.getArraySizeInBytes(this.%s));%n",
                                            field.camelCaseName());
                                        generateVariableLengthWriter(fieldFlexibleVersions(field),
                                            field.camelCaseName(),
                                            field.type(),
                                            presentAndTaggedVersions,
                                            Versions.NONE,
                                            field.zeroCopy());
                                    } else if (field.type().isStruct()) {
                                        buffer.printf("_writable.writeUnsignedVarint(this.%s.size(_cache, _version));%n",
                                                field.camelCaseName());
                                        buffer.printf("%s;%n",
                                                primitiveWriteExpression(field.type(), field.camelCaseName()));
                                    } else {
                                        buffer.printf("_writable.writeUnsignedVarint(%d);%n",
                                            field.type().fixedLength().get());
                                        buffer.printf("%s;%n",
                                            primitiveWriteExpression(field.type(), field.camelCaseName()));
                                    }
                                    if (!field.defaultString().equals("null")) {
                                        buffer.decrementIndent();
                                        buffer.printf("}%n");
                                    }
                                });
                            if (!field.defaultString().equals("null")) {
                                cond.ifNull(() -> {
                                    buffer.printf("_writable.writeUnsignedVarint(%d);%n", field.tag().get());
                                    buffer.printf("_writable.writeUnsignedVarint(1);%n");
                                    buffer.printf("_writable.writeUnsignedVarint(0);%n");
                                });
                            }
                            cond.generate(buffer);
                        }).
                        generate(buffer);
                    prevTag = field.tag().get();
                }
                if (prevTag < Integer.MAX_VALUE) {
                    buffer.printf("_rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);%n");
                }
            }).
            generate(buffer);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateCheckForUnsupportedNumTaggedFields(String conditional) {
        buffer.printf("if (%s) {%n", conditional);
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(\"Tagged fields were set, " +
            "but version \" + _version + \" of this message does not support them.\");%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String primitiveWriteExpression(FieldType type, String name) {
        if (type instanceof FieldType.BoolFieldType) {
            return String.format("_writable.writeByte(%s ? (byte) 1 : (byte) 0)", name);
        } else if (type instanceof FieldType.Int8FieldType) {
            return String.format("_writable.writeByte(%s)", name);
        } else if (type instanceof FieldType.Int16FieldType) {
            return String.format("_writable.writeShort(%s)", name);
        } else if (type instanceof FieldType.Int32FieldType) {
            return String.format("_writable.writeInt(%s)", name);
        } else if (type instanceof FieldType.Int64FieldType) {
            return String.format("_writable.writeLong(%s)", name);
        } else if (type instanceof FieldType.UUIDFieldType) {
            return String.format("_writable.writeUUID(%s)", name);
        } else if (type instanceof FieldType.Float64FieldType) {
            return String.format("_writable.writeDouble(%s)", name);
        } else if (type instanceof FieldType.StructType) {
            return String.format("%s.write(_writable, _cache, _version)", name);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateVariableLengthWriter(Versions fieldFlexibleVersions,
                                              String name,
                                              FieldType type,
                                              Versions possibleVersions,
                                              Versions nullableVersions,
                                              boolean zeroCopy) {
        IsNullConditional.forName(name).
            possibleVersions(possibleVersions).
            nullableVersions(nullableVersions).
            alwaysEmitBlockScope(type.isString()).
            ifNull(() -> {
                VersionConditional.forVersions(nullableVersions, possibleVersions).
                    ifMember(presentVersions -> {
                        VersionConditional.forVersions(fieldFlexibleVersions, presentVersions).
                            ifMember(___ -> {
                                buffer.printf("_writable.writeUnsignedVarint(0);%n");
                            }).
                            ifNotMember(___ -> {
                                if (type.isString()) {
                                    buffer.printf("_writable.writeShort((short) -1);%n");
                                } else {
                                    buffer.printf("_writable.writeInt(-1);%n");
                                }
                            }).
                            generate(buffer);
                    }).
                    ifNotMember(__ -> {
                        buffer.printf("throw new NullPointerException();%n");
                    }).
                    generate(buffer);
            }).
            ifShouldNotBeNull(() -> {
                final String lengthExpression;
                if (type.isString()) {
                    buffer.printf("byte[] _stringBytes = _cache.getSerializedValue(%s);%n",
                        name);
                    lengthExpression = "_stringBytes.length";
                } else if (type.isBytes()) {
                    if (zeroCopy) {
                        lengthExpression = String.format("%s.remaining()", name);
                    } else {
                        lengthExpression = String.format("%s.length", name);
                    }
                } else if (type.isRecords()) {
                    lengthExpression = String.format("%s.sizeInBytes()", name);
                } else if (type.isArray()) {
                    lengthExpression = String.format("%s.size()", name);
                } else {
                    throw new RuntimeException("Unhandled type " + type);
                }
                // Check whether we're dealing with a flexible version or not.  In a flexible
                // version, the length is serialized differently.
                //
                // Note: for arrays, each branch of the if contains the loop for writing out
                // the elements.  This allows us to lift the version check out of the loop.
                // This is helpful for things like arrays of strings, where each element
                // will be serialized differently based on whether the version is flexible.
                VersionConditional.forVersions(fieldFlexibleVersions, possibleVersions).
                    ifMember(ifMemberVersions -> {
                        buffer.printf("_writable.writeUnsignedVarint(%s + 1);%n", lengthExpression);
                    }).
                    ifNotMember(ifNotMemberVersions -> {
                        if (type.isString()) {
                            buffer.printf("_writable.writeShort((short) %s);%n", lengthExpression);
                        } else {
                            buffer.printf("_writable.writeInt(%s);%n", lengthExpression);
                        }
                    }).
                    generate(buffer);
                if (type.isString()) {
                    buffer.printf("_writable.writeByteArray(_stringBytes);%n");
                } else if (type.isBytes()) {
                    if (zeroCopy) {
                        buffer.printf("_writable.writeByteBuffer(%s);%n", name);
                    } else {
                        buffer.printf("_writable.writeByteArray(%s);%n", name);
                    }
                } else if (type.isRecords()) {
                    headerGenerator.addImport(MessageGenerator.RECORDS_WRITABLE_CLASS);
                    buffer.printf("if (_writable instanceof RecordsWritable) {%n");
                    buffer.incrementIndent();
                    buffer.printf("((RecordsWritable) _writable).writeRecords(%s);%n", name);
                    buffer.decrementIndent();
                    buffer.printf("} else {%n");
                    buffer.incrementIndent();
                    buffer.printf("throw new RuntimeException(\"Cannot write records to writer of class: \" + _writable.getClass().getSimpleName());%n");
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                } else if (type.isArray()) {
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
                    FieldType elementType = arrayType.elementType();
                    String elementName = String.format("%sElement", name);
                    buffer.printf("for (%s %s : %s) {%n",
                        getBoxedJavaType(elementType),
                        elementName,
                        name);
                    buffer.incrementIndent();
                    if (elementType.isArray()) {
                        throw new RuntimeException("Nested arrays are not supported.  " +
                            "Use an array of structures containing another array.");
                    } else if (elementType.isBytes() || elementType.isString()) {
                        generateVariableLengthWriter(fieldFlexibleVersions,
                            elementName,
                            elementType,
                            possibleVersions,
                            Versions.NONE,
                            false);
                    } else {
                        buffer.printf("%s;%n", primitiveWriteExpression(elementType, elementName));
                    }
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                }
            }).
            generate(buffer);
    }

    private void generateNonDefaultValueCheck(FieldSpec field, Versions nullableVersions) {
        if (field.type().isArray()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n",
                    field.camelCaseName());
            } else if (nullableVersions.empty()) {
                buffer.printf("if (!%s.isEmpty()) {%n",
                    field.camelCaseName());
            } else {
                buffer.printf("if (%s == null || !%s.isEmpty()) {%n",
                    field.camelCaseName(), field.camelCaseName());
            }
        } else if (field.type().isBytes()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n", field.camelCaseName());
            } else if (nullableVersions.empty()) {
                if (field.zeroCopy()) {
                    buffer.printf("if (%s.hasRemaining()) {%n", field.camelCaseName());
                } else {
                    buffer.printf("if (%s.length != 0) {%n", field.camelCaseName());
                }
            } else {
                if (field.zeroCopy()) {
                    buffer.printf("if (%s == null || %s.remaining() > 0) {%n",
                        field.camelCaseName(), field.camelCaseName());
                } else {
                    buffer.printf("if (%s == null || %s.length != 0) {%n",
                        field.camelCaseName(), field.camelCaseName());
                }
            }
        } else if (field.type().isString() || field.type().isStruct()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n", field.camelCaseName());
            } else if (nullableVersions.empty()) {
                buffer.printf("if (!%s.equals(%s)) {%n",
                    field.camelCaseName(), fieldDefault(field));
            } else {
                buffer.printf("if (%s == null || !%s.equals(%s)) {%n",
                    field.camelCaseName(), field.camelCaseName(), fieldDefault(field));
            }
        } else if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("if (%s%s) {%n",
                fieldDefault(field).equals("true") ? "!" : "",
                field.camelCaseName());
        } else {
            buffer.printf("if (%s != %s) {%n", field.camelCaseName(), fieldDefault(field));
        }
    }

    private void generateClassToStruct(String className, StructSpec struct,
                                       Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public Struct toStruct(short _version) {%n");
        buffer.incrementIndent();
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        headerGenerator.addImport(MessageGenerator.TREE_MAP_CLASS);
        buffer.printf("TreeMap<Integer, Object> _taggedFields = null;%n");
        VersionConditional.forVersions(messageFlexibleVersions, struct.versions()).
            ifMember(__ -> {
                buffer.printf("_taggedFields = new TreeMap<>();%n");
            }).
            generate(buffer);
        buffer.printf("Struct struct = new Struct(SCHEMAS[_version]);%n");
        for (FieldSpec field : struct.fields()) {
            VersionConditional cond = VersionConditional.forVersions(field.versions(), curVersions).
                alwaysEmitBlockScope(field.type().isArray()).
                ifMember(presentVersions -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifNotMember(presentAndUntaggedVersions -> {
                            generateFieldToStruct(field, presentAndUntaggedVersions);
                        }).
                        ifMember(presentAndTaggedVersions -> {
                            generateNonDefaultValueCheck(field, field.nullableVersions());
                            buffer.incrementIndent();
                            generateTaggedFieldToMap(field, presentAndTaggedVersions);
                            buffer.decrementIndent();
                            buffer.printf("}%n");
                        }).
                        generate(buffer);
                });
            if (!field.ignorable()) {
                cond.ifNotMember(__ -> {
                    generateNonIgnorableFieldCheck(field);
                });
            }
            cond.generate(buffer);
        }
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).
            ifMember(__ -> {
                buffer.printf("struct.set(\"%s\", _taggedFields);%n", TAGGED_FIELDS_SECTION_NAME);
            }).
            generate(buffer);
        buffer.printf("return struct;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldToStruct(FieldSpec field, Versions versions) {
        if ((!field.type().canBeNullable()) &&
            (!field.nullableVersions().empty())) {
            throw new RuntimeException("Fields of type " + field.type() +
                " cannot be nullable.");
        }
        if ((field.type() instanceof FieldType.BoolFieldType) ||
                (field.type() instanceof FieldType.Int8FieldType) ||
                (field.type() instanceof FieldType.Int16FieldType) ||
                (field.type() instanceof FieldType.Int32FieldType) ||
                (field.type() instanceof FieldType.Int64FieldType) ||
                (field.type() instanceof FieldType.UUIDFieldType) ||
                (field.type() instanceof FieldType.Float64FieldType) ||
                (field.type() instanceof FieldType.StringFieldType)) {
            buffer.printf("struct.set(\"%s\", this.%s);%n",
                field.snakeCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            if (field.zeroCopy()) {
                buffer.printf("struct.set(\"%s\", this.%s);%n",
                    field.snakeCaseName(), field.camelCaseName());
            } else {
                buffer.printf("struct.setByteArray(\"%s\", this.%s);%n",
                    field.snakeCaseName(), field.camelCaseName());
            }
        } else if (field.type().isRecords()) {
            buffer.printf("struct.set(\"%s\", this.%s);%n",
                    field.snakeCaseName(), field.camelCaseName());
        } else if (field.type().isArray()) {
            IsNullConditional.forField(field).
                possibleVersions(versions).
                ifNull(() -> {
                    buffer.printf("struct.set(\"%s\", null);%n", field.snakeCaseName());
                }).
                ifShouldNotBeNull(() -> {
                    generateFieldToObjectArray(field);
                    buffer.printf("struct.set(\"%s\", (Object[]) _nestedObjects);%n",
                        field.snakeCaseName());
                }).generate(buffer);
        } else if (field.type().isStruct()) {
            buffer.printf("struct.set(\"%s\", this.%s.toStruct(_version));%n",
                    field.snakeCaseName(), field.camelCaseName());
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateTaggedFieldToMap(FieldSpec field, Versions versions) {
        if ((!field.type().canBeNullable()) &&
            (!field.nullableVersions().empty())) {
            throw new RuntimeException("Fields of type " + field.type() +
                " cannot be nullable.");
        }
        if ((field.type() instanceof FieldType.BoolFieldType) ||
            (field.type() instanceof FieldType.Int8FieldType) ||
            (field.type() instanceof FieldType.Int16FieldType) ||
            (field.type() instanceof FieldType.Int32FieldType) ||
            (field.type() instanceof FieldType.Int64FieldType) ||
            (field.type() instanceof FieldType.UUIDFieldType) ||
            (field.type() instanceof FieldType.Float64FieldType) ||
            (field.type() instanceof FieldType.StringFieldType)) {
            buffer.printf("_taggedFields.put(%d, %s);%n",
                field.tag().get(), field.camelCaseName());
        } else if (field.type().isStruct()) {
            buffer.printf("_taggedFields.put(%d, %s.toStruct(_version));%n",
                field.tag().get(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
            if (field.taggedVersions().intersect(field.nullableVersions()).empty()) {
                buffer.printf("_taggedFields.put(%d, ByteBuffer.wrap(%s));%n",
                    field.tag().get(), field.camelCaseName());
            } else {
                buffer.printf("_taggedFields.put(%d, (%s == null) ? null : ByteBuffer.wrap(%s));%n",
                    field.tag().get(), field.camelCaseName(), field.camelCaseName());
            }
        } else if (field.type().isArray()) {
            IsNullConditional.forField(field).
                possibleVersions(versions).
                ifNull(() -> {
                    buffer.printf("_taggedFields.put(%d, null);%n", field.tag().get());
                }).
                ifShouldNotBeNull(() -> {
                    generateFieldToObjectArray(field);
                    buffer.printf("_taggedFields.put(%d, _nestedObjects);%n", field.tag().get());
                }).generate(buffer);
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateFieldToObjectArray(FieldSpec field) {
        FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
        FieldType elementType = arrayType.elementType();
        String boxdElementType = elementType.isStruct() ? "Struct" : getBoxedJavaType(elementType);
        buffer.printf("%s[] _nestedObjects = new %s[%s.size()];%n",
            boxdElementType, boxdElementType, field.camelCaseName());
        buffer.printf("int i = 0;%n");
        buffer.printf("for (%s element : this.%s) {%n",
            getBoxedJavaType(arrayType.elementType()), field.camelCaseName());
        buffer.incrementIndent();
        if (elementType.isStruct()) {
            buffer.printf("_nestedObjects[i++] = element.toStruct(_version);%n");
        } else {
            buffer.printf("_nestedObjects[i++] = element;%n");
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassSize(String className, StructSpec struct,
                                   Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.OBJECT_SERIALIZATION_CACHE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public int size(ObjectSerializationCache _cache, short _version) {%n");
        buffer.incrementIndent();
        buffer.printf("int _size = 0, _numTaggedFields = 0;%n");
        VersionConditional.forVersions(parentVersions, struct.versions()).
            allowMembershipCheckAlwaysFalse(false).
            ifNotMember(__ -> {
                headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
                buffer.printf("throw new UnsupportedVersionException(\"Can't size " +
                    "version \" + _version + \" of %s\");%n", className);
            }).
            generate(buffer);
        Versions curVersions = parentVersions.intersect(struct.versions());
        for (FieldSpec field : struct.fields()) {
            VersionConditional.forVersions(field.versions(), curVersions).
                ifMember(presentVersions -> {
                    VersionConditional.forVersions(field.taggedVersions(), presentVersions).
                        ifMember(presentAndTaggedVersions -> {
                            generateFieldSize(field, presentAndTaggedVersions, true);
                        }).
                        ifNotMember(presentAndUntaggedVersions -> {
                            generateFieldSize(field, presentAndUntaggedVersions, false);
                        }).
                        generate(buffer);
                }).generate(buffer);
        }
        buffer.printf("if (_unknownTaggedFields != null) {%n");
        buffer.incrementIndent();
        buffer.printf("_numTaggedFields += _unknownTaggedFields.size();%n");
        buffer.printf("for (RawTaggedField _field : _unknownTaggedFields) {%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
        buffer.printf("_size += ByteUtils.sizeOfUnsignedVarint(_field.tag());%n");
        buffer.printf("_size += ByteUtils.sizeOfUnsignedVarint(_field.size());%n");
        buffer.printf("_size += _field.size();%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        VersionConditional.forVersions(messageFlexibleVersions, curVersions).
            ifNotMember(__ -> {
                generateCheckForUnsupportedNumTaggedFields("_numTaggedFields > 0");
            }).
            ifMember(__ -> {
                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                buffer.printf("_size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);%n");
            }).
            generate(buffer);
        buffer.printf("return _size;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    /**
     * Generate the size calculator for a variable-length array element.
     * Array elements cannot be null.
     */
    private void generateVariableLengthArrayElementSize(Versions flexibleVersions,
                                                        String fieldName,
                                                        FieldType type,
                                                        Versions versions) {
        if (type instanceof FieldType.StringFieldType) {
            generateStringToBytes(fieldName);
            VersionConditional.forVersions(flexibleVersions, versions).
                ifNotMember(__ -> {
                    buffer.printf("_arraySize += _stringBytes.length + 2;%n");
                }).
                ifMember(__ -> {
                    headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                    buffer.printf("_arraySize += _stringBytes.length + " +
                        "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);%n");
                }).
                generate(buffer);
        } else if (type instanceof FieldType.BytesFieldType) {
            VersionConditional.forVersions(flexibleVersions, versions).
                ifNotMember(__ -> {
                    buffer.printf("_arraySize += %s.length + 4;%n", fieldName);
                }).
                ifMember(__ -> {
                    headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                    buffer.printf("_arraySize += %s.length + " +
                        "ByteUtils.sizeOfUnsignedVarint(%s.length + 1);%n",
                        fieldName, fieldName);
                }).
                generate(buffer);
        } else if (type instanceof FieldType.StructType) {
            buffer.printf("_arraySize += %s.size(_cache, _version);%n", fieldName);
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
    }

    private void generateFieldSize(FieldSpec field,
                                   Versions possibleVersions,
                                   boolean tagged) {
        if (field.type().fixedLength().isPresent()) {
            generateFixedLengthFieldSize(field, tagged);
        } else {
            generateVariableLengthFieldSize(field, possibleVersions, tagged);
        }
    }

    private void generateFixedLengthFieldSize(FieldSpec field,
                                              boolean tagged) {
        if (tagged) {
            // Check to see that the field is not set to the default value.
            // If it is, then we don't need to serialize it.
            generateNonDefaultValueCheck(field, field.nullableVersions());
            buffer.incrementIndent();
            buffer.printf("_numTaggedFields++;%n");
            buffer.printf("_size += %d;%n",
                MessageGenerator.sizeOfUnsignedVarint(field.tag().get()));
            // Account for the tagged field prefix length.
            buffer.printf("_size += %d;%n",
                MessageGenerator.sizeOfUnsignedVarint(field.type().fixedLength().get()));
            buffer.printf("_size += %d;%n", field.type().fixedLength().get());
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else {
            buffer.printf("_size += %d;%n", field.type().fixedLength().get());
        }
    }

    private void generateVariableLengthFieldSize(FieldSpec field,
                                                 Versions possibleVersions,
                                                 boolean tagged) {
        IsNullConditional.forField(field).
            alwaysEmitBlockScope(true).
            possibleVersions(possibleVersions).
            nullableVersions(field.nullableVersions()).
            ifNull(() -> {
                if (!tagged || !field.defaultString().equals("null")) {
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            if (tagged) {
                                buffer.printf("_numTaggedFields++;%n");
                                buffer.printf("_size += %d;%n",
                                    MessageGenerator.sizeOfUnsignedVarint(field.tag().get()));
                                buffer.printf("_size += %d;%n", MessageGenerator.sizeOfUnsignedVarint(
                                    MessageGenerator.sizeOfUnsignedVarint(0)));
                            }
                            buffer.printf("_size += %d;%n", MessageGenerator.sizeOfUnsignedVarint(0));
                        }).
                        ifNotMember(__ -> {
                            if (tagged) {
                                throw new RuntimeException("Tagged field " + field.name() +
                                    " should not be present in non-flexible versions.");
                            }
                            if (field.type().isString()) {
                                buffer.printf("_size += 2;%n");
                            } else {
                                buffer.printf("_size += 4;%n");
                            }
                        }).
                        generate(buffer);
                }
            }).
            ifShouldNotBeNull(() -> {
                if (tagged) {
                    if (!field.defaultString().equals("null")) {
                        generateNonDefaultValueCheck(field, Versions.NONE);
                        buffer.incrementIndent();
                    }
                    buffer.printf("_numTaggedFields++;%n");
                    buffer.printf("_size += %d;%n",
                        MessageGenerator.sizeOfUnsignedVarint(field.tag().get()));
                }
                if (field.type().isString()) {
                    generateStringToBytes(field.camelCaseName());
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            if (tagged) {
                                buffer.printf("int _stringPrefixSize = " +
                                    "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);%n");
                                buffer.printf("_size += _stringBytes.length + _stringPrefixSize + " +
                                    "ByteUtils.sizeOfUnsignedVarint(_stringPrefixSize);%n");
                            } else {
                                buffer.printf("_size += _stringBytes.length + " +
                                    "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);%n");
                            }
                        }).
                        ifNotMember(__ -> {
                            if (tagged) {
                                throw new RuntimeException("Tagged field " + field.name() +
                                    " should not be present in non-flexible versions.");
                            }
                            buffer.printf("_size += _stringBytes.length + 2;%n");
                        }).
                        generate(buffer);
                } else if (field.type().isArray()) {
                    buffer.printf("int _arraySize = 0;%n");
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            buffer.printf("_arraySize += ByteUtils.sizeOfUnsignedVarint(%s.size() + 1);%n",
                                field.camelCaseName());
                        }).
                        ifNotMember(__ -> {
                            buffer.printf("_arraySize += 4;%n");
                        }).
                        generate(buffer);
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                    FieldType elementType = arrayType.elementType();
                    if (elementType.fixedLength().isPresent()) {
                        buffer.printf("_arraySize += %s.size() * %d;%n",
                            field.camelCaseName(),
                            elementType.fixedLength().get());
                    } else if (elementType instanceof FieldType.ArrayType) {
                        throw new RuntimeException("Arrays of arrays are not supported " +
                            "(use a struct).");
                    } else {
                        buffer.printf("for (%s %sElement : %s) {%n",
                            getBoxedJavaType(elementType), field.camelCaseName(), field.camelCaseName());
                        buffer.incrementIndent();
                        generateVariableLengthArrayElementSize(fieldFlexibleVersions(field),
                            String.format("%sElement", field.camelCaseName()),
                            elementType,
                            possibleVersions);
                        buffer.decrementIndent();
                        buffer.printf("}%n");
                    }
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                        buffer.printf("_cache.setArraySizeInBytes(%s, _arraySize);%n",
                            field.camelCaseName());
                        buffer.printf("_size += _arraySize + ByteUtils.sizeOfUnsignedVarint(_arraySize);%n");
                    } else {
                        buffer.printf("_size += _arraySize;%n");
                    }
                } else if (field.type().isBytes()) {
                    if (field.zeroCopy()) {
                        buffer.printf("int _bytesSize = %s.remaining();%n", field.camelCaseName());
                    } else {
                        buffer.printf("int _bytesSize = %s.length;%n", field.camelCaseName());
                    }
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            if (field.zeroCopy()) {
                                buffer.printf("_bytesSize += " +
                                        "ByteUtils.sizeOfUnsignedVarint(%s.remaining() + 1);%n", field.camelCaseName());
                            } else {
                                buffer.printf("_bytesSize += ByteUtils.sizeOfUnsignedVarint(%s.length + 1);%n",
                                    field.camelCaseName());
                            }
                        }).
                        ifNotMember(__ -> {
                            buffer.printf("_bytesSize += 4;%n");
                        }).
                        generate(buffer);
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                        buffer.printf("_size += _bytesSize + ByteUtils.sizeOfUnsignedVarint(_bytesSize);%n");
                    } else {
                        buffer.printf("_size += _bytesSize;%n");
                    }
                } else if (field.type().isRecords()) {
                    buffer.printf("_size += %s.sizeInBytes() + 4;%n", field.camelCaseName());
                } else if (field.type().isStruct()) {
                    buffer.printf("int size = this.%s.size(_cache, _version);%n", field.camelCaseName());
                    if (tagged) {
                        buffer.printf("_size += ByteUtils.sizeOfUnsignedVarint(size);%n");
                    }
                    buffer.printf("_size += size;%n");
                } else {
                    throw new RuntimeException("unhandled type " + field.type());
                }
                if (tagged && !field.defaultString().equals("null")) {
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                }
            }).
            generate(buffer);
    }

    private void generateStringToBytes(String name) {
        headerGenerator.addImport(MessageGenerator.STANDARD_CHARSETS);
        buffer.printf("byte[] _stringBytes = %s.getBytes(StandardCharsets.UTF_8);%n", name);
        buffer.printf("if (_stringBytes.length > 0x7fff) {%n");
        buffer.incrementIndent();
        buffer.printf("throw new RuntimeException(\"'%s' field is too long to " +
            "be serialized\");%n", name);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("_cache.cacheSerializedValue(%s, _stringBytes);%n", name);
    }

    private void generateClassEquals(String className, StructSpec struct,
                                     boolean elementKeysAreEqual) {
        buffer.printf("@Override%n");
        buffer.printf("public boolean %s(Object obj) {%n",
            elementKeysAreEqual ? "elementKeysAreEqual" : "equals");
        buffer.incrementIndent();
        buffer.printf("if (!(obj instanceof %s)) return false;%n", className);
        buffer.printf("%s other = (%s) obj;%n", className, className);
        if (!struct.fields().isEmpty()) {
            for (FieldSpec field : struct.fields()) {
                if (!elementKeysAreEqual || field.mapKey()) {
                    generateFieldEquals(field);
                }
            }
        }
        if (elementKeysAreEqual) {
            buffer.printf("return true;%n");
        } else {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, " +
                "other._unknownTaggedFields);%n");
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldEquals(FieldSpec field) {
        if (field.type() instanceof FieldType.UUIDFieldType) {
            buffer.printf("if (!this.%s.equals(other.%s)) return false;%n",
                field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isString() || field.type().isArray() || field.type().isStruct()) {
            buffer.printf("if (this.%s == null) {%n", field.camelCaseName());
            buffer.incrementIndent();
            buffer.printf("if (other.%s != null) return false;%n", field.camelCaseName());
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
            buffer.printf("if (!this.%s.equals(other.%s)) return false;%n",
                field.camelCaseName(), field.camelCaseName());
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else if (field.type().isBytes()) {
            if (field.zeroCopy()) {
                headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS);
                buffer.printf("if (!Objects.equals(this.%s, other.%s)) return false;%n",
                    field.camelCaseName(), field.camelCaseName());
            } else {
                // Arrays#equals handles nulls.
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
                buffer.printf("if (!Arrays.equals(this.%s, other.%s)) return false;%n",
                    field.camelCaseName(), field.camelCaseName());
            }
        } else if (field.type().isRecords()) {
            headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS);
            buffer.printf("if (!Objects.equals(this.%s, other.%s)) return false;%n",
                    field.camelCaseName(), field.camelCaseName());
        } else {
            buffer.printf("if (%s != other.%s) return false;%n",
                field.camelCaseName(), field.camelCaseName());
        }
    }

    private void generateClassHashCode(StructSpec struct, boolean onlyMapKeys) {
        buffer.printf("@Override%n");
        buffer.printf("public int hashCode() {%n");
        buffer.incrementIndent();
        buffer.printf("int hashCode = 0;%n");
        for (FieldSpec field : struct.fields()) {
            if ((!onlyMapKeys) || field.mapKey()) {
                generateFieldHashCode(field);
            }
        }
        buffer.printf("return hashCode;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldHashCode(FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("hashCode = 31 * hashCode + (%s ? 1231 : 1237);%n",
                field.camelCaseName());
        } else if ((field.type() instanceof FieldType.Int8FieldType) ||
                    (field.type() instanceof FieldType.Int16FieldType) ||
                    (field.type() instanceof FieldType.Int32FieldType)) {
            buffer.printf("hashCode = 31 * hashCode + %s;%n",
                field.camelCaseName());
        } else if (field.type() instanceof FieldType.Int64FieldType) {
            buffer.printf("hashCode = 31 * hashCode + ((int) (%s >> 32) ^ (int) %s);%n",
                field.camelCaseName(), field.camelCaseName());
        } else if (field.type() instanceof FieldType.UUIDFieldType) {
            buffer.printf("hashCode = 31 * hashCode + %s.hashCode();%n",
                field.camelCaseName());
        } else if (field.type() instanceof FieldType.Float64FieldType) {
            buffer.printf("hashCode = 31 * hashCode + Double.hashCode(%s);%n",
                field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            if (field.zeroCopy()) {
                headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS);
                buffer.printf("hashCode = 31 * hashCode + Objects.hashCode(%s);%n",
                        field.camelCaseName());
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
                buffer.printf("hashCode = 31 * hashCode + Arrays.hashCode(%s);%n",
                    field.camelCaseName());
            }
        } else if (field.type().isRecords()) {
            headerGenerator.addImport(MessageGenerator.OBJECTS_CLASS);
            buffer.printf("hashCode = 31 * hashCode + Objects.hashCode(%s);%n",
                    field.camelCaseName());
        } else if (field.type().isStruct()
                   || field.type().isArray()
                   || field.type().isString()) {
            buffer.printf("hashCode = 31 * hashCode + (%s == null ? 0 : %s.hashCode());%n",
                          field.camelCaseName(), field.camelCaseName());
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateClassDuplicate(String className, StructSpec struct) {
        buffer.printf("@Override%n");
        buffer.printf("public %s duplicate() {%n", className);
        buffer.incrementIndent();
        buffer.printf("%s _duplicate = new %s();%n", className, className);
        for (FieldSpec field : struct.fields()) {
            generateFieldDuplicate(new Target(field,
                field.camelCaseName(),
                field.camelCaseName(),
                input -> String.format("_duplicate.%s = %s", field.camelCaseName(), input)));
        }
        buffer.printf("return _duplicate;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldDuplicate(Target target) {
        FieldSpec field = target.field();
        if ((field.type() instanceof FieldType.BoolFieldType) ||
                (field.type() instanceof FieldType.Int8FieldType) ||
                (field.type() instanceof FieldType.Int16FieldType) ||
                (field.type() instanceof FieldType.Int32FieldType) ||
                (field.type() instanceof FieldType.Int64FieldType) ||
                (field.type() instanceof FieldType.Float64FieldType) ||
                (field.type() instanceof FieldType.UUIDFieldType)) {
            buffer.printf("%s;%n", target.assignmentStatement(target.sourceVariable()));
        } else {
            IsNullConditional cond = IsNullConditional.forName(target.sourceVariable()).
                nullableVersions(target.field().nullableVersions()).
                ifNull(() -> buffer.printf("%s;%n", target.assignmentStatement("null")));
            if (field.type().isBytes()) {
                if (field.zeroCopy()) {
                    cond.ifShouldNotBeNull(() ->
                        buffer.printf("%s;%n", target.assignmentStatement(
                            String.format("%s.duplicate()", target.sourceVariable()))));
                } else {
                    cond.ifShouldNotBeNull(() -> {
                        headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
                        buffer.printf("%s;%n", target.assignmentStatement(
                            String.format("MessageUtil.duplicate(%s)",
                                target.sourceVariable())));
                    });
                }
            } else if (field.type().isRecords()) {
                cond.ifShouldNotBeNull(() -> {
                    headerGenerator.addImport(MessageGenerator.MEMORY_RECORDS_CLASS);
                    buffer.printf("%s;%n", target.assignmentStatement(
                        String.format("MemoryRecords.readableRecords(((MemoryRecords) %s).buffer().duplicate())",
                            target.sourceVariable())));
                });
            } else if (field.type().isStruct()) {
                cond.ifShouldNotBeNull(() ->
                    buffer.printf("%s;%n", target.assignmentStatement(
                        String.format("%s.duplicate()", target.sourceVariable()))));
            } else if (field.type().isString()) {
                // Strings are immutable, so we don't need to duplicate them.
                cond.ifShouldNotBeNull(() ->
                    buffer.printf("%s;%n", target.assignmentStatement(
                        target.sourceVariable())));
            } else if (field.type().isArray()) {
                cond.ifShouldNotBeNull(() -> {
                    String newArrayName =
                        String.format("new%s", field.capitalizedCamelCaseName());
                    buffer.printf("%s %s = new %s(%s.size());%n",
                        fieldConcreteJavaType(field), newArrayName,
                        fieldConcreteJavaType(field), target.sourceVariable());
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                    buffer.printf("for (%s _element : %s) {%n",
                        getBoxedJavaType(arrayType.elementType()), target.sourceVariable());
                    buffer.incrementIndent();
                    generateFieldDuplicate(target.arrayElementTarget(input ->
                        String.format("%s.add(%s)", newArrayName, input)));
                    buffer.decrementIndent();
                    buffer.printf("}%n");
                    buffer.printf("%s;%n", target.assignmentStatement(
                        String.format("new%s", field.capitalizedCamelCaseName())));
                });
            } else {
                throw new RuntimeException("Unhandled field type " + field.type());
            }
            cond.generate(buffer);
        }
    }

    private void generateClassToString(String className, StructSpec struct) {
        buffer.printf("@Override%n");
        buffer.printf("public String toString() {%n");
        buffer.incrementIndent();
        buffer.printf("return \"%s(\"%n", className);
        buffer.incrementIndent();
        String prefix = "";
        for (FieldSpec field : struct.fields()) {
            generateFieldToString(prefix, field);
            prefix = ", ";
        }
        buffer.printf("+ \")\";%n");
        buffer.decrementIndent();
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldToString(String prefix, FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("+ \"%s%s=\" + (%s ? \"true\" : \"false\")%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if ((field.type() instanceof FieldType.Int8FieldType) ||
                (field.type() instanceof FieldType.Int16FieldType) ||
                (field.type() instanceof FieldType.Int32FieldType) ||
                (field.type() instanceof FieldType.Int64FieldType) ||
                (field.type() instanceof FieldType.Float64FieldType)) {
            buffer.printf("+ \"%s%s=\" + %s%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isString()) {
            buffer.printf("+ \"%s%s=\" + ((%s == null) ? \"null\" : \"'\" + %s.toString() + \"'\")%n",
                prefix, field.camelCaseName(), field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            if (field.zeroCopy()) {
                buffer.printf("+ \"%s%s=\" + %s%n",
                    prefix, field.camelCaseName(), field.camelCaseName());
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
                buffer.printf("+ \"%s%s=\" + Arrays.toString(%s)%n",
                    prefix, field.camelCaseName(), field.camelCaseName());
            }
        } else if (field.type().isRecords()) {
            buffer.printf("+ \"%s%s=\" + %s%n",
                    prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isStruct() ||
            field.type() instanceof FieldType.UUIDFieldType) {
        } else if (field.type().isStruct()) {
            buffer.printf("+ \"%s%s=\" + %s.toString()%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isArray()) {
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            if (field.nullableVersions().empty()) {
                buffer.printf("+ \"%s%s=\" + MessageUtil.deepToString(%s.iterator())%n",
                    prefix, field.camelCaseName(), field.camelCaseName());
            } else {
                buffer.printf("+ \"%s%s=\" + ((%s == null) ? \"null\" : " +
                    "MessageUtil.deepToString(%s.iterator()))%n",
                    prefix, field.camelCaseName(), field.camelCaseName(), field.camelCaseName());
            }
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private String fieldDefault(FieldSpec field) {
        if (field.type() instanceof FieldType.BoolFieldType) {
            if (field.defaultString().isEmpty()) {
                return "false";
            } else if (field.defaultString().equalsIgnoreCase("true")) {
                return "true";
            } else if (field.defaultString().equalsIgnoreCase("false")) {
                return "false";
            } else {
                throw new RuntimeException("Invalid default for boolean field " +
                    field.name() + ": " + field.defaultString());
            }
        } else if ((field.type() instanceof FieldType.Int8FieldType) ||
                   (field.type() instanceof FieldType.Int16FieldType) ||
                   (field.type() instanceof FieldType.Int32FieldType) ||
                   (field.type() instanceof FieldType.Int64FieldType)) {
            int base = 10;
            String defaultString = field.defaultString();
            if (defaultString.startsWith("0x")) {
                base = 16;
                defaultString = defaultString.substring(2);
            }
            if (field.type() instanceof FieldType.Int8FieldType) {
                if (defaultString.isEmpty()) {
                    return "(byte) 0";
                } else {
                    try {
                        Byte.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int8 field " +
                            field.name() + ": " + defaultString, e);
                    }
                    return "(byte) " + field.defaultString();
                }
            } else if (field.type() instanceof FieldType.Int16FieldType) {
                if (defaultString.isEmpty()) {
                    return "(short) 0";
                } else {
                    try {
                        Short.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int16 field " +
                            field.name() + ": " + field.defaultString(), e);
                    }
                    return "(short) " + field.defaultString();
                }
            } else if (field.type() instanceof FieldType.Int32FieldType) {
                if (defaultString.isEmpty()) {
                    return "0";
                } else {
                    try {
                        Integer.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int32 field " +
                            field.name() + ": " + field.defaultString(), e);
                    }
                    return field.defaultString();
                }
            } else if (field.type() instanceof FieldType.Int64FieldType) {
                if (defaultString.isEmpty()) {
                    return "0L";
                } else {
                    try {
                        Long.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int64 field " +
                            field.name() + ": " + field.defaultString(), e);
                    }
                    return field.defaultString() + "L";
                }
            } else {
                throw new RuntimeException("Unsupported field type " + field.type());
            }
        } else if (field.type() instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            if (field.defaultString().isEmpty()) {
                headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
                return "MessageUtil.ZERO_UUID";
            } else {
                try {
                    UUID.fromString(field.defaultString());
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Invalid default for uuid field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                headerGenerator.addImport(MessageGenerator.UUID_CLASS);
                return "UUID.fromString(\"" + field.defaultString() + "\")";
            }
        } else if (field.type() instanceof FieldType.Float64FieldType) {
            if (field.defaultString().isEmpty()) {
                return "0.0";
            } else {
                try {
                    Double.parseDouble(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for float64 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return "Double.parseDouble(\"" + field.defaultString() + "\")";
            }
        } else if (field.type() instanceof FieldType.StringFieldType) {
            if (field.defaultString().equals("null")) {
                validateNullDefault(field);
                return "null";
            } else {
                return "\"" + field.defaultString() + "\"";
            }
        } else if (field.type().isBytes()) {
            if (field.defaultString().equals("null")) {
                validateNullDefault(field);
                return "null";
            } else if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for bytes field " +
                        field.name() + ".  The only valid default for a bytes field " +
                        "is empty or null.");
            }
            if (field.zeroCopy()) {
                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                return "ByteUtils.EMPTY_BUF";
            } else {
                headerGenerator.addImport(MessageGenerator.BYTES_CLASS);
                return "Bytes.EMPTY";
            }
        } else if (field.type().isRecords()) {
            return "null";
        } else if (field.type().isStruct()) {
            if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for struct field " +
                    field.name() + ": custom defaults are not supported for struct fields.");
            }
            return "new " + field.type().toString() + "()";
        } else if (field.type().isArray()) {
            if (field.defaultString().equals("null")) {
                validateNullDefault(field);
                return "null";
            } else if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for array field " +
                    field.name() + ".  The only valid default for an array field " +
                        "is the empty array or null.");
            }
            return String.format("new %s(0)", fieldConcreteJavaType(field));
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void validateNullDefault(FieldSpec field) {
        if (!(field.nullableVersions().contains(field.versions()))) {
            throw new RuntimeException("null cannot be the default for field " +
                    field.name() + ", because not all versions of this field are " +
                    "nullable.");
        }
    }

    private void generateFieldAccessor(FieldSpec field) {
        buffer.printf("%n");
        generateAccessor(fieldAbstractJavaType(field), field.camelCaseName(),
            field.camelCaseName());
    }

    private void generateAccessor(String javaType, String functionName, String memberName) {
        buffer.printf("public %s %s() {%n", javaType, functionName);
        buffer.incrementIndent();
        buffer.printf("return this.%s;%n", memberName);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldMutator(String className, FieldSpec field) {
        buffer.printf("%n");
        buffer.printf("public %s set%s(%s v) {%n",
            className,
            field.capitalizedCamelCaseName(),
            fieldAbstractJavaType(field));
        buffer.incrementIndent();
        buffer.printf("this.%s = v;%n", field.camelCaseName());
        buffer.printf("return this;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateSetter(String javaType, String functionName, String memberName) {
        buffer.printf("public void %s(%s v) {%n", functionName, javaType);
        buffer.incrementIndent();
        buffer.printf("this.%s = v;%n", memberName);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private Versions fieldFlexibleVersions(FieldSpec field) {
        if (field.flexibleVersions().isPresent()) {
            if (!messageFlexibleVersions.intersect(field.flexibleVersions().get()).
                    equals(field.flexibleVersions().get())) {
                throw new RuntimeException("The flexible versions for field " +
                    field.name() + " are " + field.flexibleVersions().get() +
                    ", which are not a subset of the flexible versions for the " +
                    "message as a whole, which are " + messageFlexibleVersions);
            }
            return field.flexibleVersions().get();
        } else {
            return messageFlexibleVersions;
        }
    }
}
