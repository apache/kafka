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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generates Kafka MessageData classes.
 */
public final class MessageDataGenerator implements MessageClassGenerator {
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

    @Override
    public String outputName(MessageSpec spec) {
        return spec.dataClassName();
    }

    @Override
    public void generateAndWrite(MessageSpec message, BufferedWriter writer) throws Exception {
        generate(message);
        write(writer);
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
            message.dataClassName(),
            message.struct(),
            message.struct().versions());
        headerGenerator.generate();
    }

    void write(BufferedWriter writer) throws Exception {
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
        generateClassMessageSize(className, struct, parentVersions);
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
        generateUnknownTaggedFieldsAccessor();
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
        Set<String> classModifiers = new LinkedHashSet<>();
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
                if (!structRegistry.commonStructNames().contains(field.typeString())) {
                    generateClass(Optional.empty(),
                            field.typeString(),
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
            FieldSpec.collectionType(className), className);
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
        generateCollectionDuplicateMethod(className);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetZeroArgConstructor(String className) {
        buffer.printf("public %s() {%n", FieldSpec.collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super();%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetSizeArgConstructor(String className) {
        buffer.printf("public %s(int expectedNumElements) {%n",
            FieldSpec.collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super(expectedNumElements);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetIteratorConstructor(String className) {
        headerGenerator.addImport(MessageGenerator.ITERATOR_CLASS);
        buffer.printf("public %s(Iterator<%s> iterator) {%n",
            FieldSpec.collectionType(className), className);
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
            map(f -> String.format("%s %s",
                f.concreteJavaType(headerGenerator, structRegistry), f.camelCaseName())).
            collect(Collectors.joining(", "));
    }

    private void generateCollectionDuplicateMethod(String className) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public %s duplicate() {%n", FieldSpec.collectionType(className));
        buffer.incrementIndent();
        buffer.printf("%s _duplicate = new %s(size());%n",
            FieldSpec.collectionType(className), FieldSpec.collectionType(className));
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
        buffer.printf("%s %s;%n",
            field.fieldAbstractJavaType(headerGenerator, structRegistry),
            field.camelCaseName());
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

    private void generateUnknownTaggedFieldsAccessor() {
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

    private void generateClassConstructors(String className, StructSpec struct, boolean isSetElement) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS);
        buffer.printf("public %s(Readable _readable, short _version) {%n", className);
        buffer.incrementIndent();
        buffer.printf("read(_readable, _version);%n");
        generateConstructorEpilogue(isSetElement);
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");

        buffer.printf("public %s() {%n", className);
        buffer.incrementIndent();
        for (FieldSpec field : struct.fields()) {
            buffer.printf("this.%s = %s;%n",
                field.camelCaseName(),
                field.fieldDefault(headerGenerator, structRegistry));
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
        buffer.printf("public final void read(Readable _readable, short _version) {%n");
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
                    buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                        field.fieldDefault(headerGenerator, structRegistry));
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
                    } else if (field.type().isStruct()) {
                        generateStructReader(field, presentAndUntaggedVersions, false);
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
                                } else if (field.type().isStruct()) {
                                    generateStructReader(field, presentAndTaggedVersions, true);
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

    private void generateStructReader(
        FieldSpec field,
        Versions supportedVersions,
        boolean tagged
    ) {
        VersionConditional.forVersions(field.nullableVersions(), supportedVersions).
            ifMember(__ -> {
                if (tagged) {
                    buffer.printf("if (_readable.readUnsignedVarint() <= 0) {%n");
                } else {
                    buffer.printf("if (_readable.readByte() < 0) {%n");
                }
                buffer.incrementIndent();
                buffer.printf("this.%s = null;%n", field.camelCaseName());
                buffer.decrementIndent();
                buffer.printf("} else {%n");
                buffer.incrementIndent();
                buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                    primitiveReadExpression(field.type()));
                buffer.decrementIndent();
                buffer.printf("}%n");
            }).
            ifNotMember(__ -> {
                buffer.printf("this.%s = %s;%n", field.camelCaseName(),
                    primitiveReadExpression(field.type()));
            }).
            generate(buffer);
    }

    private String primitiveReadExpression(FieldType type) {
        if (type instanceof FieldType.BoolFieldType) {
            return "_readable.readByte() != 0";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "_readable.readByte()";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "_readable.readShort()";
        } else if (type instanceof FieldType.Uint16FieldType) {
            return "_readable.readUnsignedShort()";
        } else if (type instanceof FieldType.Uint32FieldType) {
            return "_readable.readUnsignedInt()";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "_readable.readInt()";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "_readable.readLong()";
        } else if (type instanceof FieldType.UUIDFieldType) {
            return "_readable.readUuid()";
        } else if (type instanceof FieldType.Float64FieldType) {
            return "_readable.readDouble()";
        } else if (type.isStruct()) {
            return String.format("new %s(_readable, _version)", type);
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
                buffer.printf("%s_readable.readByteBuffer(%s)%s",
                    assignmentPrefix, lengthVar, assignmentSuffix);
            } else {
                buffer.printf("byte[] newBytes = _readable.readArray(%s);%n", lengthVar);
                buffer.printf("%snewBytes%s", assignmentPrefix, assignmentSuffix);
            }
        } else if (type.isRecords()) {
            buffer.printf("%s_readable.readRecords(%s)%s",
                assignmentPrefix, lengthVar, assignmentSuffix);
        } else if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            buffer.printf("if (%s > _readable.remaining()) {%n", lengthVar);
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"Tried to allocate a collection of size \" + %s + \", but " +
                    "there are only \" + _readable.remaining() + \" bytes remaining.\");%n", lengthVar);
            buffer.decrementIndent();
            buffer.printf("}%n");
            if (isStructArrayWithKeys) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                buffer.printf("%s newCollection = new %s(%s);%n",
                    FieldSpec.collectionType(arrayType.elementType().toString()),
                        FieldSpec.collectionType(arrayType.elementType().toString()), lengthVar);
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                String boxedArrayType =
                    arrayType.elementType().getBoxedJavaType(headerGenerator);
                buffer.printf("ArrayList<%s> newCollection = new ArrayList<>(%s);%n", boxedArrayType, lengthVar);
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
                            } else if (field.type().isStruct()) {
                                IsNullConditional.forName(field.camelCaseName()).
                                    possibleVersions(presentAndUntaggedVersions).
                                    nullableVersions(field.nullableVersions()).
                                    ifNull(() -> {
                                        VersionConditional.forVersions(field.nullableVersions(), presentAndUntaggedVersions).
                                            ifMember(__ -> {
                                                buffer.printf("_writable.writeByte((byte) -1);%n");
                                            }).
                                            ifNotMember(__ -> {
                                                buffer.printf("throw new NullPointerException();%n");
                                            }).
                                            generate(buffer);
                                    }).
                                    ifShouldNotBeNull(() -> {
                                        VersionConditional.forVersions(field.nullableVersions(), presentAndUntaggedVersions).
                                            ifMember(__ -> {
                                                buffer.printf("_writable.writeByte((byte) 1);%n");
                                            }).
                                            generate(buffer);
                                        buffer.printf("%s;%n",
                                            primitiveWriteExpression(field.type(), field.camelCaseName()));
                                    }).
                                    generate(buffer);
                            } else {
                                buffer.printf("%s;%n",
                                    primitiveWriteExpression(field.type(), field.camelCaseName()));
                            }
                        }).
                        ifMember(__ -> {
                            field.generateNonDefaultValueCheck(headerGenerator,
                                structRegistry, buffer, "this.", field.nullableVersions());
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
                    field.generateNonIgnorableFieldCheck(headerGenerator,
                        structRegistry, "this.", buffer);
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
            ifMember(flexibleVersions -> {
                buffer.printf("_writable.writeUnsignedVarint(_numTaggedFields);%n");
                int prevTag = -1;
                for (FieldSpec field : taggedFields.values()) {
                    if (prevTag + 1 != field.tag().get()) {
                        buffer.printf("_rawWriter.writeRawTags(_writable, %d);%n", field.tag().get());
                    }
                    VersionConditional.
                        forVersions(field.taggedVersions().intersect(field.versions()), flexibleVersions).
                        allowMembershipCheckAlwaysFalse(false).
                        ifMember(presentAndTaggedVersions -> {
                            IsNullConditional cond = IsNullConditional.forName(field.camelCaseName()).
                                nullableVersions(field.nullableVersions()).
                                possibleVersions(presentAndTaggedVersions).
                                alwaysEmitBlockScope(true).
                                ifShouldNotBeNull(() -> {
                                    if (!field.defaultString().equals("null")) {
                                        field.generateNonDefaultValueCheck(headerGenerator,
                                            structRegistry, buffer, "this.", Versions.NONE);
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
                                        VersionConditional.forVersions(field.nullableVersions(), presentAndTaggedVersions).
                                            ifMember(___ -> {
                                                buffer.printf("_writable.writeUnsignedVarint(this.%s.size(_cache, _version) + 1);%n",
                                                    field.camelCaseName());
                                                buffer.printf("_writable.writeUnsignedVarint(1);%n");
                                            }).
                                            ifNotMember(___ -> {
                                                buffer.printf("_writable.writeUnsignedVarint(this.%s.size(_cache, _version));%n",
                                                    field.camelCaseName());
                                            }).
                                            generate(buffer);
                                        buffer.printf("%s;%n",
                                            primitiveWriteExpression(field.type(), field.camelCaseName()));
                                    } else if (field.type().isRecords()) {
                                        throw new RuntimeException("Unsupported attempt to declare field `" +
                                            field.name() + "` with `records` type as a tagged field.");
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
        } else if (type instanceof FieldType.Uint16FieldType) {
            return String.format("_writable.writeUnsignedShort(%s)", name);
        } else if (type instanceof FieldType.Uint32FieldType) {
            return String.format("_writable.writeUnsignedInt(%s)", name);
        } else if (type instanceof FieldType.Int32FieldType) {
            return String.format("_writable.writeInt(%s)", name);
        } else if (type instanceof FieldType.Int64FieldType) {
            return String.format("_writable.writeLong(%s)", name);
        } else if (type instanceof FieldType.UUIDFieldType) {
            return String.format("_writable.writeUuid(%s)", name);
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
                    buffer.printf("_writable.writeRecords(%s);%n", name);
                } else if (type.isArray()) {
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
                    FieldType elementType = arrayType.elementType();
                    String elementName = String.format("%sElement", name);
                    buffer.printf("for (%s %s : %s) {%n",
                        elementType.getBoxedJavaType(headerGenerator),
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

    private void generateClassMessageSize(
        String className,
        StructSpec struct,
        Versions parentVersions
    ) {
        headerGenerator.addImport(MessageGenerator.OBJECT_SERIALIZATION_CACHE_CLASS);
        headerGenerator.addImport(MessageGenerator.MESSAGE_SIZE_ACCUMULATOR_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {%n");
        buffer.incrementIndent();
        buffer.printf("int _numTaggedFields = 0;%n");
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
        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));%n");
        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));%n");
        buffer.printf("_size.addBytes(_field.size());%n");
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
                buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_numTaggedFields));%n");
            }).
            generate(buffer);
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
                    buffer.printf("_size.addBytes(_stringBytes.length + 2);%n");
                }).
                ifMember(__ -> {
                    headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                    buffer.printf("_size.addBytes(_stringBytes.length + " +
                        "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));%n");
                }).
                generate(buffer);
        } else if (type instanceof FieldType.BytesFieldType) {
            buffer.printf("_size.addBytes(%s.length);%n", fieldName);
            VersionConditional.forVersions(flexibleVersions, versions).
                ifNotMember(__ -> {
                    buffer.printf("_size.addBytes(4);%n");
                }).
                ifMember(__ -> {
                    headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                    buffer.printf("_size.addBytes(" +
                            "ByteUtils.sizeOfUnsignedVarint(%s.length + 1));%n",
                        fieldName);
                }).
                generate(buffer);
        } else if (type instanceof FieldType.StructType) {
            buffer.printf("%s.addSize(_size, _cache, _version);%n", fieldName);
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
            field.generateNonDefaultValueCheck(headerGenerator, structRegistry, buffer,
                "this.", field.nullableVersions());
            buffer.incrementIndent();
            buffer.printf("_numTaggedFields++;%n");
            buffer.printf("_size.addBytes(%d);%n",
                MessageGenerator.sizeOfUnsignedVarint(field.tag().get()));
            // Account for the tagged field prefix length.
            buffer.printf("_size.addBytes(%d);%n",
                MessageGenerator.sizeOfUnsignedVarint(field.type().fixedLength().get()));
            buffer.printf("_size.addBytes(%d);%n", field.type().fixedLength().get());
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else {
            buffer.printf("_size.addBytes(%d);%n", field.type().fixedLength().get());
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
                                buffer.printf("_size.addBytes(%d);%n",
                                    MessageGenerator.sizeOfUnsignedVarint(field.tag().get()));
                                buffer.printf("_size.addBytes(%d);%n", MessageGenerator.sizeOfUnsignedVarint(
                                    MessageGenerator.sizeOfUnsignedVarint(0)));
                            }
                            buffer.printf("_size.addBytes(%d);%n", MessageGenerator.sizeOfUnsignedVarint(0));
                        }).
                        ifNotMember(__ -> {
                            if (tagged) {
                                throw new RuntimeException("Tagged field " + field.name() +
                                    " should not be present in non-flexible versions.");
                            }
                            if (field.type().isString()) {
                                buffer.printf("_size.addBytes(2);%n");
                            } else if (field.type().isStruct()) {
                                buffer.printf("_size.addBytes(1);%n");
                            } else {
                                buffer.printf("_size.addBytes(4);%n");
                            }
                        }).
                        generate(buffer);
                }
            }).
            ifShouldNotBeNull(() -> {
                if (tagged) {
                    if (!field.defaultString().equals("null")) {
                        field.generateNonDefaultValueCheck(headerGenerator,
                            structRegistry, buffer, "this.", Versions.NONE);
                        buffer.incrementIndent();
                    }
                    buffer.printf("_numTaggedFields++;%n");
                    buffer.printf("_size.addBytes(%d);%n",
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
                                buffer.printf("_size.addBytes(_stringBytes.length + _stringPrefixSize + " +
                                    "ByteUtils.sizeOfUnsignedVarint(_stringPrefixSize + _stringBytes.length));%n");

                            } else {
                                buffer.printf("_size.addBytes(_stringBytes.length + " +
                                    "ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1));%n");
                            }
                        }).
                        ifNotMember(__ -> {
                            if (tagged) {
                                throw new RuntimeException("Tagged field " + field.name() +
                                    " should not be present in non-flexible versions.");
                            }
                            buffer.printf("_size.addBytes(_stringBytes.length + 2);%n");
                        }).
                        generate(buffer);
                } else if (field.type().isArray()) {
                    if (tagged) {
                        buffer.printf("int _sizeBeforeArray = _size.totalSize();%n");
                    }
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.size() + 1));%n",
                                field.camelCaseName());
                        }).
                        ifNotMember(__ -> {
                            buffer.printf("_size.addBytes(4);%n");
                        }).
                        generate(buffer);
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                    FieldType elementType = arrayType.elementType();
                    if (elementType.fixedLength().isPresent()) {
                        buffer.printf("_size.addBytes(%s.size() * %d);%n",
                            field.camelCaseName(),
                            elementType.fixedLength().get());
                    } else if (elementType instanceof FieldType.ArrayType) {
                        throw new RuntimeException("Arrays of arrays are not supported " +
                            "(use a struct).");
                    } else {
                        buffer.printf("for (%s %sElement : %s) {%n",
                            elementType.getBoxedJavaType(headerGenerator),
                            field.camelCaseName(), field.camelCaseName());
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
                        buffer.printf("int _arraySize = _size.totalSize() - _sizeBeforeArray;%n");
                        buffer.printf("_cache.setArraySizeInBytes(%s, _arraySize);%n",
                            field.camelCaseName());
                        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_arraySize));%n");
                    }
                } else if (field.type().isBytes()) {
                    if (tagged) {
                        buffer.printf("int _sizeBeforeBytes = _size.totalSize();%n");
                    }
                    if (field.zeroCopy()) {
                        buffer.printf("_size.addZeroCopyBytes(%s.remaining());%n", field.camelCaseName());
                    } else {
                        buffer.printf("_size.addBytes(%s.length);%n", field.camelCaseName());
                    }
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            if (field.zeroCopy()) {
                                buffer.printf("_size.addBytes(" +
                                        "ByteUtils.sizeOfUnsignedVarint(%s.remaining() + 1));%n", field.camelCaseName());
                            } else {
                                buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(%s.length + 1));%n",
                                    field.camelCaseName());
                            }
                        }).
                        ifNotMember(__ -> {
                            buffer.printf("_size.addBytes(4);%n");
                        }).
                        generate(buffer);
                    if (tagged) {
                        headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                        buffer.printf("int _bytesSize = _size.totalSize() - _sizeBeforeBytes;%n");
                        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_bytesSize));%n");
                    }
                } else if (field.type().isRecords()) {
                    buffer.printf("_size.addZeroCopyBytes(%s.sizeInBytes());%n", field.camelCaseName());
                    VersionConditional.forVersions(fieldFlexibleVersions(field), possibleVersions).
                        ifMember(__ -> {
                            headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                            buffer.printf("_size.addBytes(" +
                                "ByteUtils.sizeOfUnsignedVarint(%s.sizeInBytes() + 1));%n", field.camelCaseName());
                        }).
                        ifNotMember(__ -> {
                            buffer.printf("_size.addBytes(4);%n");
                        }).
                        generate(buffer);
                } else if (field.type().isStruct()) {
                    // Adding a byte if the field is nullable. A byte works for both regular and tagged struct fields.
                    VersionConditional.forVersions(field.nullableVersions(), possibleVersions).
                        ifMember(__ -> {
                            buffer.printf("_size.addBytes(1);%n");
                        }).
                        generate(buffer);

                    if (tagged) {
                        buffer.printf("int _sizeBeforeStruct = _size.totalSize();%n", field.camelCaseName());
                        buffer.printf("this.%s.addSize(_size, _cache, _version);%n", field.camelCaseName());
                        buffer.printf("int _structSize = _size.totalSize() - _sizeBeforeStruct;%n", field.camelCaseName());
                        buffer.printf("_size.addBytes(ByteUtils.sizeOfUnsignedVarint(_structSize));%n");
                    } else {
                        buffer.printf("this.%s.addSize(_size, _cache, _version);%n", field.camelCaseName());
                    }
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
                    (field.type() instanceof FieldType.Uint16FieldType) ||
                    (field.type() instanceof FieldType.Int32FieldType)) {
            buffer.printf("hashCode = 31 * hashCode + %s;%n",
                field.camelCaseName());
        } else if (field.type() instanceof FieldType.Int64FieldType ||
                    (field.type() instanceof FieldType.Uint32FieldType)) {
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
                (field.type() instanceof FieldType.Uint16FieldType) ||
                (field.type() instanceof FieldType.Uint32FieldType) ||
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
                    String type = field.concreteJavaType(headerGenerator, structRegistry);
                    buffer.printf("%s %s = new %s(%s.size());%n",
                        type, newArrayName, type, target.sourceVariable());
                    FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
                    buffer.printf("for (%s _element : %s) {%n",
                        arrayType.elementType().getBoxedJavaType(headerGenerator),
                        target.sourceVariable());
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
            buffer.printf("+ \"%s%s=\" + (%s ? \"true\" : \"false\")%n", prefix, field.camelCaseName(), field.camelCaseName());
        } else if ((field.type() instanceof FieldType.Int8FieldType) ||
                (field.type() instanceof FieldType.Int16FieldType) ||
                (field.type() instanceof FieldType.Uint16FieldType) ||
                (field.type() instanceof FieldType.Uint32FieldType) ||
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
        } else if (field.type() instanceof FieldType.UUIDFieldType) {
            buffer.printf("+ \"%s%s=\" + %s.toString()%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isStruct()) {
            if (field.nullableVersions().empty()) {
                buffer.printf("+ \"%s%s=\" + %s.toString()%n",
                    prefix, field.camelCaseName(), field.camelCaseName());
            } else {
                buffer.printf("+ \"%s%s=\" + ((%s == null) ? \"null\" : %s.toString())%n",
                    prefix, field.camelCaseName(), field.camelCaseName(), field.camelCaseName());
            }
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

    private void generateFieldAccessor(FieldSpec field) {
        buffer.printf("%n");
        generateAccessor(field.fieldAbstractJavaType(headerGenerator, structRegistry),
            field.camelCaseName(),
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
            field.fieldAbstractJavaType(headerGenerator, structRegistry));
        buffer.incrementIndent();
        if (field.type() instanceof FieldType.Uint16FieldType) {
            buffer.printf("if (v < 0 || v > %d) {%n", MessageGenerator.UNSIGNED_SHORT_MAX);
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"Invalid value \" + v + " +
                    "\" for unsigned short field.\");%n");
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        if (field.type() instanceof FieldType.Uint32FieldType) {
            buffer.printf("if (v < 0 || v > %dL) {%n", MessageGenerator.UNSIGNED_INT_MAX);
            buffer.incrementIndent();
            buffer.printf("throw new RuntimeException(\"Invalid value \" + v + " +
                    "\" for unsigned int field.\");%n");
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
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
