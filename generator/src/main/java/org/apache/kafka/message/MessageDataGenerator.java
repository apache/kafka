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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Generates Kafka MessageData classes.
 */
public final class MessageDataGenerator {
    private final HeaderGenerator headerGenerator;
    private final SchemaGenerator schemaGenerator;
    private final CodeBuffer buffer;

    MessageDataGenerator() {
        this.headerGenerator = new HeaderGenerator();
        this.schemaGenerator = new SchemaGenerator(headerGenerator);
        this.buffer = new CodeBuffer();
    }

    void generate(MessageSpec message) throws Exception {
        if (message.struct().versions().contains(Short.MAX_VALUE)) {
            throw new RuntimeException("Message " + message.name() + " does " +
                "not specify a maximum version.");
        }
        schemaGenerator.generateSchemas(message);
        generateClass(Optional.of(message),
            message.name() + "Data",
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
        generateClassConstructors(className, struct);
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
        generateClassSize(className, struct, parentVersions);
        buffer.printf("%n");
        generateClassEquals(className, struct, isSetElement);
        buffer.printf("%n");
        generateClassHashCode(struct, isSetElement);
        buffer.printf("%n");
        generateClassToString(className, struct);
        generateFieldAccessors(struct, isSetElement);
        generateFieldMutators(struct, className, isSetElement);

        if (!isTopLevel) {
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        generateSubclasses(className, struct, parentVersions, isSetElement);
        if (isTopLevel) {
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
                generateClass(Optional.empty(),
                    arrayType.elementType().toString(),
                    field.toStruct(),
                    parentVersions.intersect(struct.versions()));
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
        generateHashSetSizeArgConstructor(className);
        generateHashSetIteratorConstructor(className);
        generateHashSetFindMethod(className, struct);
        generateHashSetFindAllMethod(className, struct);
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateHashSetZeroArgConstructor(String className) {
        buffer.printf("public %s() {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super();%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetSizeArgConstructor(String className) {
        buffer.printf("public %s(int expectedNumElements) {%n", collectionType(className));
        buffer.incrementIndent();
        buffer.printf("super(expectedNumElements);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetIteratorConstructor(String className) {
        headerGenerator.addImport(MessageGenerator.ITERATOR_CLASS);
        buffer.printf("public %s(Iterator<%s> iterator) {%n", collectionType(className), className);
        buffer.incrementIndent();
        buffer.printf("super(iterator);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetFindMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public %s find(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct));
        buffer.incrementIndent();
        generateKeyElement(className, struct);
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("return find(key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateHashSetFindAllMethod(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.LIST_CLASS);
        buffer.printf("public List<%s> findAll(%s) {%n", className,
            commaSeparatedHashSetFieldAndTypes(struct));
        buffer.incrementIndent();
        generateKeyElement(className, struct);
        headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
        buffer.printf("return findAll(key);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
    }

    private void generateKeyElement(String className, StructSpec struct) {
        buffer.printf("%s key = new %s();%n", className, className);
        for (FieldSpec field : struct.fields()) {
            if (field.mapKey()) {
                buffer.printf("key.set%s(%s);%n",
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

    private void generateFieldDeclarations(StructSpec struct, boolean isSetElement) {
        for (FieldSpec field : struct.fields()) {
            generateFieldDeclaration(field);
        }
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
        } else if (field.type().isString()) {
            return "String";
        } else if (field.type().isBytes()) {
            return "byte[]";
        } else if (field.type().isStruct()) {
            return MessageGenerator.capitalizeFirst(field.typeString());
        } else if (field.type().isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (field.toStruct().hasKeys()) {
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
            if (field.toStruct().hasKeys()) {
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

    private void generateClassConstructors(String className, StructSpec struct) {
        headerGenerator.addImport(MessageGenerator.READABLE_CLASS);
        buffer.printf("public %s(Readable readable, short version) {%n", className);
        buffer.incrementIndent();
        initializeArrayDefaults(struct);
        buffer.printf("read(readable, version);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("public %s(Struct struct, short version) {%n", className);
        buffer.incrementIndent();
        initializeArrayDefaults(struct);
        buffer.printf("fromStruct(struct, version);%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.printf("%n");
        buffer.printf("public %s() {%n", className);
        buffer.incrementIndent();
        for (FieldSpec field : struct.fields()) {
            buffer.printf("this.%s = %s;%n",
                field.camelCaseName(), fieldDefault(field));
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void initializeArrayDefaults(StructSpec struct) {
        for (FieldSpec field : struct.fields()) {
            if (field.type().isArray()) {
                buffer.printf("this.%s = %s;%n",
                    field.camelCaseName(), fieldDefault(field));
            }
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
        buffer.printf("public void read(Readable readable, short version) {%n");
        buffer.incrementIndent();
        if (generateInverseVersionCheck(parentVersions, struct.versions())) {
            buffer.incrementIndent();
            headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
            buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                "version \" + version + \" of %s\");%n", className);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        Versions curVersions = parentVersions.intersect(struct.versions());
        if (curVersions.empty()) {
            throw new RuntimeException("Version ranges " + parentVersions +
                " and " + struct.versions() + " have no versions in common.");
        }
        for (FieldSpec field : struct.fields()) {
            generateFieldReader(field, curVersions);
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldReader(FieldSpec field, Versions curVersions) {
        if (field.type().isArray()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            if (!maybeAbsent) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            boolean hasKeys = field.toStruct().hasKeys();
            buffer.printf("int arrayLength = readable.readInt();%n");
            buffer.printf("if (arrayLength < 0) {%n");
            buffer.incrementIndent();
            buffer.printf("this.%s = null;%n",
                field.camelCaseName());
            buffer.decrementIndent();
            buffer.printf("} else {%n");
            buffer.incrementIndent();
            buffer.printf("this.%s.clear(%s);%n",
                field.camelCaseName(),
                hasKeys ? "arrayLength" : "");
            buffer.printf("for (int i = 0; i < arrayLength; i++) {%n");
            buffer.incrementIndent();
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            buffer.printf("this.%s.add(%s);%n",
                field.camelCaseName(), readFieldFromReadable(arrayType.elementType()));
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.decrementIndent();
            buffer.printf("}%n");
            if (maybeAbsent) {
                generateSetDefault(field);
            } else {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            buffer.printf("this.%s = %s;%n",
                field.camelCaseName(),
                readFieldFromReadable(field.type()));
            if (maybeAbsent) {
                generateSetDefault(field);
            }
        }
    }

    private String readFieldFromReadable(FieldType type) {
        if (type instanceof FieldType.BoolFieldType) {
            return "readable.readByte() != 0";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "readable.readByte()";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "readable.readShort()";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "readable.readInt()";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "readable.readLong()";
        } else if (type.isString()) {
            return "readable.readNullableString()";
        } else if (type.isBytes()) {
            return "readable.readNullableBytes()";
        } else if (type.isStruct()) {
            return String.format("new %s(readable, version)", type.toString());
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateClassFromStruct(String className, StructSpec struct,
                                         Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void fromStruct(Struct struct, short version) {%n");
        buffer.incrementIndent();
        if (generateInverseVersionCheck(parentVersions, struct.versions())) {
            buffer.incrementIndent();
            headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
            buffer.printf("throw new UnsupportedVersionException(\"Can't read " +
                "version \" + version + \" of %s\");%n", className);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        Versions curVersions = parentVersions.intersect(struct.versions());
        if (curVersions.empty()) {
            throw new RuntimeException("Version ranges " + parentVersions +
                " and " + struct.versions() + " have no versions in common.");
        }
        for (FieldSpec field : struct.fields()) {
            generateFieldFromStruct(field, curVersions);
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldFromStruct(FieldSpec field, Versions curVersions) {
        if (field.type().isArray()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            if (!maybeAbsent) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
            buffer.printf("Object[] nestedObjects = struct.getArray(\"%s\");%n",
                field.snakeCaseName());
            boolean maybeNull = false;
            if (!curVersions.intersect(field.nullableVersions()).empty()) {
                maybeNull = true;
                buffer.printf("if (nestedObjects == null) {%n", field.camelCaseName());
                buffer.incrementIndent();
                buffer.printf("this.%s = null;%n", field.camelCaseName());
                buffer.decrementIndent();
                buffer.printf("} else {%n");
                buffer.incrementIndent();
            }
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            FieldType elementType = arrayType.elementType();
            buffer.printf("this.%s = new %s(nestedObjects.length);%n",
                field.camelCaseName(), fieldConcreteJavaType(field));
            buffer.printf("for (Object nestedObject : nestedObjects) {%n");
            buffer.incrementIndent();
            if (elementType.isStruct()) {
                buffer.printf("this.%s.add(new %s((Struct) nestedObject, version));%n",
                    field.camelCaseName(), elementType.toString());
            } else {
                buffer.printf("this.%s.add((%s) nestedObject);%n",
                    field.camelCaseName(), getBoxedJavaType(elementType));
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
            if (maybeNull) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            if (maybeAbsent) {
                generateSetDefault(field);
            } else {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            buffer.printf("this.%s = %s;%n",
                field.camelCaseName(),
                readFieldFromStruct(field.type(), field.snakeCaseName()));
            if (maybeAbsent) {
                generateSetDefault(field);
            }
        }
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
        } else if (type.isString()) {
            return "String";
        } else if (type.isStruct()) {
            return type.toString();
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private String readFieldFromStruct(FieldType type, String name) {
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
        } else if (type.isString()) {
            return String.format("struct.getString(\"%s\")", name);
        } else if (type.isBytes()) {
            return String.format("struct.getByteArray(\"%s\")", name);
        } else if (type.isStruct()) {
            return String.format("new %s(struct, version)", type.toString());
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateClassWriter(String className, StructSpec struct,
            Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.WRITABLE_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public void write(Writable writable, short version) {%n");
        buffer.incrementIndent();
        if (generateInverseVersionCheck(parentVersions, struct.versions())) {
            buffer.incrementIndent();
            headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
            buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                "version \" + version + \" of %s\");%n", className);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        Versions curVersions = parentVersions.intersect(struct.versions());
        if (curVersions.empty()) {
            throw new RuntimeException("Version ranges " + parentVersions +
                " and " + struct.versions() + " have no versions in common.");
        }
        for (FieldSpec field : struct.fields()) {
            generateFieldWriter(field, curVersions);
        }
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private String writeFieldToWritable(FieldType type, boolean nullable, String name) {
        if (type instanceof FieldType.BoolFieldType) {
            return String.format("writable.writeByte(%s ? (byte) 1 : (byte) 0)", name);
        } else if (type instanceof FieldType.Int8FieldType) {
            return String.format("writable.writeByte(%s)", name);
        } else if (type instanceof FieldType.Int16FieldType) {
            return String.format("writable.writeShort(%s)", name);
        } else if (type instanceof FieldType.Int32FieldType) {
            return String.format("writable.writeInt(%s)", name);
        } else if (type instanceof FieldType.Int64FieldType) {
            return String.format("writable.writeLong(%s)", name);
        } else if (type instanceof FieldType.StringFieldType) {
            if (nullable) {
                return String.format("writable.writeNullableString(%s)", name);
            } else {
                return String.format("writable.writeString(%s)", name);
            }
        } else if (type instanceof FieldType.BytesFieldType) {
            if (nullable) {
                return String.format("writable.writeNullableBytes(%s)", name);
            } else {
                return String.format("writable.writeBytes(%s)", name);
            }
        } else if (type instanceof FieldType.StructType) {
            return String.format("%s.write(writable, version)", name);
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void generateFieldWriter(FieldSpec field, Versions curVersions) {
        if (field.type().isArray()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            boolean maybeNull = generateNullCheck(curVersions, field);
            if (maybeNull) {
                buffer.printf("writable.writeInt(-1);%n");
                buffer.decrementIndent();
                buffer.printf("} else {%n");
                buffer.incrementIndent();
            }
            buffer.printf("writable.writeInt(%s.size());%n", field.camelCaseName());
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            FieldType elementType = arrayType.elementType();
            String nestedTypeName = elementType.isStruct() ?
                elementType.toString() : getBoxedJavaType(elementType);
            buffer.printf("for (%s element : %s) {%n",
                nestedTypeName, field.camelCaseName());
            buffer.incrementIndent();
            buffer.printf("%s;%n", writeFieldToWritable(elementType, false, "element"));
            buffer.decrementIndent();
            buffer.printf("}%n");
            if (maybeNull) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            if (maybeAbsent) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            buffer.printf("%s;%n", writeFieldToWritable(field.type(),
                !field.nullableVersions().empty(),
                field.camelCaseName()));
            if (maybeAbsent) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        }
    }

    private void generateClassToStruct(String className, StructSpec struct,
                                       Versions parentVersions) {
        headerGenerator.addImport(MessageGenerator.STRUCT_CLASS);
        buffer.printf("@Override%n");
        buffer.printf("public Struct toStruct(short version) {%n");
        buffer.incrementIndent();
        if (generateInverseVersionCheck(parentVersions, struct.versions())) {
            buffer.incrementIndent();
            headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
            buffer.printf("throw new UnsupportedVersionException(\"Can't write " +
                "version \" + version + \" of %s\");%n", className);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        Versions curVersions = parentVersions.intersect(struct.versions());
        if (curVersions.empty()) {
            throw new RuntimeException("Version ranges " + parentVersions +
                " and " + struct.versions() + " have no versions in common.");
        }
        buffer.printf("Struct struct = new Struct(SCHEMAS[version]);%n");
        for (FieldSpec field : struct.fields()) {
            generateFieldToStruct(field, curVersions);
        }
        buffer.printf("return struct;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldToStruct(FieldSpec field, Versions curVersions) {
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
                (field.type() instanceof FieldType.StringFieldType)) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            buffer.printf("struct.set(\"%s\", this.%s);%n",
                field.snakeCaseName(), field.camelCaseName());
            if (maybeAbsent) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else if (field.type().isBytes()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            buffer.printf("struct.setByteArray(\"%s\", this.%s);%n",
                field.snakeCaseName(), field.camelCaseName());
            if (maybeAbsent) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else if (field.type().isArray()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            if (!maybeAbsent) {
                buffer.printf("{%n");
                buffer.incrementIndent();
            }
            boolean maybeNull = generateNullCheck(curVersions, field);
            if (maybeNull) {
                buffer.printf("struct.set(\"%s\", null);%n", field.snakeCaseName());
                buffer.decrementIndent();
                buffer.printf("} else {%n");
                buffer.incrementIndent();
            }
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            FieldType elementType = arrayType.elementType();
            String boxdElementType = elementType.isStruct() ? "Struct" : getBoxedJavaType(elementType);
            buffer.printf("%s[] nestedObjects = new %s[%s.size()];%n",
                boxdElementType, boxdElementType, field.camelCaseName());
            buffer.printf("int i = 0;%n");
            buffer.printf("for (%s element : this.%s) {%n",
                getBoxedJavaType(arrayType.elementType()), field.camelCaseName());
            buffer.incrementIndent();
            if (elementType.isStruct()) {
                buffer.printf("nestedObjects[i++] = element.toStruct(version);%n");
            } else {
                buffer.printf("nestedObjects[i++] = element;%n");
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
            buffer.printf("struct.set(\"%s\", (Object[]) nestedObjects);%n",
                field.snakeCaseName());
            if (maybeNull) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            buffer.decrementIndent();
            buffer.printf("}%n");
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateClassSize(String className, StructSpec struct,
                                   Versions parentVersions) {
        buffer.printf("@Override%n");
        buffer.printf("public int size(short version) {%n");
        buffer.incrementIndent();
        buffer.printf("int size = 0;%n");
        if (generateInverseVersionCheck(parentVersions, struct.versions())) {
            buffer.incrementIndent();
            headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
            buffer.printf("throw new UnsupportedVersionException(\"Can't size " +
                "version \" + version + \" of %s\");%n", className);
            buffer.decrementIndent();
            buffer.printf("}%n");
        }
        Versions curVersions = parentVersions.intersect(struct.versions());
        if (curVersions.empty()) {
            throw new RuntimeException("Version ranges " + parentVersions +
                " and " + struct.versions() + " have no versions in common.");
        }
        for (FieldSpec field : struct.fields()) {
            generateFieldSize(field, curVersions);
        }
        buffer.printf("return size;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateVariableLengthFieldSize(String fieldName, FieldType type, boolean nullable) {
        if (type instanceof FieldType.StringFieldType) {
            buffer.printf("size += 2;%n");
            if (nullable) {
                buffer.printf("if (%s != null) {%n", fieldName);
                buffer.incrementIndent();
            }
            headerGenerator.addImport(MessageGenerator.MESSAGE_UTIL_CLASS);
            buffer.printf("size += MessageUtil.serializedUtf8Length(%s);%n", fieldName);
            if (nullable) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else if (type instanceof FieldType.BytesFieldType) {
            buffer.printf("size += 4;%n");
            if (nullable) {
                buffer.printf("if (%s != null) {%n", fieldName);
                buffer.incrementIndent();
            }
            buffer.printf("size += %s.length;%n", fieldName);
            if (nullable) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
        } else if (type instanceof FieldType.StructType) {
            buffer.printf("size += %s.size(version);%n", fieldName);
        } else {
            throw new RuntimeException("Unsupported type " + type);
        }
    }

    private void generateFieldSize(FieldSpec field, Versions curVersions) {
        if (field.type().fixedLength().isPresent()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            buffer.printf("size += %d;%n", field.type().fixedLength().get());
            if (maybeAbsent) {
                buffer.decrementIndent();
                generateAbsentValueCheck(field);
            }
        } else if (field.type().isString() || field.type().isBytes() || field.type().isStruct()) {
            boolean nullable = !curVersions.intersect(field.nullableVersions()).empty();
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            generateVariableLengthFieldSize(field.camelCaseName(), field.type(), nullable);
            if (maybeAbsent) {
                buffer.decrementIndent();
                generateAbsentValueCheck(field);
            }
        } else if (field.type().isArray()) {
            boolean maybeAbsent =
                generateVersionCheck(curVersions, field.versions());
            boolean maybeNull = generateNullCheck(curVersions, field);
            if (maybeNull) {
                buffer.printf("size += 4;%n");
                buffer.decrementIndent();
                buffer.printf("} else {%n");
                buffer.incrementIndent();
            }
            buffer.printf("size += 4;%n");
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            FieldType elementType = arrayType.elementType();
            if (elementType.fixedLength().isPresent()) {
                buffer.printf("size += %s.size() * %d;%n",
                    field.camelCaseName(),
                    elementType.fixedLength().get());
            } else if (elementType instanceof FieldType.ArrayType) {
                throw new RuntimeException("Arrays of arrays are not supported " +
                    "(use a struct).");
            } else {
                buffer.printf("for (%s element : %s) {%n",
                    getBoxedJavaType(elementType), field.camelCaseName());
                buffer.incrementIndent();
                generateVariableLengthFieldSize("element", elementType, false);
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            if (maybeNull) {
                buffer.decrementIndent();
                buffer.printf("}%n");
            }
            if (maybeAbsent) {
                buffer.decrementIndent();
                generateAbsentValueCheck(field);
            }
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
        }
    }

    private void generateAbsentValueCheck(FieldSpec field) {
        if (field.ignorable()) {
            buffer.printf("}%n");
            return;
        }
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        if (field.type().isArray()) {
            buffer.printf("if (!%s.isEmpty()) {%n", field.camelCaseName());
        } else if (field.type().isBytes()) {
            buffer.printf("if (%s.length != 0) {%n", field.camelCaseName());
        } else if (field.type().isString()) {
            if (fieldDefault(field).equals("null")) {
                buffer.printf("if (%s != null) {%n", field.camelCaseName());
            } else {
                buffer.printf("if (!%s.equals(%s)) {%n", field.camelCaseName(), fieldDefault(field));
            }
        } else if (field.type() instanceof FieldType.BoolFieldType) {
            buffer.printf("if (%s%s) {%n",
                fieldDefault(field).equals("true") ? "!" : "",
                field.camelCaseName());
        } else {
            buffer.printf("if (%s != %s) {%n", field.camelCaseName(), fieldDefault(field));
        }
        buffer.incrementIndent();
        buffer.printf("throw new UnsupportedVersionException(" +
                "\"Attempted to write a non-default %s at version \" + version);%n",
            field.camelCaseName());
        buffer.decrementIndent();
        buffer.printf("}%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateClassEquals(String className, StructSpec struct, boolean onlyMapKeys) {
        buffer.printf("@Override%n");
        buffer.printf("public boolean equals(Object obj) {%n");
        buffer.incrementIndent();
        buffer.printf("if (!(obj instanceof %s)) return false;%n", className);
        if (!struct.fields().isEmpty()) {
            buffer.printf("%s other = (%s) obj;%n", className, className);
            for (FieldSpec field : struct.fields()) {
                if ((!onlyMapKeys) || field.mapKey()) {
                    generateFieldEquals(field);
                }
            }
        }
        buffer.printf("return true;%n");
        buffer.decrementIndent();
        buffer.printf("}%n");
    }

    private void generateFieldEquals(FieldSpec field) {
        if (field.type().isString() || field.type().isArray() || field.type().isStruct()) {
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
            // Arrays#equals handles nulls.
            headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
            buffer.printf("if (!Arrays.equals(this.%s, other.%s)) return false;%n",
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
        } else if (field.type().isString()) {
            buffer.printf("hashCode = 31 * hashCode + (%s == null ? 0 : %s.hashCode());%n",
                field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
            buffer.printf("hashCode = 31 * hashCode + Arrays.hashCode(%s);%n",
                field.camelCaseName());
        } else if (field.type().isStruct() || field.type().isArray()) {
            buffer.printf("hashCode = 31 * hashCode + (%s == null ? 0 : %s.hashCode());%n",
                field.camelCaseName(), field.camelCaseName());
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
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
                (field.type() instanceof FieldType.Int64FieldType)) {
            buffer.printf("+ \"%s%s=\" + %s%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isString()) {
            buffer.printf("+ \"%s%s='\" + %s + \"'\"%n",
                prefix, field.camelCaseName(), field.camelCaseName());
        } else if (field.type().isBytes()) {
            headerGenerator.addImport(MessageGenerator.ARRAYS_CLASS);
            buffer.printf("+ \"%s%s=\" + Arrays.toString(%s)%n",
                prefix, field.camelCaseName(), field.camelCaseName());
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

    private boolean generateNullCheck(Versions prevVersions, FieldSpec field) {
        if (prevVersions.intersect(field.nullableVersions()).empty()) {
            return false;
        }
        buffer.printf("if (%s == null) {%n", field.camelCaseName());
        buffer.incrementIndent();
        return true;
    }

    private boolean generateVersionCheck(Versions prev, Versions cur) {
        if (cur.lowest() > prev.lowest()) {
            if (cur.highest() < prev.highest()) {
                buffer.printf("if ((version >= %d) && (version <= %d)) {%n",
                    cur.lowest(), cur.highest());
                buffer.incrementIndent();
                return true;
            } else {
                buffer.printf("if (version >= %d) {%n", cur.lowest());
                buffer.incrementIndent();
                return true;
            }
        } else {
            if (cur.highest() < prev.highest()) {
                buffer.printf("if (version <= %d) {%n", cur.highest());
                buffer.incrementIndent();
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean generateInverseVersionCheck(Versions prev, Versions cur) {
        if (cur.lowest() > prev.lowest()) {
            if (cur.highest() < prev.highest()) {
                buffer.printf("if ((version < %d) || (version > %d)) {%n",
                    cur.lowest(), cur.highest());
                return true;
            } else {
                buffer.printf("if (version < %d) {%n", cur.lowest());
                return true;
            }
        } else {
            if (cur.highest() < prev.highest()) {
                buffer.printf("if (version > %d) {%n", cur.highest());
                return true;
            } else {
                return false;
            }
        }
    }

    private void generateSetDefault(FieldSpec field) {
        buffer.decrementIndent();
        buffer.printf("} else {%n");
        buffer.incrementIndent();
        buffer.printf("this.%s = %s;%n", field.camelCaseName(), fieldDefault(field));
        buffer.decrementIndent();
        buffer.printf("}%n");
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
        } else if (field.type() instanceof FieldType.Int8FieldType) {
            if (field.defaultString().isEmpty()) {
                return "(byte) 0";
            } else {
                try {
                    Byte.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int8 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return "(byte) " + field.defaultString();
            }
        } else if (field.type() instanceof FieldType.Int16FieldType) {
            if (field.defaultString().isEmpty()) {
                return "(short) 0";
            } else {
                try {
                    Short.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int16 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return "(short) " + field.defaultString();
            }
        } else if (field.type() instanceof FieldType.Int32FieldType) {
            if (field.defaultString().isEmpty()) {
                return "0";
            } else {
                try {
                    Integer.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int32 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return field.defaultString();
            }
        } else if (field.type() instanceof FieldType.Int64FieldType) {
            if (field.defaultString().isEmpty()) {
                return "0L";
            } else {
                try {
                    Integer.decode(field.defaultString());
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for int64 field " +
                        field.name() + ": " + field.defaultString(), e);
                }
                return field.defaultString() + "L";
            }
        } else if (field.type() instanceof FieldType.StringFieldType) {
            if (field.defaultString().equals("null")) {
                if (!(field.nullableVersions().contains(field.versions()))) {
                    throw new RuntimeException("null cannot be the default for field " +
                        field.name() + ", because not all versions of this field are " +
                        "nullable.");
                }
                return "null";
            } else {
                return "\"" + field.defaultString() + "\"";
            }
        } else if (field.type().isBytes()) {
            if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for bytes field " +
                    field.name() + ": custom defaults are not supported for bytes fields.");
            }
            headerGenerator.addImport(MessageGenerator.BYTES_CLASS);
            return "Bytes.EMPTY";
        } else if (field.type().isStruct()) {
            if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for struct field " +
                    field.name() + ": custom defaults are not supported for struct fields.");
            }
            return "new " + field.type().toString() + "()";
        } else if (field.type().isArray()) {
            if (!field.defaultString().isEmpty()) {
                throw new RuntimeException("Invalid default for array field " +
                    field.name() + ": custom defaults are not supported for array fields.");
            }
            FieldType.ArrayType arrayType = (FieldType.ArrayType) field.type();
            if (field.toStruct().hasKeys()) {
                return "new " + collectionType(arrayType.elementType().toString()) + "(0)";
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                return "new ArrayList<" + getBoxedJavaType(arrayType.elementType()) + ">()";
            }
        } else {
            throw new RuntimeException("Unsupported field type " + field.type());
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
}
