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

import java.util.Optional;

public interface FieldType {
    String ARRAY_PREFIX = "[]";

    final class BoolFieldType implements FieldType {
        static final BoolFieldType INSTANCE = new BoolFieldType();
        private static final String NAME = "bool";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Boolean";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(1);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Int8FieldType implements FieldType {
        static final Int8FieldType INSTANCE = new Int8FieldType();
        private static final String NAME = "int8";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Byte";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(1);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Int16FieldType implements FieldType {
        static final Int16FieldType INSTANCE = new Int16FieldType();
        private static final String NAME = "int16";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Short";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(2);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Uint16FieldType implements FieldType {
        static final Uint16FieldType INSTANCE = new Uint16FieldType();
        private static final String NAME = "uint16";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Integer";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(2);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Int32FieldType implements FieldType {
        static final Int32FieldType INSTANCE = new Int32FieldType();
        private static final String NAME = "int32";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Integer";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(4);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Uint32FieldType implements FieldType {
        static final Uint32FieldType INSTANCE = new Uint32FieldType();
        private static final String NAME = "uint32";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Long";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(4);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Int64FieldType implements FieldType {
        static final Int64FieldType INSTANCE = new Int64FieldType();
        private static final String NAME = "int64";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Long";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(8);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class UUIDFieldType implements FieldType {
        static final UUIDFieldType INSTANCE = new UUIDFieldType();
        private static final String NAME = "uuid";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            return "Uuid";
        }

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(16);
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class Float64FieldType implements FieldType {
        static final Float64FieldType INSTANCE = new Float64FieldType();
        private static final String NAME = "float64";

        @Override
        public Optional<Integer> fixedLength() {
            return Optional.of(8);
        }

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "Double";
        }

        @Override
        public boolean isFloat() {
            return true;
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class StringFieldType implements FieldType {
        static final StringFieldType INSTANCE = new StringFieldType();
        private static final String NAME = "string";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return "String";
        }

        @Override
        public boolean serializationIsDifferentInFlexibleVersions() {
            return true;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public boolean canBeNullable() {
            return true;
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class BytesFieldType implements FieldType {
        static final BytesFieldType INSTANCE = new BytesFieldType();
        private static final String NAME = "bytes";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
            return "ByteBuffer";
        }

        @Override
        public boolean serializationIsDifferentInFlexibleVersions() {
            return true;
        }

        @Override
        public boolean isBytes() {
            return true;
        }

        @Override
        public boolean canBeNullable() {
            return true;
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class RecordsFieldType implements FieldType {
        static final RecordsFieldType INSTANCE = new RecordsFieldType();
        private static final String NAME = "records";

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            headerGenerator.addImport(MessageGenerator.BASE_RECORDS_CLASS);
            return "BaseRecords";
        }

        @Override
        public boolean serializationIsDifferentInFlexibleVersions() {
            return true;
        }

        @Override
        public boolean isRecords() {
            return true;
        }

        @Override
        public boolean canBeNullable() {
            return true;
        }

        @Override
        public String toString() {
            return NAME;
        }
    }

    final class StructType implements FieldType {
        private final String type;

        StructType(String type) {
            this.type = type;
        }

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            return type;
        }

        @Override
        public boolean serializationIsDifferentInFlexibleVersions() {
            return true;
        }

        @Override
        public boolean isStruct() {
            return true;
        }

        @Override
        public boolean canBeNullable() {
            return true;
        }

        public String typeName() {
            return type;
        }

        @Override
        public String toString() {
            return type;
        }
    }

    final class ArrayType implements FieldType {
        private final FieldType elementType;

        ArrayType(FieldType elementType) {
            this.elementType = elementType;
        }

        @Override
        public boolean serializationIsDifferentInFlexibleVersions() {
            return true;
        }

        @Override
        public String getBoxedJavaType(HeaderGenerator headerGenerator) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isArray() {
            return true;
        }

        @Override
        public boolean isStructArray() {
            return elementType.isStruct();
        }

        @Override
        public boolean canBeNullable() {
            return true;
        }

        public FieldType elementType() {
            return elementType;
        }

        public String elementName() {
            return elementType.toString();
        }

        @Override
        public String toString() {
            return "[]" + elementType.toString();
        }
    }

    static FieldType parse(String string) {
        string = string.trim();
        switch (string) {
            case BoolFieldType.NAME:
                return BoolFieldType.INSTANCE;
            case Int8FieldType.NAME:
                return Int8FieldType.INSTANCE;
            case Int16FieldType.NAME:
                return Int16FieldType.INSTANCE;
            case Uint16FieldType.NAME:
                return Uint16FieldType.INSTANCE;
            case Uint32FieldType.NAME:
                return Uint32FieldType.INSTANCE;
            case Int32FieldType.NAME:
                return Int32FieldType.INSTANCE;
            case Int64FieldType.NAME:
                return Int64FieldType.INSTANCE;
            case UUIDFieldType.NAME:
                return UUIDFieldType.INSTANCE;
            case Float64FieldType.NAME:
                return Float64FieldType.INSTANCE;
            case StringFieldType.NAME:
                return StringFieldType.INSTANCE;
            case BytesFieldType.NAME:
                return BytesFieldType.INSTANCE;
            case RecordsFieldType.NAME:
                return RecordsFieldType.INSTANCE;
            default:
                if (string.startsWith(ARRAY_PREFIX)) {
                    String elementTypeString = string.substring(ARRAY_PREFIX.length());
                    if (elementTypeString.isEmpty()) {
                        throw new RuntimeException("Can't parse array type " + string +
                            ".  No element type found.");
                    }
                    FieldType elementType = parse(elementTypeString);
                    if (elementType.isArray()) {
                        throw new RuntimeException("Can't have an array of arrays.  " +
                            "Use an array of structs containing an array instead.");
                    }
                    return new ArrayType(elementType);
                } else if (MessageGenerator.firstIsCapitalized(string)) {
                    return new StructType(string);
                } else {
                    throw new RuntimeException("Can't parse type " + string);
                }
        }
    }

    String getBoxedJavaType(HeaderGenerator headerGenerator);

    /**
     * Returns true if this is an array type.
     */
    default boolean isArray() {
        return false;
    }

    /**
     * Returns true if this is an array of structures.
     */
    default boolean isStructArray() {
        return false;
    }

    /**
     * Returns true if the serialization of this type is different in flexible versions.
     */
    default boolean serializationIsDifferentInFlexibleVersions() {
        return false;
    }

    /**
     * Returns true if this is a string type.
     */
    default boolean isString() {
        return false;
    }

    /**
     * Returns true if this is a bytes type.
     */
    default boolean isBytes() {
        return false;
    }

    /**
     * Returns true if this is a records type
     */
    default boolean isRecords() {
        return false;
    }

    /**
     * Returns true if this is a floating point type.
     */
    default boolean isFloat() {
        return false;
    }

    /**
     * Returns true if this is a struct type.
     */
    default boolean isStruct() {
        return false;
    }

    /**
     * Returns true if this field type is compatible with nullability.
     */
    default boolean canBeNullable() {
        return false;
    }

    /**
     * Gets the fixed length of the field, or Empty if the field is variable-length.
     */
    default Optional<Integer> fixedLength() {
        return Optional.empty();
    }

    default boolean isVariableLength() {
        return fixedLength().isEmpty();
    }

    /**
     * Convert the field type to a JSON string.
     */
    String toString();
}
