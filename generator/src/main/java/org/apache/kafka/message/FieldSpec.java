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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;

public final class FieldSpec {
    private static final Pattern VALID_FIELD_NAMES = Pattern.compile("[A-Za-z]([A-Za-z0-9]*)");

    private final String name;

    private final Versions versions;

    private final List<FieldSpec> fields;

    private final FieldType type;

    private final boolean mapKey;

    private final Versions nullableVersions;

    private final String fieldDefault;

    private final boolean ignorable;

    private final EntityType entityType;

    private final String about;

    private final Versions taggedVersions;

    private final Optional<Versions> flexibleVersions;

    private final Optional<Integer> tag;

    private final boolean zeroCopy;

    @JsonCreator
    public FieldSpec(@JsonProperty("name") String name,
                     @JsonProperty("versions") String versions,
                     @JsonProperty("fields") List<FieldSpec> fields,
                     @JsonProperty("type") String type,
                     @JsonProperty("mapKey") boolean mapKey,
                     @JsonProperty("nullableVersions") String nullableVersions,
                     @JsonProperty("default") String fieldDefault,
                     @JsonProperty("ignorable") boolean ignorable,
                     @JsonProperty("entityType") EntityType entityType,
                     @JsonProperty("about") String about,
                     @JsonProperty("taggedVersions") String taggedVersions,
                     @JsonProperty("flexibleVersions") String flexibleVersions,
                     @JsonProperty("tag") Integer tag,
                     @JsonProperty("zeroCopy") boolean zeroCopy) {
        this.name = Objects.requireNonNull(name);
        if (!VALID_FIELD_NAMES.matcher(this.name).matches()) {
            throw new RuntimeException("Invalid field name " + this.name);
        }
        this.taggedVersions = Versions.parse(taggedVersions, Versions.NONE);
        // If versions is not set, but taggedVersions is, default to taggedVersions.
        this.versions = Versions.parse(versions, this.taggedVersions.empty() ?
            null : this.taggedVersions);
        if (this.versions == null) {
            throw new RuntimeException("You must specify the version of the " +
                name + " structure.");
        }
        this.fields = Collections.unmodifiableList(fields == null ?
            Collections.emptyList() : new ArrayList<>(fields));
        this.type = FieldType.parse(Objects.requireNonNull(type));
        this.mapKey = mapKey;
        this.nullableVersions = Versions.parse(nullableVersions, Versions.NONE);
        if (!this.nullableVersions.empty()) {
            if (!this.type.canBeNullable()) {
                throw new RuntimeException("Type " + this.type + " cannot be nullable.");
            }
        }
        this.fieldDefault = fieldDefault == null ? "" : fieldDefault;
        this.ignorable = ignorable;
        this.entityType = (entityType == null) ? EntityType.UNKNOWN : entityType;
        this.entityType.verifyTypeMatches(name, this.type);

        this.about = about == null ? "" : about;
        if (!this.fields().isEmpty()) {
            if (!this.type.isArray() && !this.type.isStruct()) {
                throw new RuntimeException("Non-array or Struct field " + name + " cannot have fields");
            }
            // Check struct invariants
            if (this.type.isStruct() || this.type.isStructArray()) {
                new StructSpec(name,
                    versions,
                    Versions.NONE_STRING, // version deprecations not supported at field level
                    fields);
            }
        }

        if (flexibleVersions == null || flexibleVersions.isEmpty()) {
            this.flexibleVersions = Optional.empty();
        } else {
            this.flexibleVersions = Optional.of(Versions.parse(flexibleVersions, null));
            if (!(this.type.isString() || this.type.isBytes())) {
                // For now, only allow flexibleVersions overrides for the string and bytes
                // types.  Overrides are only needed to keep compatibility with some old formats,
                // so there isn't any need to support them for all types.
                throw new RuntimeException("Invalid flexibleVersions override for " + name +
                    ".  Only fields of type string or bytes can specify a flexibleVersions " +
                    "override.");
            }
        }
        this.tag = Optional.ofNullable(tag);
        if (this.tag.isPresent() && mapKey) {
            throw new RuntimeException("Tagged fields cannot be used as keys.");
        }
        checkTagInvariants();

        this.zeroCopy = zeroCopy;
        if (this.zeroCopy && !this.type.isBytes()) {
            throw new RuntimeException("Invalid zeroCopy value for " + name +
                ". Only fields of type bytes can use zeroCopy flag.");
        }
    }

    private void checkTagInvariants() {
        if (this.tag.isPresent()) {
            if (this.tag.get() < 0) {
                throw new RuntimeException("Field " + name + " specifies a tag of " + this.tag.get() +
                    ".  Tags cannot be negative.");
            }
            if (this.taggedVersions.empty()) {
                throw new RuntimeException("Field " + name + " specifies a tag of " + this.tag.get() +
                    ", but has no tagged versions.  If a tag is specified, taggedVersions must " +
                    "be specified as well.");
            }
            Versions nullableTaggedVersions = this.nullableVersions.intersect(this.taggedVersions);
            if (!(nullableTaggedVersions.empty() || nullableTaggedVersions.equals(this.taggedVersions))) {
                throw new RuntimeException("Field " + name + " specifies nullableVersions " +
                    this.nullableVersions + " and taggedVersions " + this.taggedVersions + ".  " +
                    "Either all tagged versions must be nullable, or none must be.");
            }
            if (this.taggedVersions.highest() < Short.MAX_VALUE) {
                throw new RuntimeException("Field " + name + " specifies taggedVersions " +
                    this.taggedVersions + ", which is not open-ended.  taggedVersions must " +
                    "be either none, or an open-ended range (that ends with a plus sign).");
            }
            if (!this.taggedVersions.intersect(this.versions).equals(this.taggedVersions)) {
                throw new RuntimeException("Field " + name + " specifies taggedVersions " +
                    this.taggedVersions + ", and versions " + this.versions + ".  " +
                    "taggedVersions must be a subset of versions.");
            }
        } else if (!this.taggedVersions.empty()) {
            throw new RuntimeException("Field " + name + " does not specify a tag, " +
                "but specifies tagged versions of " + this.taggedVersions + ".  " +
                "Please specify a tag, or remove the taggedVersions.");
        }
    }

    @JsonProperty("name")
    public String name() {
        return name;
    }

    String capitalizedCamelCaseName() {
        return MessageGenerator.capitalizeFirst(name);
    }

    String camelCaseName() {
        return MessageGenerator.lowerCaseFirst(name);
    }

    String snakeCaseName() {
        return MessageGenerator.toSnakeCase(name);
    }

    public Versions versions() {
        return versions;
    }

    @JsonProperty("versions")
    public String versionsString() {
        return versions.toString();
    }

    @JsonProperty("fields")
    public List<FieldSpec> fields() {
        return fields;
    }

    @JsonProperty("type")
    public String typeString() {
        return type.toString();
    }

    public FieldType type() {
        return type;
    }

    @JsonProperty("mapKey")
    public boolean mapKey() {
        return mapKey;
    }

    public Versions nullableVersions() {
        return nullableVersions;
    }

    @JsonProperty("nullableVersions")
    public String nullableVersionsString() {
        return nullableVersions.toString();
    }

    @JsonProperty("default")
    public String defaultString() {
        return fieldDefault;
    }

    @JsonProperty("ignorable")
    public boolean ignorable() {
        return ignorable;
    }

    @JsonProperty("entityType")
    public EntityType entityType() {
        return entityType;
    }

    @JsonProperty("about")
    public String about() {
        return about;
    }

    @JsonProperty("taggedVersions")
    public String taggedVersionsString() {
        return taggedVersions.toString();
    }

    public Versions taggedVersions() {
        return taggedVersions;
    }

    @JsonProperty("flexibleVersions")
    public String flexibleVersionsString() {
        return flexibleVersions.isPresent() ? flexibleVersions.get().toString() : null;
    }

    public Optional<Versions> flexibleVersions() {
        return flexibleVersions;
    }

    @JsonProperty("tag")
    public Integer tagInteger() {
        return tag.orElse(null);
    }

    public Optional<Integer> tag() {
        return tag;
    }

    @JsonProperty("zeroCopy")
    public boolean zeroCopy() {
        return zeroCopy;
    }

    /**
     * Get a string representation of the field default.
     *
     * @param headerGenerator   The header generator in case we need to add imports.
     * @param structRegistry    The struct registry in case we need to look up structs.
     *
     * @return                  A string that can be used for the field default in the
     *                          generated code.
     */
    String fieldDefault(HeaderGenerator headerGenerator,
                        StructRegistry structRegistry) {
        if (type instanceof FieldType.BoolFieldType) {
            if (fieldDefault.isEmpty()) {
                return "false";
            } else if (fieldDefault.equalsIgnoreCase("true")) {
                return "true";
            } else if (fieldDefault.equalsIgnoreCase("false")) {
                return "false";
            } else {
                throw new RuntimeException("Invalid default for boolean field " +
                    name + ": " + fieldDefault);
            }
        } else if ((type instanceof FieldType.Int8FieldType) ||
            (type instanceof FieldType.Int16FieldType) ||
            (type instanceof FieldType.Uint16FieldType) ||
            (type instanceof FieldType.Uint32FieldType) ||
            (type instanceof FieldType.Int32FieldType) ||
            (type instanceof FieldType.Int64FieldType)) {
            int base = 10;
            String defaultString = fieldDefault;
            if (defaultString.startsWith("0x")) {
                base = 16;
                defaultString = defaultString.substring(2);
            }
            if (type instanceof FieldType.Int8FieldType) {
                if (defaultString.isEmpty()) {
                    return "(byte) 0";
                } else {
                    try {
                        Byte.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int8 field " +
                            name + ": " + defaultString, e);
                    }
                    return "(byte) " + fieldDefault;
                }
            } else if (type instanceof FieldType.Int16FieldType) {
                if (defaultString.isEmpty()) {
                    return "(short) 0";
                } else {
                    try {
                        Short.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int16 field " +
                            name + ": " + defaultString, e);
                    }
                    return "(short) " + fieldDefault;
                }
            } else if (type instanceof FieldType.Uint16FieldType) {
                if (defaultString.isEmpty()) {
                    return "0";
                } else {
                    try {
                        int value = Integer.valueOf(defaultString, base);
                        if (value < 0 || value > MessageGenerator.UNSIGNED_SHORT_MAX) {
                            throw new RuntimeException("Invalid default for uint16 field " +
                                    name + ": out of range.");
                        }
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for uint16 field " +
                            name + ": " + defaultString, e);
                    }
                    return fieldDefault;
                }
            } else if (type instanceof FieldType.Uint32FieldType) {
                if (defaultString.isEmpty()) {
                    return "0";
                } else {
                    try {
                        long value = Long.valueOf(defaultString, base);
                        if (value < 0 || value > MessageGenerator.UNSIGNED_INT_MAX) {
                            throw new RuntimeException("Invalid default for uint32 field " +
                                    name + ": out of range.");
                        }
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for uint32 field " +
                                name + ": " + defaultString, e);
                    }
                    return fieldDefault;
                }
            } else if (type instanceof FieldType.Int32FieldType) {
                if (defaultString.isEmpty()) {
                    return "0";
                } else {
                    try {
                        Integer.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int32 field " +
                            name + ": " + defaultString, e);
                    }
                    return fieldDefault;
                }
            } else if (type instanceof FieldType.Int64FieldType) {
                if (defaultString.isEmpty()) {
                    return "0L";
                } else {
                    try {
                        Long.valueOf(defaultString, base);
                    } catch (NumberFormatException e) {
                        throw new RuntimeException("Invalid default for int64 field " +
                            name + ": " + defaultString, e);
                    }
                    return fieldDefault + "L";
                }
            } else {
                throw new RuntimeException("Unsupported field type " + type);
            }
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            if (fieldDefault.isEmpty()) {
                return "Uuid.ZERO_UUID";
            } else {
                try {
                    ByteBuffer uuidBytes = ByteBuffer.wrap(Base64.getUrlDecoder().decode(fieldDefault));
                    uuidBytes.getLong();
                    uuidBytes.getLong();
                } catch (IllegalArgumentException e) {
                    throw new RuntimeException("Invalid default for uuid field " +
                        name + ": " + fieldDefault, e);
                }
                headerGenerator.addImport(MessageGenerator.UUID_CLASS);
                return "Uuid.fromString(\"" + fieldDefault + "\")";
            }
        } else if (type instanceof FieldType.Float64FieldType) {
            if (fieldDefault.isEmpty()) {
                return "0.0";
            } else {
                try {
                    Double.parseDouble(fieldDefault);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Invalid default for float64 field " +
                        name + ": " + fieldDefault, e);
                }
                return "Double.parseDouble(\"" + fieldDefault + "\")";
            }
        } else if (type instanceof FieldType.StringFieldType) {
            if (fieldDefault.equals("null")) {
                validateNullDefault();
                return "null";
            } else {
                return "\"" + fieldDefault + "\"";
            }
        } else if (type.isBytes()) {
            if (fieldDefault.equals("null")) {
                validateNullDefault();
                return "null";
            } else if (!fieldDefault.isEmpty()) {
                throw new RuntimeException("Invalid default for bytes field " +
                    name + ".  The only valid default for a bytes field " +
                    "is empty or null.");
            }
            if (zeroCopy) {
                headerGenerator.addImport(MessageGenerator.BYTE_UTILS_CLASS);
                return "ByteUtils.EMPTY_BUF";
            } else {
                headerGenerator.addImport(MessageGenerator.BYTES_CLASS);
                return "Bytes.EMPTY";
            }
        } else if (type.isRecords()) {
            return "null";
        } else if (type.isStruct()) {
            if (fieldDefault.equals("null")) {
                validateNullDefault();
                return "null";
            } else if (!fieldDefault.isEmpty()) {
                throw new RuntimeException("Invalid default for struct field " +
                    name + ".  The only valid default for a struct field " +
                    "is the empty struct or null.");
            }
            return "new " + type + "()";
        } else if (type.isArray()) {
            if (fieldDefault.equals("null")) {
                validateNullDefault();
                return "null";
            } else if (!fieldDefault.isEmpty()) {
                throw new RuntimeException("Invalid default for array field " +
                    name + ".  The only valid default for an array field " +
                    "is the empty array or null.");
            }
            return String.format("new %s(0)",
                concreteJavaType(headerGenerator, structRegistry));
        } else {
            throw new RuntimeException("Unsupported field type " + type);
        }
    }

    private void validateNullDefault() {
        if (!(nullableVersions().contains(versions))) {
            throw new RuntimeException("null cannot be the default for field " +
                name + ", because not all versions of this field are " +
                "nullable.");
        }
    }

    /**
     * Get the abstract Java type of the field-- for example, List.
     *
     * @param headerGenerator   The header generator in case we need to add imports.
     * @param structRegistry    The struct registry in case we need to look up structs.
     *
     * @return                  The abstract java type name.
     */
    String fieldAbstractJavaType(HeaderGenerator headerGenerator,
                                 StructRegistry structRegistry) {
        if (type instanceof FieldType.BoolFieldType) {
            return "boolean";
        } else if (type instanceof FieldType.Int8FieldType) {
            return "byte";
        } else if (type instanceof FieldType.Int16FieldType) {
            return "short";
        } else if (type instanceof FieldType.Uint16FieldType) {
            return "int";
        } else if (type instanceof FieldType.Uint32FieldType) {
            return "long";
        } else if (type instanceof FieldType.Int32FieldType) {
            return "int";
        } else if (type instanceof FieldType.Int64FieldType) {
            return "long";
        } else if (type instanceof FieldType.UUIDFieldType) {
            headerGenerator.addImport(MessageGenerator.UUID_CLASS);
            return "Uuid";
        } else if (type instanceof FieldType.Float64FieldType) {
            return "double";
        } else if (type.isString()) {
            return "String";
        } else if (type.isBytes()) {
            if (zeroCopy) {
                headerGenerator.addImport(MessageGenerator.BYTE_BUFFER_CLASS);
                return "ByteBuffer";
            } else {
                return "byte[]";
            }
        } else if (type instanceof FieldType.RecordsFieldType) {
            headerGenerator.addImport(MessageGenerator.BASE_RECORDS_CLASS);
            return "BaseRecords";
        } else if (type.isStruct()) {
            return MessageGenerator.capitalizeFirst(typeString());
        } else if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            if (structRegistry.isStructArrayWithKeys(this)) {
                headerGenerator.addImport(MessageGenerator.IMPLICIT_LINKED_HASH_MULTI_COLLECTION_CLASS);
                return collectionType(arrayType.elementType().toString());
            } else {
                headerGenerator.addImport(MessageGenerator.LIST_CLASS);
                return String.format("List<%s>",
                    arrayType.elementType().getBoxedJavaType(headerGenerator));
            }
        } else {
            throw new RuntimeException("Unknown field type " + type);
        }
    }

    /**
     * Get the concrete Java type of the field-- for example, ArrayList.
     *
     * @param headerGenerator   The header generator in case we need to add imports.
     * @param structRegistry    The struct registry in case we need to look up structs.
     *
     * @return                  The abstract java type name.
     */
    String concreteJavaType(HeaderGenerator headerGenerator,
                            StructRegistry structRegistry) {
        if (type.isArray()) {
            FieldType.ArrayType arrayType = (FieldType.ArrayType) type;
            if (structRegistry.isStructArrayWithKeys(this)) {
                return collectionType(arrayType.elementType().toString());
            } else {
                headerGenerator.addImport(MessageGenerator.ARRAYLIST_CLASS);
                return String.format("ArrayList<%s>",
                    arrayType.elementType().getBoxedJavaType(headerGenerator));
            }
        } else {
            return fieldAbstractJavaType(headerGenerator, structRegistry);
        }
    }

    static String collectionType(String baseType) {
        return baseType + "Collection";
    }

    /**
     * Generate an if statement that checks if this field has a non-default value.
     *
     * @param headerGenerator   The header generator in case we need to add imports.
     * @param structRegistry    The struct registry in case we need to look up structs.
     * @param buffer            The code buffer to write to.
     * @param fieldPrefix       The prefix to prepend before references to this field.
     * @param nullableVersions  The nullable versions to use for this field.  This is
     *                          mainly to let us choose to ignore the possibility of
     *                          nulls sometimes (like when dealing with array entries
     *                          that cannot be null).
     */
    void generateNonDefaultValueCheck(HeaderGenerator headerGenerator,
                                      StructRegistry structRegistry,
                                      CodeBuffer buffer,
                                      String fieldPrefix,
                                      Versions nullableVersions) {
        String fieldDefault = fieldDefault(headerGenerator, structRegistry);
        if (type().isArray()) {
            if (fieldDefault.equals("null")) {
                buffer.printf("if (%s%s != null) {%n", fieldPrefix, camelCaseName());
            } else if (nullableVersions.empty()) {
                buffer.printf("if (!%s%s.isEmpty()) {%n", fieldPrefix, camelCaseName());
            } else {
                buffer.printf("if (%s%s == null || !%s%s.isEmpty()) {%n",
                    fieldPrefix, camelCaseName(), fieldPrefix, camelCaseName());
            }
        } else if (type().isBytes()) {
            if (fieldDefault.equals("null")) {
                buffer.printf("if (%s%s != null) {%n", fieldPrefix, camelCaseName());
            } else if (nullableVersions.empty()) {
                if (zeroCopy()) {
                    buffer.printf("if (%s%s.hasRemaining()) {%n",
                        fieldPrefix, camelCaseName());
                } else {
                    buffer.printf("if (%s%s.length != 0) {%n",
                        fieldPrefix, camelCaseName());
                }
            } else {
                if (zeroCopy()) {
                    buffer.printf("if (%s%s == null || %s%s.remaining() > 0) {%n",
                        fieldPrefix, camelCaseName(), fieldPrefix, camelCaseName());
                } else {
                    buffer.printf("if (%s%s == null || %s%s.length != 0) {%n",
                        fieldPrefix, camelCaseName(), fieldPrefix, camelCaseName());
                }
            }
        } else if (type().isString() || type().isStruct() || type() instanceof FieldType.UUIDFieldType) {
            if (fieldDefault.equals("null")) {
                buffer.printf("if (%s%s != null) {%n", fieldPrefix, camelCaseName());
            } else if (nullableVersions.empty()) {
                buffer.printf("if (!%s%s.equals(%s)) {%n",
                    fieldPrefix, camelCaseName(), fieldDefault);
            } else {
                buffer.printf("if (%s%s == null || !%s%s.equals(%s)) {%n",
                    fieldPrefix, camelCaseName(), fieldPrefix, camelCaseName(),
                    fieldDefault);
            }
        } else if (type() instanceof FieldType.BoolFieldType) {
            buffer.printf("if (%s%s%s) {%n",
                fieldDefault.equals("true") ? "!" : "",
                fieldPrefix, camelCaseName());
        } else {
            buffer.printf("if (%s%s != %s) {%n",
                fieldPrefix, camelCaseName(), fieldDefault);
        }
    }

    /**
     * Generate an if statement that checks if this field is non-default and also
     * non-ignorable.
     *
     * @param headerGenerator   The header generator in case we need to add imports.
     * @param structRegistry    The struct registry in case we need to look up structs.
     * @param fieldPrefix       The prefix to prepend before references to this field.
     * @param buffer            The code buffer to write to.
     */
    void generateNonIgnorableFieldCheck(HeaderGenerator headerGenerator,
                                        StructRegistry structRegistry,
                                        String fieldPrefix,
                                        CodeBuffer buffer) {
        generateNonDefaultValueCheck(headerGenerator, structRegistry,
            buffer, fieldPrefix, nullableVersions());
        buffer.incrementIndent();
        headerGenerator.addImport(MessageGenerator.UNSUPPORTED_VERSION_EXCEPTION_CLASS);
        buffer.printf("throw new UnsupportedVersionException(" +
                "\"Attempted to write a non-default %s at version \" + _version);%n",
            camelCaseName());
        buffer.decrementIndent();
        buffer.printf("}%n");
    }
}
