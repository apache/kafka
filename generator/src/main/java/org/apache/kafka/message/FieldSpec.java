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

import java.util.ArrayList;
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

    private boolean zeroCopy;

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
            if (!this.type.isArray()) {
                throw new RuntimeException("Non-array field " + name + " cannot have fields");
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
}
