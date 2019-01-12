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

public final class FieldSpec {
    private final String name;

    private final Versions versions;

    private final List<FieldSpec> fields;

    private final FieldType type;

    private final boolean mapKey;

    private final Versions nullableVersions;

    private final String fieldDefault;

    private final boolean ignorable;

    private final String about;

    @JsonCreator
    public FieldSpec(@JsonProperty("name") String name,
                     @JsonProperty("versions") String versions,
                     @JsonProperty("fields") List<FieldSpec> fields,
                     @JsonProperty("type") String type,
                     @JsonProperty("mapKey") boolean mapKey,
                     @JsonProperty("nullableVersions") String nullableVersions,
                     @JsonProperty("default") String fieldDefault,
                     @JsonProperty("ignorable") boolean ignorable,
                     @JsonProperty("about") String about) {
        this.name = Objects.requireNonNull(name);
        this.versions = Versions.parse(versions, null);
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
        this.about = about == null ? "" : about;
        if (!this.fields().isEmpty()) {
            if (!this.type.isArray()) {
                throw new RuntimeException("Non-array field " + name + " cannot have fields");
            }
        }
    }

    public StructSpec toStruct() {
        if ((!this.type.isArray()) && (this.type.isStruct())) {
            throw new RuntimeException("Field " + name + " cannot be treated as a structure.");
        }
        return new StructSpec(name, versions.toString(), fields);
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

    @JsonProperty("about")
    public String about() {
        return about;
    }
}
