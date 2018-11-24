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

import java.util.List;
import java.util.Objects;

public final class FieldSpec {
    private final StructSpec struct;

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
        this.struct = new StructSpec(name, versions, fields);
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
        if (!this.struct.fields().isEmpty()) {
            if (!this.type.isArray()) {
                throw new RuntimeException("Non-array field " + name + " cannot have fields");
            }
        }
    }

    public StructSpec struct() {
        return struct;
    }

    @JsonProperty("name")
    public String name() {
        return struct.name();
    }

    String capitalizedCamelCaseName() {
        return MessageGenerator.capitalizeFirst(struct.name());
    }

    String camelCaseName() {
        return MessageGenerator.lowerCaseFirst(struct.name());
    }

    String snakeCaseName() {
        return MessageGenerator.toSnakeCase(struct.name());
    }

    @JsonProperty("versions")
    public String versionsString() {
        return struct.versionsString();
    }

    @JsonProperty("fields")
    public List<FieldSpec> fields() {
        return struct.fields();
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

    boolean hasKeys() {
        return struct.hasKeys();
    }

    @JsonProperty("ignorable")
    public boolean ignorable() {
        return ignorable;
    }

    @JsonProperty("about")
    public String about() {
        return about;
    }
};
