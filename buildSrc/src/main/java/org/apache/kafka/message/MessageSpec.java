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
import java.util.Optional;

public final class MessageSpec {
    private final StructSpec struct;

    private final Optional<Short> apiKey;

    private final MessageSpecType type;

    @JsonCreator
    public MessageSpec(@JsonProperty("name") String name,
                       @JsonProperty("validVersions") String validVersions,
                       @JsonProperty("fields") List<FieldSpec> fields,
                       @JsonProperty("apiKey") Short apiKey,
                       @JsonProperty("type") MessageSpecType type) {
        this.struct = new StructSpec(name, validVersions, fields);
        this.apiKey = apiKey == null ? Optional.empty() : Optional.of(apiKey);
        this.type = Objects.requireNonNull(type);
    }

    public StructSpec struct() {
        return struct;
    }

    @JsonProperty("name")
    public String name() {
        return struct.name();
    }

    @JsonProperty("validVersions")
    public String validVersionsString() {
        return struct.versionsString();
    }

    @JsonProperty("fields")
    public List<FieldSpec> fields() {
        return struct.fields();
    }

    @JsonProperty("apiKey")
    public Optional<Short> apiKey() {
        return apiKey;
    }

    @JsonProperty("type")
    public MessageSpecType type() {
        return type;
    }

    public String generatedClassName() {
        return struct.name() + "Data";
    }
}
