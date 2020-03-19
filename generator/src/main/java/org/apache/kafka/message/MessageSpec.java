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

public final class MessageSpec extends DeclarationSpec {

    private final StructSpec struct;

    private final Optional<Short> apiKey;

    private final MessageSpecType type;

    private final List<StructSpec> commonStructs;

    private final Versions flexibleVersions;

    @JsonCreator
    public MessageSpec(@JsonProperty("name") String name,
                       @JsonProperty("validVersions") String validVersions,
                       @JsonProperty("fields") List<FieldSpec> fields,
                       @JsonProperty("apiKey") Short apiKey,
                       @JsonProperty("type") MessageSpecType type,
                       @JsonProperty("commonStructs") List<StructSpec> commonStructs,
                       @JsonProperty("flexibleVersions") String flexibleVersions) {
        this.struct = new StructSpec(name, validVersions, fields);
        this.apiKey = apiKey == null ? Optional.empty() : Optional.of(apiKey);
        this.type = Objects.requireNonNull(type);
        this.commonStructs = commonStructs == null ? Collections.emptyList() :
                Collections.unmodifiableList(new ArrayList<>(commonStructs));
        this.flexibleVersions = Versions.parse(flexibleVersions, Versions.NONE);
        if ((!this.flexibleVersions().empty()) &&
                (this.flexibleVersions.highest() < Short.MAX_VALUE)) {
            throw new RuntimeException("Field " + name + " specifies flexibleVersions " +
                this.flexibleVersions + ", which is not open-ended.  flexibleVersions must " +
                "be either none, or an open-ended range (that ends with a plus sign).");
        }
    }

    public StructSpec struct() {
        return struct;
    }

    @JsonProperty("name")
    public String name() {
        return struct.name();
    }

    public Versions validVersions() {
        return struct.versions();
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

    @JsonProperty("commonStructs")
    public List<StructSpec> commonStructs() {
        return commonStructs;
    }

    public Versions flexibleVersions() {
        return flexibleVersions;
    }

    @JsonProperty("flexibleVersions")
    public String flexibleVersionsString() {
        return flexibleVersions.toString();
    }

    @Override
    public String generatedClassName() {
        return struct.name() + "Data";
    }
}
