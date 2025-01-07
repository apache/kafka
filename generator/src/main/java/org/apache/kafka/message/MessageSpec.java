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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public final class MessageSpec {
    private final StructSpec struct;

    private final Optional<Short> apiKey;

    private final MessageSpecType type;

    private final List<StructSpec> commonStructs;

    private final Versions flexibleVersions;

    private final List<RequestListenerType> listeners;

    private final boolean latestVersionUnstable;

    @JsonCreator
    @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
    public MessageSpec(@JsonProperty("name") String name,
                       @JsonProperty("validVersions") String validVersions,
                       @JsonProperty("deprecatedVersions") String deprecatedVersions,
                       @JsonProperty("fields") List<FieldSpec> fields,
                       @JsonProperty("apiKey") Short apiKey,
                       @JsonProperty("type") MessageSpecType type,
                       @JsonProperty("commonStructs") List<StructSpec> commonStructs,
                       @JsonProperty("flexibleVersions") String flexibleVersions,
                       @JsonProperty("listeners") List<RequestListenerType> listeners,
                       @JsonProperty("latestVersionUnstable") boolean latestVersionUnstable
    ) {
        this.struct = new StructSpec(name, validVersions, deprecatedVersions, fields);
        this.apiKey = apiKey == null ? Optional.empty() : Optional.of(apiKey);
        this.type = Objects.requireNonNull(type);
        this.commonStructs = commonStructs == null ? Collections.emptyList() :
                List.copyOf(commonStructs);
        if (flexibleVersions == null) {
            throw new RuntimeException("You must specify a value for flexibleVersions. " +
                    "Please use 0+ for all new messages.");
        }
        this.flexibleVersions = Versions.parse(flexibleVersions, Versions.NONE);
        if ((!this.flexibleVersions().empty()) &&
                (this.flexibleVersions.highest() < Short.MAX_VALUE)) {
            throw new RuntimeException("Field " + name + " specifies flexibleVersions " +
                this.flexibleVersions + ", which is not open-ended.  flexibleVersions must " +
                "be either none, or an open-ended range (that ends with a plus sign).");
        }

        if (listeners != null && !listeners.isEmpty() && type != MessageSpecType.REQUEST) {
            throw new RuntimeException("The `requestScope` property is only valid for " +
                "messages with type `request`");
        }
        this.listeners = listeners;

        if (latestVersionUnstable && type != MessageSpecType.REQUEST) {
            throw new RuntimeException("The `latestVersionUnstable` property is only valid for " +
                "messages with type `request`");
        }
        this.latestVersionUnstable = latestVersionUnstable;

        if (type == MessageSpecType.COORDINATOR_KEY) {
            if (this.apiKey.isEmpty()) {
                throw new RuntimeException("The ApiKey must be set for messages " + name + " with type `coordinator-key`");
            }
            if (!this.validVersions().equals(new Versions((short) 0, ((short) 0)))) {
                throw new RuntimeException("The Versions must be set to `0` for messages " + name + " with type `coordinator-key`");
            }
            if (!this.flexibleVersions.empty()) {
                throw new RuntimeException("The FlexibleVersions are not supported for messages " + name + "  with type `coordinator-key`");
            }
        }

        if (type == MessageSpecType.COORDINATOR_VALUE) {
            if (this.apiKey.isEmpty()) {
                throw new RuntimeException("The ApiKey must be set for messages with type `coordinator-value`");
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

    @JsonProperty("listeners")
    public List<RequestListenerType> listeners() {
        return listeners;
    }

    @JsonProperty("latestVersionUnstable")
    public boolean latestVersionUnstable() {
        return latestVersionUnstable;
    }

    public String dataClassName() {
        switch (type) {
            case HEADER:
            case REQUEST:
            case RESPONSE:
                // We append the Data suffix to request/response/header classes to avoid
                // collisions with existing objects. This can go away once the protocols
                // have all been converted and we begin using the generated types directly.
                return struct.name() + "Data";
            default:
                return struct.name();
        }
    }
}
