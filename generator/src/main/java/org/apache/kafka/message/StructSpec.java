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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public final class StructSpec {
    private final String name;

    private final Versions versions;

    private final Versions deprecatedVersions;

    private final List<FieldSpec> fields;

    private final boolean hasKeys;

    @JsonCreator
    public StructSpec(@JsonProperty("name") String name,
                      @JsonProperty("versions") String versions,
                      @JsonProperty("deprecatedVersions") String deprecatedVersions,
                      @JsonProperty("fields") List<FieldSpec> fields) {
        this.name = Objects.requireNonNull(name);
        this.versions = Versions.parse(versions, null);
        if (this.versions == null) {
            throw new RuntimeException("You must specify the version of the " +
                    name + " structure.");
        }
        this.deprecatedVersions = Versions.parse(deprecatedVersions, Versions.NONE);
        ArrayList<FieldSpec> newFields = new ArrayList<>();
        if (fields != null) {
            // Each field should have a unique tag ID (if the field has a tag ID).
            HashSet<Integer> tags = new HashSet<>();
            for (FieldSpec field : fields) {
                if (field.tag().isPresent()) {
                    if (tags.contains(field.tag().get())) {
                        throw new RuntimeException("In " + name + ", field " + field.name() +
                            " has a duplicate tag ID " + field.tag().get() + ".  All tags IDs " +
                            "must be unique.");
                    }
                    tags.add(field.tag().get());
                }
                newFields.add(field);
            }
            // Tag IDs should be contiguous and start at 0.  This optimizes space on the wire,
            // since larger numbers take more space.
            for (int i = 0; i < tags.size(); i++) {
                if (!tags.contains(i)) {
                    throw new RuntimeException("In " + name + ", the tag IDs are not " +
                        "contiguous.  Make use of tag " + i + " before using any " +
                        "higher tag IDs.");
                }
            }
        }
        this.fields = Collections.unmodifiableList(newFields);
        this.hasKeys = this.fields.stream().anyMatch(f -> f.mapKey());
    }

    @JsonProperty
    public String name() {
        return name;
    }

    public Versions versions() {
        return versions;
    }

    @JsonProperty
    public String versionsString() {
        return versions.toString();
    }

    public Versions deprecatedVersions() {
        return deprecatedVersions;
    }

    @JsonProperty
    public List<FieldSpec> fields() {
        return fields;
    }

    boolean hasKeys() {
        return hasKeys;
    }
}
