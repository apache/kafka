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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.regex.Pattern;

public class CodeSpec {

    private static final Pattern VALID_CODE_NAMES = Pattern.compile("[A-Z]([A-Za-z0-9_]*)");

    private final String name;
    private final Long value;
    private final String about;

    public CodeSpec(@JsonProperty("name") String name,
                    @JsonProperty("value") Long value,
                    @JsonProperty("about") String about) {
        this.name = Objects.requireNonNull(name);
        if (!VALID_CODE_NAMES.matcher(this.name).matches()) {
            throw new RuntimeException("Invalid code name " + this.name);
        }
        this.value = value;
        this.about = about == null ? "" : about;
    }

    public String name() {
        return name;
    }

    public Long value() {
        return value;
    }

    public String about() {
        return about;
    }
}
