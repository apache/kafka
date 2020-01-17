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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class CodesSpec extends DeclarationSpec {
    private final String name;
    private final MessageSpecType type;
    private final Map<String, CodeSpec> codesMap;
    private FieldType valueType;

    private List<CodeSpec> codeSpecs;

    @JsonCreator
    public CodesSpec(@JsonProperty("name") String name,
                     @JsonProperty("type") MessageSpecType type,
                     @JsonProperty("valueType") String valueType,
                     @JsonProperty("codes") List<CodeSpec> codes) {
        super();
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.valueType = FieldType.parse(Objects.requireNonNull(valueType));
        if (!(this.valueType instanceof FieldType.Int8FieldType
            || this.valueType instanceof FieldType.Int16FieldType
            || this.valueType instanceof FieldType.Int32FieldType
            || this.valueType instanceof FieldType.Int64FieldType)) {
            throw new RuntimeException("Codes " + name + " has non-integer valueType " + valueType);
        }
        this.codeSpecs = codes;

        this.codesMap = codes.stream().collect(Collectors.toMap(c -> c.name(), c -> c));
    }

    @Override
    public MessageSpecType type() {
        return type;
    }

    public String name() {
        return name;
    }

    public FieldType valueType() {
        return valueType;
    }

    public List<CodeSpec> codes() {
        return codeSpecs;
    }

    @Override
    public String generatedClassName() {
        return name();
    }

    public CodeSpec codeForName(String name) {
        return codesMap.get(name);
    }
}
