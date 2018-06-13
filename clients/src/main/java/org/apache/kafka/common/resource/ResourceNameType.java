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

package org.apache.kafka.common.resource;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Resource name type.
 */
@InterfaceStability.Evolving
public enum ResourceNameType {
    /**
     * Represents any ResourceNameType which this client cannot understand, perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any resource name type.
     */
    ANY((byte) 1),

    /**
     * A literal resource name.
     *
     * A literal name defines the full name of a resource, e.g. topic with name 'foo', or group with name 'bob'.
     *
     * The special wildcard character {@code *} can be used to represent a resource with any name.
     */
    LITERAL((byte) 2),

    /**
     * A prefixed resource name.
     *
     * A prefixed name defines a prefix for a resource, e.g. topics with names that start with 'foo'.
     */
    PREFIXED((byte) 3);

    private final static Map<Byte, ResourceNameType> CODE_TO_VALUE =
        Collections.unmodifiableMap(
            Arrays.stream(ResourceNameType.values())
                .collect(Collectors.toMap(ResourceNameType::code, Function.identity()))
        );

    private final static Map<String, ResourceNameType> NAME_TO_VALUE =
        Collections.unmodifiableMap(
            Arrays.stream(ResourceNameType.values())
                .collect(Collectors.toMap(ResourceNameType::name, Function.identity()))
        );

    private final byte code;

    ResourceNameType(byte code) {
        this.code = code;
    }

    /**
     * @return the code of this resource.
     */
    public byte code() {
        return code;
    }

    /**
     * Return whether this resource name type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    /**
     * Return the ResourceNameType with the provided code or {@link #UNKNOWN} if one cannot be found.
     */
    public static ResourceNameType fromCode(byte code) {
        return CODE_TO_VALUE.getOrDefault(code, UNKNOWN);
    }

    /**
     * Return the ResourceNameType with the provided name or {@link #UNKNOWN} if one cannot be found.
     */
    public static ResourceNameType fromString(String name) {
        return NAME_TO_VALUE.getOrDefault(name, UNKNOWN);
    }
}
