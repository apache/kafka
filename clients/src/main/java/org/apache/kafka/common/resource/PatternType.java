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
 * Resource pattern type.
 */
@InterfaceStability.Evolving
public enum PatternType {
    /**
     * Represents any PatternType which this client cannot understand, perhaps because this client is too old.
     */
    UNKNOWN((byte) 0),

    /**
     * In a filter, matches any resource pattern type.
     */
    ANY((byte) 1),

    /**
     * In a filter, will perform pattern matching.
     *
     * e.g. Given a filter of {@code ResourcePatternFilter(TOPIC, "payments.received", MATCH)`}, the filter match
     * any {@link ResourcePattern} that matches topic 'payments.received'. This might include:
     * <ul>
     *     <li>A Literal pattern with the same type and name, e.g. {@code ResourcePattern(TOPIC, "payments.received", LITERAL)}</li>
     *     <li>A Wildcard pattern with the same type, e.g. {@code ResourcePattern(TOPIC, "*", LITERAL)}</li>
     *     <li>A Prefixed pattern with the same type and where the name is a matching prefix, e.g. {@code ResourcePattern(TOPIC, "payments.", PREFIXED)}</li>
     * </ul>
     */
    MATCH((byte) 2),

    /**
     * A literal resource name.
     *
     * A literal name defines the full name of a resource, e.g. topic with name 'foo', or group with name 'bob'.
     *
     * The special wildcard character {@code *} can be used to represent a resource with any name.
     */
    LITERAL((byte) 3),

    /**
     * A prefixed resource name.
     *
     * A prefixed name defines a prefix for a resource, e.g. topics with names that start with 'foo'.
     */
    PREFIXED((byte) 4);

    private final static Map<Byte, PatternType> CODE_TO_VALUE =
        Collections.unmodifiableMap(
            Arrays.stream(PatternType.values())
                .collect(Collectors.toMap(PatternType::code, Function.identity()))
        );

    private final static Map<String, PatternType> NAME_TO_VALUE =
        Collections.unmodifiableMap(
            Arrays.stream(PatternType.values())
                .collect(Collectors.toMap(PatternType::name, Function.identity()))
        );

    private final byte code;

    PatternType(byte code) {
        this.code = code;
    }

    /**
     * @return the code of this resource.
     */
    public byte code() {
        return code;
    }

    /**
     * @eturn whether this resource pattern type is UNKNOWN.
     */
    public boolean isUnknown() {
        return this == UNKNOWN;
    }

    /**
     * @return whether this resource pattern type is a concrete type, rather than UNKNOWN or one of the filter types.
     */
    public boolean isSpecific() {
        return this != UNKNOWN && this != ANY && this != MATCH;
    }

    /**
     * Return the PatternType with the provided code or {@link #UNKNOWN} if one cannot be found.
     */
    public static PatternType fromCode(byte code) {
        return CODE_TO_VALUE.getOrDefault(code, UNKNOWN);
    }

    /**
     * Return the PatternType with the provided name or {@link #UNKNOWN} if one cannot be found.
     */
    public static PatternType fromString(String name) {
        return NAME_TO_VALUE.getOrDefault(name, UNKNOWN);
    }
}
