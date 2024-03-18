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

package org.apache.kafka.coordinator.group;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public enum GroupProtocolMigrationPolicy {
    /** Both upgrade and downgrade are enabled.*/
    BOTH("both"),

    /** Only upgrade is enabled.*/
    UPGRADE("upgrade"),

    /** Only downgrade is enabled.*/
    DOWNGRADE("downgrade"),

    /** Neither upgrade nor downgrade is enabled.*/
    NONE("none");

    private final String policy;

    GroupProtocolMigrationPolicy(String config) {
        this.policy = config;
    }

    @Override
    public String toString() {
        return policy;
    }

    private final static Map<String, GroupProtocolMigrationPolicy> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toMap(config -> config.policy.toLowerCase(Locale.ROOT), Function.identity()));

    /**
     * Parse a string into the corresponding {@code GroupProtocolMigrationPolicy} enum value, in a case-insensitive manner.
     *
     * @return The {{@link GroupProtocolMigrationPolicy}} according to the string passed. None is returned if
     * the string doesn't correspond to a valid policy.
     */
    public static GroupProtocolMigrationPolicy parse(String name) {
        if (name == null) {
            return NONE;
        }
        GroupProtocolMigrationPolicy config = NAME_TO_ENUM.get(name.toLowerCase(Locale.ROOT));

        return config == null ? NONE : config;
    }

}
