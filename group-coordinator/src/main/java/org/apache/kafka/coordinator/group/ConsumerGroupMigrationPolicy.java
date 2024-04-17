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

public enum ConsumerGroupMigrationPolicy {
    /** Both upgrade and downgrade are enabled.*/
    BIDIRECTIONAL("bidirectional", true, true),

    /** Only upgrade is enabled.*/
    UPGRADE("upgrade", true, false),

    /** Only downgrade is enabled.*/
    DOWNGRADE("downgrade", false, true),

    /** Neither upgrade nor downgrade is enabled.*/
    DISABLED("disabled", false, false);

    private final String name;
    private final boolean isUpgradeEnabled;
    private final boolean isDowngradeEnabled;

    ConsumerGroupMigrationPolicy(String config, boolean isUpgradeEnabled, boolean isDowngradeEnabled) {
        this.name = config;
        this.isUpgradeEnabled = isUpgradeEnabled;
        this.isDowngradeEnabled = isDowngradeEnabled;
    }

    public boolean isUpgradeEnabled() {
        return isUpgradeEnabled;
    }

    public boolean isDowngradeEnabled() {
        return isDowngradeEnabled;
    }

    @Override
    public String toString() {
        return name;
    }

    private final static Map<String, ConsumerGroupMigrationPolicy> NAME_TO_ENUM = Arrays.stream(values())
        .collect(Collectors.toMap(policy -> policy.name.toLowerCase(Locale.ROOT), Function.identity()));

    /**
     * Parse a string into the corresponding {@code GroupProtocolMigrationPolicy} enum value, in a case-insensitive manner.
     *
     * @return The {{@link ConsumerGroupMigrationPolicy}} according to the string passed. None is returned if
     * the string doesn't correspond to a valid policy.
     */
    public static ConsumerGroupMigrationPolicy parse(String name) {
        if (name == null) {
            return DISABLED;
        }
        ConsumerGroupMigrationPolicy policy = NAME_TO_ENUM.get(name.toLowerCase(Locale.ROOT));

        return policy == null ? DISABLED : policy;
    }
}
