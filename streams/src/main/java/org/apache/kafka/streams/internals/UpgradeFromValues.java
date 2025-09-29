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
package org.apache.kafka.streams.internals;

public enum UpgradeFromValues {
    UPGRADE_FROM_0100("0.10.0"),
    UPGRADE_FROM_0101("0.10.1"),
    UPGRADE_FROM_0102("0.10.2"),
    UPGRADE_FROM_0110("0.11.0"),
    UPGRADE_FROM_10("1.0"),
    UPGRADE_FROM_11("1.1"),
    UPGRADE_FROM_20("2.0"),
    UPGRADE_FROM_21("2.1"),
    UPGRADE_FROM_22("2.2"),
    UPGRADE_FROM_23("2.3"),
    UPGRADE_FROM_24("2.4"),
    UPGRADE_FROM_25("2.5"),
    UPGRADE_FROM_26("2.6"),
    UPGRADE_FROM_27("2.7"),
    UPGRADE_FROM_28("2.8"),
    UPGRADE_FROM_30("3.0"),
    UPGRADE_FROM_31("3.1"),
    UPGRADE_FROM_32("3.2"),
    UPGRADE_FROM_33("3.3"),
    UPGRADE_FROM_34("3.4"),
    UPGRADE_FROM_35("3.5"),
    UPGRADE_FROM_36("3.6"),
    UPGRADE_FROM_37("3.7"),
    UPGRADE_FROM_38("3.8"),
    UPGRADE_FROM_39("3.9");

    private final String value;

    UpgradeFromValues(final String value) {
        this.value = value;
    }

    public static UpgradeFromValues fromString(final String upgradeFrom) {
        return UpgradeFromValues.valueOf("UPGRADE_FROM_" + upgradeFrom.replace(".", ""));
    }
    public String toString() {
        return value;
    }
}
