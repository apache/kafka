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

package org.apache.kafka.metadata.properties;

/**
 * The version of a meta.properties file.
 */
public enum MetaPropertiesVersion {
    /**
     * The original version of meta.properties. This is the version that gets used if there is no
     * version field. No fields are guaranteed in v0.
     */
    V0(0),

    /**
     * The KRaft version of meta.properties. Not all KRaft clusters have this version; some which
     * were migrated from ZK may have v0.
     */
    V1(1);

    private final int number;

    public static MetaPropertiesVersion fromNumberString(String numberString) {
        int number;
        try {
            number = Integer.parseInt(numberString.trim());
        } catch (NumberFormatException  e) {
            throw new RuntimeException("Invalid meta.properties version string '" +
                    numberString + "'");
        }
        return fromNumber(number);
    }

    public static MetaPropertiesVersion fromNumber(int number) {
        switch (number) {
            case 0: return V0;
            case 1: return V1;
            default: throw new RuntimeException("Unknown meta.properties version number " + number);
        }
    }

    MetaPropertiesVersion(int number) {
        this.number = number;
    }

    public int number() {
        return number;
    }

    public String numberString() {
        return Integer.toString(number);
    }

    public boolean hasBrokerId() {
        return this == V0;
    }

    public boolean alwaysHasNodeId() {
        return this != V0;
    }

    public boolean alwaysHasClusterId() {
        return this != V0;
    }
}
