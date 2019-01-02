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

import java.util.Objects;

public final class Versions {
    private final short lowest;
    private final short highest;

    public static Versions parse(String input, Versions defaultVersions) {
        if (input == null) {
            return defaultVersions;
        }
        String trimmedInput = input.trim();
        if (trimmedInput.length() == 0) {
            return defaultVersions;
        }
        if (trimmedInput.equals(NONE_STRING)) {
            return NONE;
        }
        if (trimmedInput.endsWith("+")) {
            return new Versions(Short.parseShort(
            trimmedInput.substring(0, trimmedInput.length() - 1)),
                Short.MAX_VALUE);
        } else {
            int dashIndex = trimmedInput.indexOf("-");
            if (dashIndex < 0) {
                short version = Short.parseShort(trimmedInput);
                return new Versions(version, version);
            }
            return new Versions(
                Short.parseShort(trimmedInput.substring(0, dashIndex)),
                Short.parseShort(trimmedInput.substring(dashIndex + 1)));
        }
    }

    public static final Versions ALL = new Versions((short) 0, Short.MAX_VALUE);

    public static final Versions NONE = new Versions((short) 0, (short) -1);

    public static final String NONE_STRING = "none";

    public Versions(short lowest, short highest) {
        this.lowest = lowest;
        this.highest = highest;
    }

    public short lowest() {
        return lowest;
    }

    public short highest() {
        return highest;
    }

    public boolean empty() {
        return lowest > highest;
    }

    @Override
    public String toString() {
        if (empty()) {
            return NONE_STRING;
        } else if (lowest == highest) {
            return String.valueOf(lowest);
        } else if (highest == Short.MAX_VALUE) {
            return String.format("%d+", lowest);
        } else {
            return String.format("%d-%d", lowest, highest);
        }
    }

    public Versions intersect(Versions other) {
        short newLowest = lowest > other.lowest ? lowest : other.lowest;
        short newHighest = highest < other.highest ? highest : other.highest;
        if (newLowest > newHighest) {
            return Versions.NONE;
        }
        return new Versions(newLowest, newHighest);
    }

    public boolean contains(short version) {
        return version >= lowest && version <= highest;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowest, highest);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Versions)) {
            return false;
        }
        Versions otherVersions = (Versions) other;
        return lowest == otherVersions.lowest &&
               highest == otherVersions.highest;
    }
}
