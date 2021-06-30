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

package org.apache.kafka.metadata;

import java.util.Objects;

/**
 * An immutable class which represents version ranges.
 */
public class VersionRange {
    public final static VersionRange ALL = new VersionRange((short) 0, Short.MAX_VALUE);

    private final short min;
    private final short max;

    public VersionRange(short min, short max) {
        this.min = min;
        this.max = max;
    }

    public short min() {
        return min;
    }

    public short max() {
        return max;
    }

    public boolean contains(VersionRange other) {
        return other.min >= min && other.max <= max;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VersionRange)) return false;
        VersionRange other = (VersionRange) o;
        return other.min == min && other.max == max;
    }

    @Override
    public String toString() {
        if (min == max) {
            return String.valueOf(min);
        } else if (max == Short.MAX_VALUE) {
            return String.valueOf(min) + "+";
        } else {
            return String.valueOf(min) + "-" + String.valueOf(max);
        }
    }
}
