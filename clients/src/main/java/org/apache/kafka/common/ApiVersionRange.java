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
package org.apache.kafka.common;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Utils;

import java.util.Objects;

/**
 * A public class which represents a range of versions.
 */
public class ApiVersionRange {
    /**
     * The lowest supported version.
     */
    private final short lowest;

    /**
     * The highest supported version.
     */
    private final short highest;

    public ApiVersionRange(short lowest, short highest) {
        this.lowest = lowest;
        this.highest = highest;
        assert lowest <= highest;
    }

    public short lowest() {
        return lowest;
    }

    public short highest() {
        return highest;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ApiVersionRange))
            return false;
        ApiVersionRange other = (ApiVersionRange) o;
        return lowest == other.lowest && highest == other.highest;
    }

    public short usableVersion(ApiVersionRange other) {
        short usable = Utils.min(highest, other.highest());
        if (usable < lowest)
            throw new UnsupportedVersionException("unusable: node too new");
        if (usable < other.lowest())
            throw new UnsupportedVersionException("unusable: node too old");
        return usable;
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowest, highest);
    }

    @Override
    public String toString() {
        if (lowest == highest)
            return Short.toString(lowest);
        else
            return "" + lowest + " to " + highest;
    }
}
