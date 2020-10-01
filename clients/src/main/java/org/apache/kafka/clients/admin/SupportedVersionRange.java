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
package org.apache.kafka.clients.admin;

import java.util.Objects;

/**
 * Represents a range of versions that a particular broker supports for some feature.
 */
public class SupportedVersionRange {
    private final short minVersion;

    private final short firstActiveVersion;

    private final short maxVersion;

    /**
     * Raises an exception unless the following conditions are met:
     *  1 <= minVersion <= firstActiveVersion <= maxVersion
     *
     * @param minVersion           The minimum version value.
     * @param firstActiveVersion   The first active version value.
     * @param maxVersion           The maximum version value.
     *
     * @throws IllegalArgumentException   Raised when the condition described above is not met.
     */
    SupportedVersionRange(final short minVersion, final short firstActiveVersion, final short maxVersion) {
        if (minVersion < 1 ||
            maxVersion < 1 ||
            firstActiveVersion < minVersion ||
            firstActiveVersion > maxVersion) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected 1 <= minVersion <= firstActiveVersion <= maxVersion" +
                    " but received minVersion:%d, firstActiveVersion:%d, maxVersion:%d.",
                    minVersion,
                    firstActiveVersion,
                    maxVersion));
        }
        this.minVersion = minVersion;
        this.firstActiveVersion = firstActiveVersion;
        this.maxVersion = maxVersion;
    }

    public short minVersion() {
        return minVersion;
    }

    public short firstActiveVersion() {
        return firstActiveVersion;
    }

    public short maxVersion() {
        return maxVersion;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final SupportedVersionRange that = (SupportedVersionRange) other;
        return this.minVersion == that.minVersion &&
            this.firstActiveVersion == that.firstActiveVersion &&
            this.maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minVersion, firstActiveVersion, maxVersion);
    }

    @Override
    public String toString() {
        return String.format(
            "SupportedVersionRange[min_version:%d, first_active_version:%d, max_version:%d]",
            minVersion,
            firstActiveVersion,
            maxVersion);
    }
}

