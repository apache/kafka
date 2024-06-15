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

    private final short maxVersion;

    /**
     * Raises an exception unless the following conditions are met:
     *  0 <= minVersion <= maxVersion.
     *
     * @param minVersion           The minimum version value.
     * @param maxVersion           The maximum version value.
     *
     * @throws IllegalArgumentException   Raised when the condition described above is not met.
     */
    SupportedVersionRange(final short minVersion, final short maxVersion) {
        if (minVersion < 0 || maxVersion < 0 || maxVersion < minVersion) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected 0 <= minVersion <= maxVersion but received minVersion:%d, maxVersion:%d.",
                    minVersion,
                    maxVersion));
        }
        this.minVersion = minVersion;
        this.maxVersion = maxVersion;
    }

    public short minVersion() {
        return minVersion;
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
        return this.minVersion == that.minVersion && this.maxVersion == that.maxVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minVersion, maxVersion);
    }

    @Override
    public String toString() {
        return String.format("SupportedVersionRange[min_version:%d, max_version:%d]", minVersion, maxVersion);
    }
}

