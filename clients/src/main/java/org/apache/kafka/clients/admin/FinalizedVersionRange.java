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
 * Represents a range of version levels supported by every broker in a cluster for some feature.
 */
public class FinalizedVersionRange {
    private final short minVersionLevel;

    private final short maxVersionLevel;

    /**
     * Raises an exception unless the following condition is met:
     * {@code minVersionLevel >= 1} and {@code maxVersionLevel >= 1} and {@code maxVersionLevel >= minVersionLevel}.
     *
     * @param minVersionLevel   The minimum version level value.
     * @param maxVersionLevel   The maximum version level value.
     *
     * @throws IllegalArgumentException   Raised when the condition described above is not met.
     */
    public FinalizedVersionRange(final short minVersionLevel, final short maxVersionLevel) {
        if (minVersionLevel < 0 || maxVersionLevel < 0 || maxVersionLevel < minVersionLevel) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected minVersionLevel >= 0, maxVersionLevel >= 0 and" +
                    " maxVersionLevel >= minVersionLevel, but received" +
                    " minVersionLevel: %d, maxVersionLevel: %d", minVersionLevel, maxVersionLevel));
        }
        this.minVersionLevel = minVersionLevel;
        this.maxVersionLevel = maxVersionLevel;
    }

    public short minVersionLevel() {
        return minVersionLevel;
    }

    public short maxVersionLevel() {
        return maxVersionLevel;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FinalizedVersionRange)) {
            return false;
        }

        final FinalizedVersionRange that = (FinalizedVersionRange) other;
        return this.minVersionLevel == that.minVersionLevel &&
            this.maxVersionLevel == that.maxVersionLevel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minVersionLevel, maxVersionLevel);
    }

    @Override
    public String toString() {
        return String.format(
            "FinalizedVersionRange[min_version_level:%d, max_version_level:%d]",
            minVersionLevel,
            maxVersionLevel);
    }
}
