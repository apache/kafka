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
 * Encapsulates details about an update to a finalized feature.
 */
public class FeatureUpdate {
    private final short maxVersionLevel;
    private final boolean allowDowngrade;

    /**
     * @param maxVersionLevel   the new maximum version level for the finalized feature.
     *                          a value &lt; 1 is special and indicates that the update is intended to
     *                          delete the finalized feature, and should be accompanied by setting
     *                          the allowDowngrade flag to true.
     * @param allowDowngrade    - true, if this feature update was meant to downgrade the existing
     *                            maximum version level of the finalized feature.
     *                          - false, otherwise.
     */
    public FeatureUpdate(final short maxVersionLevel, final boolean allowDowngrade) {
        if (maxVersionLevel < 1 && !allowDowngrade) {
            throw new IllegalArgumentException(String.format(
                "The allowDowngrade flag should be set when the provided maxVersionLevel:%d is < 1.",
                maxVersionLevel));
        }
        this.maxVersionLevel = maxVersionLevel;
        this.allowDowngrade = allowDowngrade;
    }

    public short maxVersionLevel() {
        return maxVersionLevel;
    }

    public boolean allowDowngrade() {
        return allowDowngrade;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof FeatureUpdate)) {
            return false;
        }

        final FeatureUpdate that = (FeatureUpdate) other;
        return this.maxVersionLevel == that.maxVersionLevel && this.allowDowngrade == that.allowDowngrade;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxVersionLevel, allowDowngrade);
    }

    @Override
    public String toString() {
        return String.format("FeatureUpdate{maxVersionLevel:%d, allowDowngrade:%s}", maxVersionLevel, allowDowngrade);
    }
}
