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
package org.apache.kafka.tiered.storage.specs;

import java.util.Objects;

public final class ConsumableSpec {

    private final Long fetchOffset;
    private final Integer expectedTotalCount;
    private final Integer expectedFromSecondTierCount;

    public ConsumableSpec(Long fetchOffset,
                          Integer expectedTotalCount,
                          Integer expectedFromSecondTierCount) {
        this.fetchOffset = fetchOffset;
        this.expectedTotalCount = expectedTotalCount;
        this.expectedFromSecondTierCount = expectedFromSecondTierCount;
    }

    public Long getFetchOffset() {
        return fetchOffset;
    }

    public Integer getExpectedTotalCount() {
        return expectedTotalCount;
    }

    public Integer getExpectedFromSecondTierCount() {
        return expectedFromSecondTierCount;
    }

    @Override
    public String toString() {
        return "ConsumableSpec{" +
                "fetchOffset=" + fetchOffset +
                ", expectedTotalCount=" + expectedTotalCount +
                ", expectedFromSecondTierCount=" + expectedFromSecondTierCount +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumableSpec that = (ConsumableSpec) o;
        return Objects.equals(fetchOffset, that.fetchOffset)
                && Objects.equals(expectedTotalCount, that.expectedTotalCount)
                && Objects.equals(expectedFromSecondTierCount, that.expectedFromSecondTierCount);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fetchOffset, expectedTotalCount, expectedFromSecondTierCount);
    }
}
