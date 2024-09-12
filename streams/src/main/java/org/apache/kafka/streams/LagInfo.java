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
package org.apache.kafka.streams;

import java.util.Objects;

/**
 * Encapsulates information about lag, at a store partition replica (active or standby). This information is constantly changing as the
 * tasks process records and thus, they should be treated as simply instantaneous measure of lag.
 */
public class LagInfo {

    private final long currentOffsetPosition;

    private final long endOffsetPosition;

    private final long offsetLag;

    LagInfo(final long currentOffsetPosition, final long endOffsetPosition) {
        this.currentOffsetPosition = currentOffsetPosition;
        this.endOffsetPosition = endOffsetPosition;
        this.offsetLag = Math.max(0, endOffsetPosition - currentOffsetPosition);
    }

    /**
     * Get the current maximum offset on the store partition's changelog topic, that has been successfully written into
     * the store partition's state store.
     *
     * @return current consume offset for standby/restoring store partitions &amp;   simply endoffset for active store partition replicas
     */
    public long currentOffsetPosition() {
        return this.currentOffsetPosition;
    }

    /**
     * Get the end offset position for this store partition's changelog topic on the Kafka brokers.
     *
     * @return last offset written to the changelog topic partition
     */
    public long endOffsetPosition() {
        return this.endOffsetPosition;
    }

    /**
     * Get the measured lag between current and end offset positions, for this store partition replica
     *
     * @return lag as measured by message offsets
     */
    public long offsetLag() {
        return this.offsetLag;
    }

    @Override
    public boolean equals(final Object obj) {
        if (!(obj instanceof LagInfo)) {
            return false;
        }
        final LagInfo other = (LagInfo) obj;
        return currentOffsetPosition == other.currentOffsetPosition
            && endOffsetPosition == other.endOffsetPosition
            && this.offsetLag == other.offsetLag;
    }

    @Override
    public int hashCode() {
        return Objects.hash(currentOffsetPosition, endOffsetPosition, offsetLag);
    }

    @Override
    public String toString() {
        return "LagInfo {" +
            " currentOffsetPosition=" + currentOffsetPosition +
            ", endOffsetPosition=" + endOffsetPosition +
            ", offsetLag=" + offsetLag +
            '}';
    }
}
