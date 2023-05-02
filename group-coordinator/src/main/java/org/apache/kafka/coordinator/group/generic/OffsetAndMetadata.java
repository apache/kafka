/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group.generic;

import java.util.Objects;
import java.util.Optional;

public class OffsetAndMetadata {

    public static final String NO_METADATA = "";

    private final long offset;
    private final Optional<Integer> leaderEpoch;
    private final String metadata;
    private final long commitTimestamp;
    private final Optional<Long> expireTimestamp;

    public OffsetAndMetadata(
        long offset,
        Optional<Integer> leaderEpoch,
        String metadata,
        long commitTimestamp,
        Optional<Long> expireTimestamp
    ) {
        this.offset = offset;
        this.leaderEpoch = leaderEpoch;
        this.metadata = metadata;
        this.commitTimestamp = commitTimestamp;
        this.expireTimestamp = expireTimestamp;
    }

    public long offset() {
        return this.offset;
    }

    public Optional<Integer> leaderEpoch() {
        return this.leaderEpoch;
    }

    public String metadata() {
        return this.metadata;
    }

    public long commitTimestamp() {
        return this.commitTimestamp;
    }

    public Optional<Long> expireTimestamp() {
        return this.expireTimestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetAndMetadata that = (OffsetAndMetadata) o;

        return this.offset == that.offset &&
            this.leaderEpoch.equals(that.leaderEpoch) &&
            this.metadata.equals(that.metadata) &&
            this.commitTimestamp == that.commitTimestamp &&
            this.expireTimestamp == that.expireTimestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            offset,
            leaderEpoch,
            metadata,
            commitTimestamp,
            expireTimestamp
        );
    }
}
