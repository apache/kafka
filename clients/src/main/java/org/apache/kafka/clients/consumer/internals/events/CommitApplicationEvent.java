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
package org.apache.kafka.clients.consumer.internals.events;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Timer;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class CommitApplicationEvent extends CompletableApplicationEvent<Void> {

    /**
     * Offsets to commit per partition.
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * Timer to wait for a response, retrying on retriable errors. If not present, the request is
     * triggered without waiting for a response or being retried.
     */
    private final Optional<Timer> timer;

    /**
     * Create new event to commit offsets. If timer is present, the request will be retried on
     * retriable errors until the timer expires (sync commit offsets request). If the timer is
     * not present, the request will be sent without waiting for a response of retrying (async
     * commit offsets request).
     */
    public CommitApplicationEvent(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                  final Optional<Timer> timer) {
        super(Type.COMMIT);
        this.offsets = Collections.unmodifiableMap(offsets);
        this.timer = timer;

        for (OffsetAndMetadata offsetAndMetadata : offsets.values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    public Optional<Timer> timer() {
        return timer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CommitApplicationEvent that = (CommitApplicationEvent) o;

        return offsets.equals(that.offsets);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + offsets.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "CommitApplicationEvent{" +
                toStringBase() +
                ", offsets=" + offsets +
                ", retriable=" + (timer.isPresent() && !timer.get().isExpired()) +
                '}';
    }
}
