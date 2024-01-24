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
import java.util.Objects;

public class CommitApplicationEvent extends CompletableApplicationEvent<Void> {

    /**
     * Offsets to commit per partition.
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * The value of this flag is <code>true</code> if the requests allows to be retried
     * (sync requests that provide an expiration time to bound the retries). The value
     * of <code>false</code> means that we do not allow retries on RetriableErrors (async
     * requests that does not provide an expiration time for retries).
     */
    private final boolean allowsRetries;

    /**
     * Create new event to commit offsets. If timer is present, the request will be retried on
     * retriable errors until the timer expires (sync commit offsets request). If the timer is
     * not present, the request will be sent without waiting for a response of retrying (async
     * commit offsets request).
     */
    public CommitApplicationEvent(final Map<TopicPartition, OffsetAndMetadata> offsets,
                                  final Timer timer,
                                  final boolean allowsRetries) {
        super(Type.COMMIT, timer);
        this.offsets = Collections.unmodifiableMap(offsets);
        this.allowsRetries = allowsRetries;

        for (OffsetAndMetadata offsetAndMetadata : offsets.values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    public boolean allowsRetries() {
        return allowsRetries;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CommitApplicationEvent that = (CommitApplicationEvent) o;

        return offsets.equals(that.offsets) && allowsRetries == that.allowsRetries;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(offsets, allowsRetries);
    }

    @Override
    public String toString() {
        return "CommitApplicationEvent{" +
                toStringBase() +
                ", offsets=" + offsets +
                ", allowsRetries=" + allowsRetries +
                '}';
    }
}
