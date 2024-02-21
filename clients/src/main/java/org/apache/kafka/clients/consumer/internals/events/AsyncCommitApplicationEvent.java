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
import org.apache.kafka.clients.consumer.internals.RelaxedCompletableFuture;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Map;

/**
 * Event to commit offsets without waiting for a response, so the request won't be retried.
 */
public class AsyncCommitApplicationEvent extends ApplicationEvent implements CompletableEvent<Void> {

    private final RelaxedCompletableFuture<Void> future;

    /**
     * Offsets to commit per partition.
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public AsyncCommitApplicationEvent(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(Type.COMMIT_SYNC);
        this.offsets = Collections.unmodifiableMap(offsets);
        this.future = new RelaxedCompletableFuture<>();

        for (OffsetAndMetadata offsetAndMetadata : offsets.values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }
    }

    @Override
    public RelaxedCompletableFuture<Void> future() {
        return future;
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AsyncCommitApplicationEvent that = (AsyncCommitApplicationEvent) o;

        return offsets.equals(that.offsets);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + offsets.hashCode();
        return result;
    }
}
