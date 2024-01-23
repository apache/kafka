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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class CommitAsyncApplicationEvent extends ApplicationEvent {

    private final CompletableFuture<Void> future;

    /**
     * Offsets to commit per partition.
     */
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    /**
     * Create new event to commit offsets.
     */
    public CommitAsyncApplicationEvent(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        super(Type.COMMIT_ASYNC);
        this.offsets = Collections.unmodifiableMap(offsets);
        this.future = new CompletableFuture<>();

        for (OffsetAndMetadata offsetAndMetadata : offsets.values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> offsets() {
        return offsets;
    }

    public CompletableFuture<Void> future() {
        return future;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        CommitAsyncApplicationEvent that = (CommitAsyncApplicationEvent) o;

        return offsets.equals(that.offsets) && future.equals(that.future);
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(offsets, future);
    }

    @Override
    public String toString() {
        return "CommitAsyncApplicationEvent{" +
                toStringBase() +
                ", offsets=" + offsets +
                ", future=" + future +
                '}';
    }
}
