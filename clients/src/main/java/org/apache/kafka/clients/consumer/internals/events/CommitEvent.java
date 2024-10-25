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
import java.util.Optional;

public abstract class CommitEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>> {

    /**
     * Offsets to commit per partition.
     */
    private final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets;

    protected CommitEvent(final Type type, final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, final long deadlineMs) {
        super(type, deadlineMs);
        this.offsets = validate(offsets);
    }

    /**
     * Validates the offsets are not negative and then returns the given offset map as
     * {@link Collections#unmodifiableMap(Map) as unmodifiable}.
     */
    private static Optional<Map<TopicPartition, OffsetAndMetadata>> validate(final Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        if (!offsets.isPresent()) {
            return Optional.empty();
        }

        for (OffsetAndMetadata offsetAndMetadata : offsets.get().values()) {
            if (offsetAndMetadata.offset() < 0) {
                throw new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset());
            }
        }

        return Optional.of(Collections.unmodifiableMap(offsets.get()));
    }

    public Optional<Map<TopicPartition, OffsetAndMetadata>> offsets() {
        return offsets;
    }

    @Override
    protected String toStringBase() {
        return super.toStringBase() + ", offsets=" + offsets;
    }
}
