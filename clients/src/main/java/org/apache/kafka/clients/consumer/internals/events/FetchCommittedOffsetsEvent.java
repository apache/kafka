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
import java.util.Set;

public class FetchCommittedOffsetsEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>> {

    /**
     * Partitions to retrieve committed offsets for.
     */
    private final Set<TopicPartition> partitions;

    /**
     * Time until which the request will be retried if it fails with a retriable error.
     */
    private final long timeoutMs;

    public FetchCommittedOffsetsEvent(final Set<TopicPartition> partitions, final long timeoutMs) {
        super(Type.FETCH_COMMITTED_OFFSETS);
        this.partitions = Collections.unmodifiableSet(partitions);
        this.timeoutMs = timeoutMs;
    }

    public Set<TopicPartition> partitions() {
        return partitions;
    }

    public long timeout() {
        return timeoutMs;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() + ", partitions=" + partitions + ", partitions=" + partitions;
    }
}
