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
import java.util.Set;

public class FetchCommittedOffsetsApplicationEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndMetadata>> {

    /**
     * Partitions to retrieve committed offsets for.
     */
    private final Set<TopicPartition> partitions;

    /**
     * Timer to wait for a response, retrying on retriable errors.
     */
    private final Timer timer;

    public FetchCommittedOffsetsApplicationEvent(final Set<TopicPartition> partitions, final Timer timer) {
        super(Type.FETCH_COMMITTED_OFFSETS);
        this.partitions = Collections.unmodifiableSet(partitions);
        this.timer = timer;
    }

    public Set<TopicPartition> partitions() {
        return partitions;
    }

    public Timer timer() {
        return timer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        FetchCommittedOffsetsApplicationEvent that = (FetchCommittedOffsetsApplicationEvent) o;

        return partitions.equals(that.partitions);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + partitions.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{" +
                toStringBase() +
                ", partitions=" + partitions +
                ", retriable=" + !timer.isExpired() +
                '}';
    }
}
