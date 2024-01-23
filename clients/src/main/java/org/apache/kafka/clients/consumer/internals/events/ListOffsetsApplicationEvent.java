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

import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Timer;

import java.util.Collections;
import java.util.Map;

/**
 * Event for retrieving partition offsets by performing a
 * {@link org.apache.kafka.common.requests.ListOffsetsRequest ListOffsetsRequest}.
 * This event is created with a map of {@link TopicPartition} and target timestamps to search
 * offsets for. It is completed with the map of {@link TopicPartition} and
 * {@link OffsetAndTimestamp} found (offset of the first message whose timestamp is greater than
 * or equals to the target timestamp)
 */
public class ListOffsetsApplicationEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndTimestamp>> {

    private final Map<TopicPartition, Long> timestampsToSearch;
    private final boolean requireTimestamps;

    public ListOffsetsApplicationEvent(Map<TopicPartition, Long> timestampToSearch,
                                       boolean requireTimestamps,
                                       Timer timer) {
        super(Type.LIST_OFFSETS, timer);
        this.timestampsToSearch = Collections.unmodifiableMap(timestampToSearch);
        this.requireTimestamps = requireTimestamps;
    }

    public Map<TopicPartition, Long> timestampsToSearch() {
        return timestampsToSearch;
    }

    public boolean requireTimestamps() {
        return requireTimestamps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ListOffsetsApplicationEvent that = (ListOffsetsApplicationEvent) o;

        if (requireTimestamps != that.requireTimestamps) return false;
        return timestampsToSearch.equals(that.timestampsToSearch);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + timestampsToSearch.hashCode();
        result = 31 * result + (requireTimestamps ? 1 : 0);
        return result;
    }

    @Override
    protected String toStringBase() {
        return toStringBase() +
                ", timestampsToSearch=" + timestampsToSearch + ", " +
                "requireTimestamps=" + requireTimestamps;
    }

}