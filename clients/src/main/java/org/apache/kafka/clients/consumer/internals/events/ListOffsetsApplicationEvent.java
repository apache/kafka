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

import java.util.HashMap;
import java.util.Map;

/**
 * Event for retrieving partition offsets by performing a
 * {@link org.apache.kafka.common.requests.ListOffsetsRequest ListOffsetsRequest}.
 * This event is created with a map of {@link TopicPartition} and target timestamps to search
 * offsets for. It is completed with a map of {@link TopicPartition} and the
 * {@link OffsetAndTimestamp} found (offset of the first message whose timestamp is greater than
 * or equals to the target timestamp)
 */
public class ListOffsetsApplicationEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndTimestamp>> {
    final Map<TopicPartition, Long> timestampsToSearch;
    final boolean requireTimestamps;

    public ListOffsetsApplicationEvent(Map<TopicPartition, Long> timestampToSearch, boolean requireTimestamps) {
        super(Type.LIST_OFFSETS);
        this.timestampsToSearch = timestampToSearch;
        this.requireTimestamps = requireTimestamps;
    }

    @Override
    public String toString() {
        return "ListOffsetsApplicationEvent {" +
                "timestampsToSearch=" + timestampsToSearch + ", " +
                "requireTimestamps=" + requireTimestamps + '}';
    }

    /**
     * Build result representing that no offsets were found as part of the current event.
     *
     * @return Map containing all the partitions the event was trying to get offsets for, and
     * null {@link OffsetAndTimestamp} as value
     */
    public Map<TopicPartition, OffsetAndTimestamp> emptyResult() {
        HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(timestampsToSearch.size());
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet())
            offsetsByTimes.put(entry.getKey(), null);
        return offsetsByTimes;
    }
}
