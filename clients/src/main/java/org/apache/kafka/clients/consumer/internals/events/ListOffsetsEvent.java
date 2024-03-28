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
import org.apache.kafka.clients.consumer.internals.OffsetAndTimestampInternal;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Timer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Application event to list offsets for partitions by timestamps. It is completed with the map of
 * {@link TopicPartition} and {@link OffsetAndTimestamp} found (offset of the first message whose timestamp is
 * greater than or equals to the target timestamp)
 */
public class ListOffsetsEvent extends CompletableApplicationEvent<Map<TopicPartition, OffsetAndTimestampInternal>> {
    private final Map<TopicPartition, Long> timestampsToSearch;
    public final boolean requireTimestamps;

    public ListOffsetsEvent(Map<TopicPartition, Long> timestampToSearch,
                            Timer timer,
                            boolean requireTimestamps) {
        super(Type.LIST_OFFSETS, timer);
        this.timestampsToSearch = Collections.unmodifiableMap(timestampToSearch);
        this.requireTimestamps = requireTimestamps;
    }

    public <T> Map<TopicPartition, T> emptyResults() {
        Map<TopicPartition, T> result = new HashMap<>();
        timestampsToSearch.keySet().forEach(tp -> result.put(tp, null));
        return result;
    }

    public Map<TopicPartition, Long> timestampsToSearch() {
        return timestampsToSearch;
    }

    @Override
    public String toStringBase() {
        return super.toStringBase() +
                ", timestampsToSearch=" + timestampsToSearch;
    }

}