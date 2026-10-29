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
package org.apache.kafka.tiered.storage.specs;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.log.remote.storage.LocalTieredStorageEvent;

/**
 * Specifies a delete segment/partition event from a second-tier storage. This is used to ensure the
 * interactions between Kafka and the second-tier storage match expectations.
 *
 * @param sourceBrokerId The broker which deletes (a) remote log segments(s) (or) deletes the entire partition from
 *                       the second-tier storage.
 * @param topicPartition The topic-partition in which the deletion should happen.
 * @param eventType      Allowed event types are {@link LocalTieredStorageEvent.EventType#DELETE_PARTITION} and
 *                       {@link LocalTieredStorageEvent.EventType#DELETE_SEGMENT}
 * @param eventCount     How many events are expected to interact with the second-tier storage.
 */
public record RemoteDeleteSegmentSpec(int sourceBrokerId, TopicPartition topicPartition,
                                      LocalTieredStorageEvent.EventType eventType, int eventCount) {

    @Override
    public String toString() {
        return String.format("RemoteDeleteSegment[source-broker-id=%d topic-partition=%s event-type=%s event-count=%d]",
                sourceBrokerId, topicPartition, eventType, eventCount);
    }

}
