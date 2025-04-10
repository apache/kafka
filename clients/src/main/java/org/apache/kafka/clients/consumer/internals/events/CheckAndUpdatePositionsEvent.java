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

import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;

/**
 * Event to check if all assigned partitions have fetch positions. If there are positions missing, it will fetch
 * offsets and update positions when it gets them. This will first attempt to use the committed offsets if available. If
 * no committed offsets available, it will use the partition offsets retrieved from the leader.
 * <p/>
 * The event completes with a boolean indicating if all assigned partitions have valid fetch positions
 * (based on {@link SubscriptionState#hasAllFetchPositions()}).
 */
public class CheckAndUpdatePositionsEvent extends CompletableApplicationEvent<Boolean> {

    public CheckAndUpdatePositionsEvent(long deadlineMs) {
        super(Type.CHECK_AND_UPDATE_POSITIONS, deadlineMs);
    }

    /**
     * Indicates that this event requires subscription metadata to be present
     * for its execution. This is used to ensure that metadata errors are
     * handled correctly during the {@link org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#poll(Duration) poll} 
     * or {@link org.apache.kafka.clients.consumer.internals.AsyncKafkaConsumer#position(TopicPartition) position} process.
     */
    @Override
    public boolean requireSubscriptionMetadata() {
        return true;
    }
}