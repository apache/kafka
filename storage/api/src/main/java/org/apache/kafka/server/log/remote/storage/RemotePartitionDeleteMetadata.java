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
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * This class represents the metadata about the remote partition. It can be created/updated with {@link RemoteLogMetadataManager#putRemotePartitionDeleteMetadata(RemotePartitionDeleteMetadata)}.
 * Possible state transitions are mentioned at {@link RemotePartitionDeleteState}.
 */
@InterfaceStability.Evolving
public class RemotePartitionDeleteMetadata extends RemoteLogMetadata {

    private final TopicIdPartition topicIdPartition;
    private final RemotePartitionDeleteState state;

    /**
     * Creates an instance of this class with the given metadata.
     *
     * @param topicIdPartition topic partition for which this event is meant for.
     * @param state            State of the remote topic partition.
     * @param eventTimestampMs Epoch time in milli seconds at which this event is occurred.
     * @param brokerId         Id of the broker in which this event is raised.
     */
    public RemotePartitionDeleteMetadata(TopicIdPartition topicIdPartition,
                                         RemotePartitionDeleteState state,
                                         long eventTimestampMs,
                                         int brokerId) {
        super(brokerId, eventTimestampMs);
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition);
        this.state = Objects.requireNonNull(state);
    }

    /**
     * @return TopicIdPartition for which this event is meant for.
     */
    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    /**
     * It represents the state of the remote partition. It can be one of the values of {@link RemotePartitionDeleteState}.
     */
    public RemotePartitionDeleteState state() {
        return state;
    }

    @Override
    public String toString() {
        return "RemotePartitionDeleteMetadata{" +
               "topicPartition=" + topicIdPartition +
               ", state=" + state +
               ", eventTimestampMs=" + eventTimestampMs() +
               ", brokerId=" + brokerId() +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemotePartitionDeleteMetadata that = (RemotePartitionDeleteMetadata) o;
        return Objects.equals(topicIdPartition, that.topicIdPartition) &&
                state == that.state &&
                eventTimestampMs() == that.eventTimestampMs() &&
                brokerId() == that.brokerId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicIdPartition, state, eventTimestampMs(), brokerId());
    }
}