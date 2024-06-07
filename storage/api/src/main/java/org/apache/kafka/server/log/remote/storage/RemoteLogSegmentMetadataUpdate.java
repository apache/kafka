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
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;

import java.util.Objects;
import java.util.Optional;

/**
 * It describes the metadata update about the log segment in the remote storage. This is currently used to update the
 * state of the remote log segment by using {@link RemoteLogMetadataManager#updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate)}.
 * This also includes the timestamp of this event.
 */
@InterfaceStability.Evolving
public class RemoteLogSegmentMetadataUpdate extends RemoteLogMetadata {

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Custom metadata.
     */
    private final Optional<CustomMetadata> customMetadata;

    /**
     * It indicates the state in which the action is executed on this segment.
     */
    private final RemoteLogSegmentState state;

    /**
     * @param remoteLogSegmentId Universally unique remote log segment id.
     * @param eventTimestampMs   Epoch time in milli seconds at which the remote log segment is copied to the remote tier storage.
     * @param customMetadata     Custom metadata.
     * @param state              State of the remote log segment.
     * @param brokerId           Broker id from which this event is generated.
     */
    public RemoteLogSegmentMetadataUpdate(RemoteLogSegmentId remoteLogSegmentId, long eventTimestampMs,
                                          Optional<CustomMetadata> customMetadata,
                                          RemoteLogSegmentState state,
                                          int brokerId) {
        super(brokerId, eventTimestampMs);
        this.remoteLogSegmentId = Objects.requireNonNull(remoteLogSegmentId, "remoteLogSegmentId can not be null");
        this.customMetadata = Objects.requireNonNull(customMetadata, "customMetadata can not be null");
        this.state = Objects.requireNonNull(state, "state can not be null");
    }

    /**
     * @return Universally unique id of this remote log segment.
     */
    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    /**
     * @return Custom metadata.
     */
    public Optional<CustomMetadata> customMetadata() {
        return customMetadata;
    }

    /**
     * It represents the state of the remote log segment. It can be one of the values of {@link RemoteLogSegmentState}.
     */
    public RemoteLogSegmentState state() {
        return state;
    }

    @Override
    public TopicIdPartition topicIdPartition() {
        return remoteLogSegmentId.topicIdPartition();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RemoteLogSegmentMetadataUpdate that = (RemoteLogSegmentMetadataUpdate) o;
        return Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId) &&
               Objects.equals(customMetadata, that.customMetadata) &&
               state == that.state &&
               eventTimestampMs() == that.eventTimestampMs() &&
               brokerId() == that.brokerId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, customMetadata, state, eventTimestampMs(), brokerId());
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadataUpdate{" +
               "remoteLogSegmentId=" + remoteLogSegmentId +
               ", customMetadata=" + customMetadata +
               ", state=" + state +
               ", eventTimestampMs=" + eventTimestampMs() +
               ", brokerId=" + brokerId() +
               '}';
    }
}
