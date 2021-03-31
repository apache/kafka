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

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Objects;

/**
 * It describes the metadata update about the log segment in the remote storage. This is currently used to update the
 * state of the remote log segment by using {@link RemoteLogMetadataManager#updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate)}.
 * This also includes the timestamp of this event.
 */
@InterfaceStability.Evolving
public class RemoteLogSegmentMetadataUpdate {

    /**
     * Universally unique remote log segment id.
     */
    private final RemoteLogSegmentId remoteLogSegmentId;

    /**
     * Epoch time in milli seconds at which this event is generated.
     */
    private final long eventTimestampMs;

    /**
     * It indicates the state in which the action is executed on this segment.
     */
    private final RemoteLogSegmentState state;

    /**
     * Broker id from which this event is generated.
     */
    private final int brokerId;

    /**
     * @param remoteLogSegmentId Universally unique remote log segment id.
     * @param eventTimestampMs   Epoch time in milli seconds at which the remote log segment is copied to the remote tier storage.
     * @param state              State of the remote log segment.
     * @param brokerId           Broker id from which this event is generated.
     */
    public RemoteLogSegmentMetadataUpdate(RemoteLogSegmentId remoteLogSegmentId, long eventTimestampMs,
                                          RemoteLogSegmentState state, int brokerId) {
        this.remoteLogSegmentId = Objects.requireNonNull(remoteLogSegmentId, "remoteLogSegmentId can not be null");
        this.state = Objects.requireNonNull(state, "state can not be null");
        this.brokerId = brokerId;
        this.eventTimestampMs = eventTimestampMs;
    }

    /**
     * @return Universally unique id of this remote log segment.
     */
    public RemoteLogSegmentId remoteLogSegmentId() {
        return remoteLogSegmentId;
    }

    /**
     * @return Epoch time in milli seconds at which this event is generated.
     */
    public long eventTimestampMs() {
        return eventTimestampMs;
    }

    /**
     * It represents the state of the remote log segment. It can be one of the values of {@link RemoteLogSegmentState}.
     */
    public RemoteLogSegmentState state() {
        return state;
    }

    /**
     * @return Broker id from which this event is generated.
     */
    public int brokerId() {
        return brokerId;
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
        return eventTimestampMs == that.eventTimestampMs &&
               Objects.equals(remoteLogSegmentId, that.remoteLogSegmentId) &&
               state == that.state &&
               brokerId == that.brokerId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(remoteLogSegmentId, eventTimestampMs, state, brokerId);
    }

    @Override
    public String toString() {
        return "RemoteLogSegmentMetadataUpdate{" +
               "remoteLogSegmentId=" + remoteLogSegmentId +
               ", eventTimestampMs=" + eventTimestampMs +
               ", state=" + state +
               ", brokerId=" + brokerId +
               '}';
    }
}
