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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This enum indicates the state of the remote log segment. This will be based on the action executed on this
 * segment by the remote log service implementation.
 * <p>
 * It goes through the below state transitions. Self transition is treated as valid. This allows updating with the
 * same state in case of retries and failover.
 * <p>
 * <pre>
 * +---------------------+            +----------------------+
 * |COPY_SEGMENT_STARTED |----------> |COPY_SEGMENT_FINISHED |
 * +-------------------+-+            +--+-------------------+
 *                     |                 |
 *                     |                 |
 *                     v                 v
 *                  +--+-----------------+-+
 *                  |DELETE_SEGMENT_STARTED|
 *                  +-----------+----------+
 *                              |
 *                              |
 *                              v
 *                  +-----------+-----------+
 *                  |DELETE_SEGMENT_FINISHED|
 *                  +-----------------------+
 * </pre>
 */
@InterfaceStability.Evolving
public enum RemoteLogSegmentState {

    /**
     * This state indicates that the segment copying to remote storage is started but not yet finished.
     */
    COPY_SEGMENT_STARTED((byte) 0),

    /**
     * This state indicates that the segment copying to remote storage is finished.
     */
    COPY_SEGMENT_FINISHED((byte) 1),

    /**
     * This state indicates that the segment deletion is started but not yet finished.
     */
    DELETE_SEGMENT_STARTED((byte) 2),

    /**
     * This state indicates that the segment is deleted successfully.
     */
    DELETE_SEGMENT_FINISHED((byte) 3);

    private static final Map<Byte, RemoteLogSegmentState> STATE_TYPES = Collections.unmodifiableMap(
            Arrays.stream(values()).collect(Collectors.toMap(RemoteLogSegmentState::id, Function.identity())));

    private final byte id;

    RemoteLogSegmentState(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static RemoteLogSegmentState forId(byte id) {
        return STATE_TYPES.get(id);
    }

    public static boolean isValidTransition(RemoteLogSegmentState srcState, RemoteLogSegmentState targetState) {
        Objects.requireNonNull(targetState, "targetState can not be null");

        if (srcState == null) {
            // If the source state is null, check the target state as the initial state viz COPY_SEGMENT_STARTED
            // This ensures simplicity here as we don't have to define one more type to represent the state 'null' like
            // COPY_SEGMENT_NOT_STARTED, have the null check by the caller and pass that state.
            return targetState == COPY_SEGMENT_STARTED;
        } else if (srcState == targetState) {
            // Self transition is treated as valid. This is to maintain the idempotency for the state in case of retries
            // or failover.
            return true;
        } else if (srcState == COPY_SEGMENT_STARTED) {
            return targetState == COPY_SEGMENT_FINISHED || targetState == DELETE_SEGMENT_STARTED;
        } else if (srcState == COPY_SEGMENT_FINISHED) {
            return targetState == DELETE_SEGMENT_STARTED;
        } else if (srcState == DELETE_SEGMENT_STARTED) {
            return targetState == DELETE_SEGMENT_FINISHED;
        } else {
            return false;
        }
    }
}
