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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * It indicates the deletion state of the remote topic partition. This will be based on the action executed on this
 * partition by the remote log service implementation.
 * State transitions are mentioned below.
 * <p>
 * <PRE>
 * +-------------------------+
 * |DELETE_PARTITION_MARKED  |
 * +-----------+-------------+
 * |
 * |
 * +-----------v--------------+
 * |DELETE_PARTITION_STARTED  |
 * +-----------+--------------+
 * |
 * |
 * +-----------v--------------+
 * |DELETE_PARTITION_FINISHED |
 * +--------------------------+
 * </PRE>
 * </P>
 */
@InterfaceStability.Evolving
public enum RemotePartitionDeleteState {

    /**
     * This is used when a topic/partition is marked for delete by the controller.
     * That means, all its remote log segments are eligible for deletion so that remote partition removers can
     * start deleting them.
     */
    DELETE_PARTITION_MARKED((byte) 0),

    /**
     * This state indicates that the partition deletion is started but not yet finished.
     */
    DELETE_PARTITION_STARTED((byte) 1),

    /**
     * This state indicates that the partition is deleted successfully.
     */
    DELETE_PARTITION_FINISHED((byte) 2);

    private static final Map<Byte, RemotePartitionDeleteState> STATE_TYPES = Collections.unmodifiableMap(
            Arrays.stream(values()).collect(Collectors.toMap(RemotePartitionDeleteState::id, Function.identity())));

    private final byte id;

    RemotePartitionDeleteState(byte id) {
        this.id = id;
    }

    public byte id() {
        return id;
    }

    public static RemotePartitionDeleteState forId(byte id) {
        return STATE_TYPES.get(id);
    }

}