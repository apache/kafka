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

package org.apache.kafka.server.share.persister;

import org.apache.kafka.common.protocol.Errors;

import java.util.List;

/**
 * This is a factory class for creating instances of {@link PartitionData}.
 */
public class PartitionFactory {
    public static final int DEFAULT_STATE_EPOCH = 0;
    public static final int UNINITIALIZED_START_OFFSET = -1;
    public static final short DEFAULT_ERROR_CODE = Errors.NONE.code();
    public static final int DEFAULT_LEADER_EPOCH = 0;
    public static final String DEFAULT_ERR_MESSAGE = Errors.NONE.message();

    public static PartitionIdData newPartitionIdData(int partition) {
        return new PartitionData(partition, DEFAULT_STATE_EPOCH, UNINITIALIZED_START_OFFSET, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionIdLeaderEpochData newPartitionIdLeaderEpochData(int partition, int leaderEpoch) {
        return new PartitionData(partition, DEFAULT_STATE_EPOCH, UNINITIALIZED_START_OFFSET, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, leaderEpoch, null);
    }

    public static PartitionStateData newPartitionStateData(int partition, int stateEpoch, long startOffset) {
        return new PartitionData(partition, stateEpoch, startOffset, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionErrorData newPartitionErrorData(int partition, short errorCode, String errorMessage) {
        return new PartitionData(partition, DEFAULT_STATE_EPOCH, UNINITIALIZED_START_OFFSET, errorCode, errorMessage, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionStateErrorData newPartitionStateErrorData(int partition, int stateEpoch, long startOffset, short errorCode, String errorMessage) {
        return new PartitionData(partition, stateEpoch, startOffset, errorCode, errorMessage, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionStateBatchData newPartitionStateBatchData(int partition, int stateEpoch, long startOffset, int leaderEpoch, List<PersisterStateBatch> stateBatches) {
        return new PartitionData(partition, stateEpoch, startOffset, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, leaderEpoch, stateBatches);
    }

    public static PartitionAllData newPartitionAllData(int partition, int stateEpoch, long startOffset, short errorCode, String errorMessage, List<PersisterStateBatch> stateBatches) {
        return new PartitionData(partition, stateEpoch, startOffset, errorCode, errorMessage, DEFAULT_LEADER_EPOCH, stateBatches);
    }
}
