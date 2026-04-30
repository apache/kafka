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
    public static final int UNINITIALIZED_DELIVERY_COMPLETE_COUNT = -1;
    public static final long UNINITIALIZED_LAG = -1;
    public static final short DEFAULT_ERROR_CODE = Errors.NONE.code();
    public static final int DEFAULT_LEADER_EPOCH = 0;
    public static final String DEFAULT_ERR_MESSAGE = Errors.NONE.message();

    public static PartitionIdData newPartitionIdData(int partition) {
        return new PartitionData(partition, DEFAULT_STATE_EPOCH, UNINITIALIZED_START_OFFSET, UNINITIALIZED_DELIVERY_COMPLETE_COUNT, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionIdLeaderEpochData newPartitionIdLeaderEpochData(int partition, int leaderEpoch) {
        return new PartitionData(partition, DEFAULT_STATE_EPOCH, UNINITIALIZED_START_OFFSET, UNINITIALIZED_DELIVERY_COMPLETE_COUNT, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, leaderEpoch, null);
    }

    public static PartitionStateData newPartitionStateData(int partition, int stateEpoch, long startOffset) {
        // If the start offset is uninitialized (when the share partition is being initialized for the first time), the 
        // consumption hasn't started yet, and lag cannot be calculated. Thus, deliveryCompleteCount is also set as -1. 
        // But, if start offset is a non-negative value (when the start offset is altered), the lag can be calculated 
        // from that point onward. Hence, we set deliveryCompleteCount to 0 in that case.
        int deliveryCompleteCount = startOffset == UNINITIALIZED_START_OFFSET ? UNINITIALIZED_DELIVERY_COMPLETE_COUNT : 0;
        return new PartitionData(partition, stateEpoch, startOffset, deliveryCompleteCount, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionErrorData newPartitionErrorData(int partition, short errorCode, String errorMessage) {
        return new PartitionData(partition, DEFAULT_STATE_EPOCH, UNINITIALIZED_START_OFFSET, UNINITIALIZED_DELIVERY_COMPLETE_COUNT, errorCode, errorMessage, DEFAULT_LEADER_EPOCH, null);
    }

    public static PartitionStateSummaryData newPartitionStateSummaryData(int partition, int stateEpoch, long startOffset, int deliveryCompleteCount, int leaderEpoch, short errorCode, String errorMessage) {
        return new PartitionData(partition, stateEpoch, startOffset, deliveryCompleteCount, errorCode, errorMessage, leaderEpoch, null);
    }

    public static PartitionStateBatchData newPartitionStateBatchData(int partition, int stateEpoch, long startOffset, int deliveryCompleteCount, int leaderEpoch, List<PersisterStateBatch> stateBatches) {
        return new PartitionData(partition, stateEpoch, startOffset, deliveryCompleteCount, DEFAULT_ERROR_CODE, DEFAULT_ERR_MESSAGE, leaderEpoch, stateBatches);
    }

    public static PartitionAllData newPartitionAllData(int partition, int stateEpoch, long startOffset, short errorCode, String errorMessage, List<PersisterStateBatch> stateBatches) {
        return new PartitionData(partition, stateEpoch, startOffset, UNINITIALIZED_DELIVERY_COMPLETE_COUNT, errorCode, errorMessage, DEFAULT_LEADER_EPOCH, stateBatches);
    }
}
