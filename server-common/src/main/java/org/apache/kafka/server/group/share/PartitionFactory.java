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

package org.apache.kafka.server.group.share;

import org.apache.kafka.common.protocol.Errors;

import java.util.List;

public class PartitionFactory {
  public static PartitionIdData newPartitionIdData(int partition) {
    return new PartitionData(partition, -1, -1, Errors.NONE.code(), null);
  }

  public static PartitionStateData newPartitionStateData(int partition, int stateEpoch, long startOffset) {
    return new PartitionData(partition, stateEpoch, startOffset, Errors.NONE.code(), null);
  }

  public static PartitionErrorData newPartitionErrorData(int partition, short errorCode) {
    return new PartitionData(partition, -1, -1, errorCode, null);
  }

  public static PartitionStateErrorData newPartitionStateErrorData(int partition, int stateEpoch, long startOffset, short errorCode) {
    return new PartitionData(partition, stateEpoch, startOffset, errorCode, null);
  }

  public static PartitionStateBatchData newPartitionStateBatchData(int partition, int stateEpoch, long startOffset, List<PersisterStateBatch> stateBatches) {
    return new PartitionData(partition, stateEpoch, startOffset, Errors.NONE.code(), stateBatches);
  }

  public static PartitionAllData newPartitionAllData(int partition, int stateEpoch, long startOffset, short errorCode, List<PersisterStateBatch> stateBatches) {
    return new PartitionData(partition, stateEpoch, startOffset, errorCode, stateBatches);
  }
}
