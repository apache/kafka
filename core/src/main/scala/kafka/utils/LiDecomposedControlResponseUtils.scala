/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import org.apache.kafka.common.message.{LeaderAndIsrResponseData, StopReplicaResponseData, UpdateMetadataResponseData}
import org.apache.kafka.common.requests.{LeaderAndIsrResponse, LiCombinedControlResponse, StopReplicaResponse, UpdateMetadataResponse}
import org.apache.kafka.common.utils.LiCombinedControlTransformer

object LiDecomposedControlResponseUtils {
  def decomposeResponse(response: LiCombinedControlResponse): LiDecomposedControlResponse = {
    val leaderAndIsrResponse = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
    .setErrorCode(response.leaderAndIsrErrorCode())
    .setPartitionErrors(LiCombinedControlTransformer.restoreLeaderAndIsrPartitionErrors(response.leaderAndIsrPartitionErrors())),
      leaderAndIsrResponseVersion(response))

    val updateMetadataResponse = new UpdateMetadataResponse(new UpdateMetadataResponseData()
      .setErrorCode(response.updateMetadataErrorCode()))

    val stopReplicaResponse = new StopReplicaResponse(new StopReplicaResponseData()
    .setErrorCode(response.stopReplicaErrorCode())
    .setPartitionErrors(LiCombinedControlTransformer.restoreStopReplicaPartitionErrors(response.stopReplicaPartitionErrors())))

    LiDecomposedControlResponse(leaderAndIsrResponse, updateMetadataResponse, stopReplicaResponse)
  }

  def leaderAndIsrResponseVersion(response: LiCombinedControlResponse): Short = {
    // Version 1 of the LiCombinedControl corresponds to version 6 of the LeaderAndIsr request.
    // Version 0 of the LiCombinedControl corresponds to version 5 of the LeaderAndIsr request.
    // Inside LinkedIn, there should be no usage of any LeaderAndIsr requests earlier than version 5,
    // given the IBP is 2.4
    if(response.version == 1) 6 else 5
  }
}
