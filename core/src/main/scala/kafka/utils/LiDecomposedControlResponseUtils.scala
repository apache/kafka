package kafka.utils

import org.apache.kafka.common.message.{LeaderAndIsrResponseData, StopReplicaResponseData, UpdateMetadataResponseData}
import org.apache.kafka.common.requests.{LeaderAndIsrResponse, LiCombinedControlResponse, StopReplicaResponse, UpdateMetadataResponse}
import org.apache.kafka.common.utils.LiCombinedControlTransformer

object LiDecomposedControlResponseUtils {
  def decomposeResponse(response: LiCombinedControlResponse): LiDecomposedControlResponse = {
    val leaderAndIsrResponse = new LeaderAndIsrResponse(new LeaderAndIsrResponseData()
    .setErrorCode(response.leaderAndIsrErrorCode())
    .setPartitionErrors(LiCombinedControlTransformer.restoreLeaderAndIsrPartitionErrors(response.leaderAndIsrPartitionErrors())))

    val updateMetadataResponse = new UpdateMetadataResponse(new UpdateMetadataResponseData()
      .setErrorCode(response.updateMetadataErrorCode()))

    val stopReplicaResponse = new StopReplicaResponse(new StopReplicaResponseData()
    .setErrorCode(response.stopReplicaErrorCode())
    .setPartitionErrors(LiCombinedControlTransformer.restoreStopReplicaPartitionErrors(response.stopReplicaPartitionErrors())))

    LiDecomposedControlResponse(leaderAndIsrResponse, updateMetadataResponse, stopReplicaResponse)
  }
}
