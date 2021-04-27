package kafka.utils

import org.apache.kafka.common.requests.{LeaderAndIsrResponse, StopReplicaResponse, UpdateMetadataResponse}

case class LiDecomposedControlResponse(leaderAndIsrResponse: LeaderAndIsrResponse,
  updateMetadataResponse: UpdateMetadataResponse,
  stopReplicaResponse: StopReplicaResponse)
