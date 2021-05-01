package kafka.utils

import org.apache.kafka.common.requests.{LeaderAndIsrRequest, StopReplicaRequest, UpdateMetadataRequest}

// one LiDecomposedControlRequest may contain at most two StopReplicaRequests
// one with the deletePartitions flag set to true and the other with the flag set to false
case class LiDecomposedControlRequest(leaderAndIsrRequest: Option[LeaderAndIsrRequest], updateMetadataRequest: Option[UpdateMetadataRequest],
  stopReplicaRequests: List[StopReplicaRequest])
