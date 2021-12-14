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

import java.util

import kafka.api._
import kafka.server.KafkaConfig
import kafka.utils.CoreUtils.toJavaConsumer
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.{LiCombinedControlRequestData, UpdateMetadataRequestData}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LiCombinedControlRequest, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.LiCombinedControlTransformer

object LiDecomposedControlRequestUtils {
  def decomposeRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): LiDecomposedControlRequest = {
    val leaderAndIsrRequest = extractLeaderAndIsrRequest(request, brokerEpoch, config)
    val updateMetadataRequest = extractUpdateMetadataRequest(request, config)
    val stopReplicaRequests = extractStopReplicaRequest(request, brokerEpoch, config)
    LiDecomposedControlRequest(leaderAndIsrRequest, updateMetadataRequest, stopReplicaRequests)
  }

  private def extractLeaderAndIsrRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): Option[LeaderAndIsrRequest] = {
    val partitionsInRequest = request.leaderAndIsrPartitionStates()
    val leadersInRequest = request.liveLeaders()

    val effectivePartitionStates = new util.ArrayList[LeaderAndIsrPartitionState]()
    partitionsInRequest.forEach(toJavaConsumer((partition:LiCombinedControlRequestData.LeaderAndIsrPartitionState) =>
      if (partition.brokerEpoch() >= brokerEpoch)
        effectivePartitionStates.add(LiCombinedControlTransformer.restoreLeaderAndIsrPartition(partition))
    ))

    if (effectivePartitionStates.isEmpty) {
      None
    } else {
      val leaderNodes = new util.ArrayList[Node]()
      leadersInRequest.forEach(toJavaConsumer((leader:LiCombinedControlRequestData.LeaderAndIsrLiveLeader) =>
        leaderNodes.add(new Node(leader.brokerId(), leader.hostName(), leader.port()))
      ))

      val leaderAndIsrRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 5
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      Some(new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
        request.maxBrokerEpoch(), effectivePartitionStates, leaderNodes).build())
    }
  }

  private def extractUpdateMetadataRequest(request: LiCombinedControlRequest, config: KafkaConfig): Option[UpdateMetadataRequest] = {
    val partitionsInRequest = request.updateMetadataPartitionStates()
    val brokersInRequest = request.liveBrokers()

    val effectivePartitionStates = new util.ArrayList[UpdateMetadataPartitionState]()
    partitionsInRequest.forEach(toJavaConsumer((partition:LiCombinedControlRequestData.UpdateMetadataPartitionState) =>
      effectivePartitionStates.add(LiCombinedControlTransformer.restoreUpdateMetadataPartition(partition))
    ))

    val liveBrokers = new util.ArrayList[UpdateMetadataBroker]()
    brokersInRequest.forEach(toJavaConsumer((broker:LiCombinedControlRequestData.UpdateMetadataBroker) => liveBrokers.add(LiCombinedControlTransformer.restoreUpdateMetadataBroker(broker))))

    if (effectivePartitionStates.isEmpty && liveBrokers.isEmpty) {
      None
    } else {
      val updateMetadataRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 7
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      Some(new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
        request.maxBrokerEpoch(), effectivePartitionStates, liveBrokers).build())
    }
  }

  // extractStopReplicaRequest could possible return two StopReplicaRequests
  // the first one with the deletePartitions field set to true, and the second one with the deletePartitions field set to false
  private def extractStopReplicaRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): List[StopReplicaRequest] = {
    val partitionsInRequest = request.stopReplicaPartitionStates()

    val effectivePartitionStates = new util.ArrayList[LiCombinedControlRequestData.StopReplicaPartitionState]()
    partitionsInRequest.forEach(toJavaConsumer((partition:LiCombinedControlRequestData.StopReplicaPartitionState) =>
      if (partition.brokerEpoch() >= brokerEpoch) {
        effectivePartitionStates.add(partition)
      }
    ))
    if (effectivePartitionStates.isEmpty) {
      List.empty
    } else {
      val stopReplicaRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 3
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      val partitionsWithDelete = new util.ArrayList[TopicPartition]()
      val partitionsWithoutDelete = new util.ArrayList[TopicPartition]()
      effectivePartitionStates.forEach(toJavaConsumer((partition:LiCombinedControlRequestData.StopReplicaPartitionState) => {
        val topicPartition = new TopicPartition(partition.topicName(), partition.partitionIndex())
        if (partition.deletePartitions()) {
          partitionsWithDelete.add(topicPartition)
        } else {
          partitionsWithoutDelete.add(topicPartition)
        }
      }))

      var stopReplicaRequests: List[StopReplicaRequest] = List.empty

      if (partitionsWithDelete.isEmpty) {
        None
      } else {
        stopReplicaRequests = getStopReplicaRequest(stopReplicaRequestVersion, request, true, partitionsWithDelete) :: stopReplicaRequests
      }

      if (partitionsWithoutDelete.isEmpty) {
        None
      } else {
        stopReplicaRequests = getStopReplicaRequest(stopReplicaRequestVersion, request, false, partitionsWithoutDelete) :: stopReplicaRequests
      }

      stopReplicaRequests
    }
  }

  def getStopReplicaRequest(stopReplicaRequestVersion: Short, request: LiCombinedControlRequest,
    deletePartitions: Boolean, partitions: util.List[TopicPartition]): StopReplicaRequest = {
    new StopReplicaRequest.Builder(stopReplicaRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
      request.maxBrokerEpoch(), deletePartitions, partitions).build()
  }
}
