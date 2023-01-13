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
import org.apache.kafka.common.{Node, TopicPartition, Uuid}
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.LiCombinedControlRequestData
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests.{LeaderAndIsrRequest, LeaderAndIsrRequestType, LiCombinedControlRequest, StopReplicaRequest, UpdateMetadataRequest}
import org.apache.kafka.common.utils.LiCombinedControlTransformer

import scala.collection.JavaConverters._
import scala.collection.mutable

object LiDecomposedControlRequestUtils {
  def decomposeRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): LiDecomposedControlRequest = {
    val leaderAndIsrRequest = extractLeaderAndIsrRequest(request, brokerEpoch, config)
    val updateMetadataRequest = extractUpdateMetadataRequest(request, config)

    val stopReplicaRequestVersion: Short = {
      if (config.interBrokerProtocolVersion >= KAFKA_3_0_IV1) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 3
      else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")
    }
    val stopReplicaRequests = if (stopReplicaRequestVersion == 3) {
      extractStopReplicaRequestWithUngroupedPartitions(request, brokerEpoch, stopReplicaRequestVersion)
    } else {
      List(extractStopReplicaRequestWithTopicStates(request, stopReplicaRequestVersion))
    }

    LiDecomposedControlRequest(leaderAndIsrRequest, updateMetadataRequest, stopReplicaRequests)
  }

  private def extractLeaderAndIsrRequest(request: LiCombinedControlRequest, brokerEpoch: Long, config: KafkaConfig): Option[LeaderAndIsrRequest] = {
    val partitionsInRequest = request.leaderAndIsrPartitionStates()
    val leadersInRequest = request.liveLeaders()

    val effectivePartitionStates = new util.ArrayList[LeaderAndIsrPartitionState]()
    partitionsInRequest.forEach { partition =>
      if (partition.brokerEpoch() >= brokerEpoch)
        effectivePartitionStates.add(LiCombinedControlTransformer.restoreLeaderAndIsrPartition(partition))
    }

    if (effectivePartitionStates.isEmpty) {
      None
    } else {
      val leaderNodes = new util.ArrayList[Node]()
      leadersInRequest.forEach { leader =>
        leaderNodes.add(new Node(leader.brokerId(), leader.hostName(), leader.port()))
      }

      val leaderAndIsrRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_3_0_IV1) 6
        else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 5
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      // the LiCombinedControl request will only include incremental LeaderAndIsr requests
      val topicIds: util.Map[String, Uuid] = if (request.version() >= 1) {
        request.leaderAndIsrTopicIds();
      } else {
        util.Collections.emptyMap();
      }

      Some(new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, request.controllerId(), request.controllerEpoch(),
        request.brokerEpoch(), request.maxBrokerEpoch(), effectivePartitionStates, topicIds, leaderNodes,
        request.leaderAndIsrType() == LeaderAndIsrRequestType.FULL.code()
      ).build())
    }
  }

  private def extractUpdateMetadataRequest(request: LiCombinedControlRequest, config: KafkaConfig): Option[UpdateMetadataRequest] = {
    val partitionsInRequest = request.updateMetadataPartitionStates()
    val brokersInRequest = request.liveBrokers()

    val effectivePartitionStates = new util.ArrayList[UpdateMetadataPartitionState]()
    partitionsInRequest.forEach { partition =>
      effectivePartitionStates.add(LiCombinedControlTransformer.restoreUpdateMetadataPartition(partition))
    }

    val liveBrokers = new util.ArrayList[UpdateMetadataBroker]()
    brokersInRequest.forEach(broker => liveBrokers.add(LiCombinedControlTransformer.restoreUpdateMetadataBroker(broker)))

    if (effectivePartitionStates.isEmpty && liveBrokers.isEmpty) {
      None
    } else {
      val updateMetadataRequestVersion: Short =
        if (config.interBrokerProtocolVersion >= KAFKA_3_0_IV1) 8
        else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 7
        else throw new IllegalStateException("The inter.broker.protocol.version config should not be smaller than 2.4-IV1")

      val topicIds: util.Map[String, Uuid] = if (request.version() >= 1) {
        request.updateMetadataTopicIds();
      } else {
        util.Collections.emptyMap();
      }

      Some(new UpdateMetadataRequest.Builder(updateMetadataRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
        request.maxBrokerEpoch(), effectivePartitionStates, liveBrokers, topicIds).build())
    }
  }

  private def extractStopReplicaRequestWithTopicStates(request: LiCombinedControlRequest, stopReplicaRequestVersion: Short): StopReplicaRequest = {
    // for StopReplicaRequests versions 4+, the deletePartitions flag on the top level is not used
    val defaultDeletePartitions = false
    val stopReplicaTopicStates = new util.ArrayList[StopReplicaTopicState]()

    request.stopReplicaTopicStates().forEach{topicState => {
      val partitionStates = new util.ArrayList[StopReplicaPartitionState]()
      topicState.partitionStates().forEach{ partitionState =>
        partitionStates.add(new StopReplicaPartitionState()
          .setPartitionIndex(partitionState.partitionIndex())
          .setDeletePartition(partitionState.deletePartitions())
          .setLeaderEpoch(partitionState.leaderEpoch())
        )
      }
      stopReplicaTopicStates.add(new StopReplicaTopicState()
        .setTopicName(topicState.topicName())
        .setPartitionStates(partitionStates))
    }
    }
    new StopReplicaRequest.Builder(stopReplicaRequestVersion, request.controllerId(), request.controllerEpoch(),
      request.brokerEpoch(),
      request.maxBrokerEpoch(), defaultDeletePartitions, stopReplicaTopicStates).build()
  }

  // extractStopReplicaRequestWithUngroupedPartitions could possible return two StopReplicaRequests
  // the first one with the deletePartitions field set to true, and the second one with the deletePartitions field set to false
  private def extractStopReplicaRequestWithUngroupedPartitions(request: LiCombinedControlRequest, brokerEpoch: Long, stopReplicaRequestVersion: Short): List[StopReplicaRequest] = {
    val partitionsInRequest = request.stopReplicaPartitionStates()

    val effectivePartitionStates = new util.ArrayList[LiCombinedControlRequestData.StopReplicaPartitionState]()
    partitionsInRequest.forEach { partition =>
      if (partition.brokerEpoch() >= brokerEpoch) {
        effectivePartitionStates.add(partition)
      }

    }
    if (effectivePartitionStates.isEmpty) {
      List.empty
    } else {
      val partitionsWithDelete = new mutable.ArrayBuffer[TopicPartition]
      val partitionsWithoutDelete = new mutable.ArrayBuffer[TopicPartition]
      effectivePartitionStates.forEach { partition =>
        val topicPartition = new TopicPartition(partition.topicName(), partition.partitionIndex())
        if (partition.deletePartitions()) {
          partitionsWithDelete += topicPartition
        } else {
          partitionsWithoutDelete += topicPartition
        }
      }

      var stopReplicaRequests: List[StopReplicaRequest] = List.empty

      if (!partitionsWithDelete.isEmpty) {
        stopReplicaRequests = getStopReplicaRequest(stopReplicaRequestVersion, request, true, partitionsWithDelete) :: stopReplicaRequests
      }

      if (!partitionsWithoutDelete.isEmpty) {
        stopReplicaRequests = getStopReplicaRequest(stopReplicaRequestVersion, request, false, partitionsWithoutDelete) :: stopReplicaRequests
      }

      stopReplicaRequests
    }
  }

  def getStopReplicaRequest(stopReplicaRequestVersion: Short, request: LiCombinedControlRequest,
                            deletePartitions: Boolean, partitions: mutable.ArrayBuffer[TopicPartition]): StopReplicaRequest = {
    val topicStates = partitions.groupBy(_.topic()).map{case (topicName, partitions) =>
      val partitionStates = partitions.map{part =>
        new StopReplicaPartitionState().setPartitionIndex(part.partition()).setDeletePartition(deletePartitions)
      }
      new StopReplicaTopicState().setTopicName(topicName).setPartitionStates(partitionStates.asJava)
    }
    val topicStatesList = new util.ArrayList[StopReplicaTopicState]()
    topicStatesList.addAll(topicStates.asJavaCollection)

    new StopReplicaRequest.Builder(stopReplicaRequestVersion, request.controllerId(), request.controllerEpoch(), request.brokerEpoch(),
      request.maxBrokerEpoch(), deletePartitions, topicStatesList).build()
  }

}
