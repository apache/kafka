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

package kafka.controller

import com.google.common.collect.ImmutableMap
import kafka.api.KAFKA_3_0_IV0
import kafka.server.KafkaConfig
import kafka.utils.{LiDecomposedControlResponse, LiDecomposedControlResponseUtils, Logging}
import org.apache.kafka.common.message.LiCombinedControlRequestData
import org.apache.kafka.common.message.LiCombinedControlRequestData._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.LiCombinedControlTransformer
import org.apache.kafka.common.{Node, TopicIdPartition, TopicPartition, Uuid}

import java.util
import java.util.Map
import scala.collection.mutable

case class RequestControllerState(controllerId: Int, controllerEpoch: Int)

class ControllerRequestMerger(config: KafkaConfig) extends Logging {
  val liCombinedControlRequestVersion: Short =
    if (config.interBrokerProtocolVersion >= KAFKA_3_0_IV0) 1
    else 0

  var isFullLeaderAndIsr: Boolean = false
  val leaderAndIsrPartitionStates: mutable.Map[TopicIdPartition, util.LinkedList[LeaderAndIsrPartitionState]] = mutable.HashMap.empty
  var leaderAndIsrLiveLeaders: util.Collection[Node] = new util.ArrayList[Node]()

  val updateMetadataPartitionStates: mutable.Map[TopicIdPartition, UpdateMetadataPartitionState] = mutable.HashMap.empty
  var updateMetadataLiveBrokers: util.List[UpdateMetadataBroker] = new util.ArrayList[UpdateMetadataBroker]()

  val stopReplicaPartitionStates: mutable.Map[TopicIdPartition, util.LinkedList[StopReplicaPartitionState]] = mutable.HashMap.empty

  // If a controller resigns and becomes the active controller again, a new
  // ControllerRequestMerger object will be created for each RequestSendThread.
  // Thus the controllerState, once set, should not change for the lifetime of this object.
  var controllerState : RequestControllerState = null

  // Here we store one callback for the LeaderAndIsr response and one for the StopReplica response,
  // given all the requests of a particular type sent to the same broker always have the same callback.
  var leaderAndIsrCallback: AbstractResponse => Unit = null
  var stopReplicaCallback: AbstractResponse => Unit = null
  val aggregateTopicIds: util.Map[String, Uuid] = new util.HashMap[String, Uuid]()

  /**
   *@return This method should return true if the merging should continue, and false otherwise.
   */
  def addRequest(request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
    callback: AbstractResponse => Unit = null): Boolean = {
    val newControllerState = new RequestControllerState(request.controllerId(), request.controllerEpoch())
    if (controllerState != null) {
      if (!controllerState.equals(newControllerState)) {
        throw new IllegalStateException("The controller state in the ControllerRequestMerger should not change")
      }
    } else {
      controllerState = newControllerState
    }

    request match {
      case leaderAndIsrRequest : LeaderAndIsrRequest.Builder => addLeaderAndIsrRequest(leaderAndIsrRequest, callback)
      case updateMetadataRequest : UpdateMetadataRequest.Builder => addUpdateMetadataRequest(updateMetadataRequest)
      case stopReplicaRequest: StopReplicaRequest.Builder => addStopReplicaRequest(stopReplicaRequest, callback)
    }
  }

  def isLeaderAndIsrReplaceable(newState: LeaderAndIsrPartitionState, currentState: LeaderAndIsrPartitionState): Boolean = {
    newState.brokerEpoch() > currentState.brokerEpoch() || newState.leaderEpoch() > currentState.leaderEpoch()
  }

  def mergeLeaderAndIsrPartitionState(incomingState: LeaderAndIsrPartitionState,
    queuedStates: util.LinkedList[LeaderAndIsrPartitionState]): Unit = {
    // keep merging requests from the tail of the queued States
    while (!queuedStates.isEmpty && isLeaderAndIsrReplaceable(incomingState, queuedStates.getLast)) {
      queuedStates.pollLast()
    }
    val inserted = queuedStates.offerLast(incomingState)
    if (!inserted) {
      throw new IllegalStateException(s"Unable to insert LeaderAndIsrPartitionState $incomingState to the merger queue")
    }
  }

  private def mergeTopicIds(topicIds: Map[String, Uuid]): Unit = {
    // validate that the incoming topicIds is connistent with the existing aggregateTopicIds
    topicIds.forEach((topicName, topicId) => {
      val existingTopicId = aggregateTopicIds.get(topicName)
      if (existingTopicId != null && !existingTopicId.equals(topicId)) {
        warn(s"topic $topicName associated with the topic id $existingTopicId is given a new topic id $topicId " +
          "clearing relevant requests associated with previous topic ids")

        def clearObsoleteEntriesInMap(map: mutable.Map[TopicIdPartition, _]) = {
          val obsoletePartitions = map.filterKeys{
            topicIdPartition => topicIdPartition.topicPartition().topic().equals(topicName) && !topicIdPartition.topicId().equals(topicId)}
          obsoletePartitions.foreach{
            case (topicIdPartition, _) => map.remove(topicIdPartition)}
        }

        clearObsoleteEntriesInMap(leaderAndIsrPartitionStates)
        clearObsoleteEntriesInMap(updateMetadataPartitionStates)
        clearObsoleteEntriesInMap(stopReplicaPartitionStates)
      }
    })
    aggregateTopicIds.putAll(topicIds)
  }

  /**
   *@return This method should return true if the merging should continue, and false otherwise.
   */
  private def addLeaderAndIsrRequest(request: LeaderAndIsrRequest.Builder,
                                     callback: AbstractResponse => Unit): Boolean = {
    mergeTopicIds(request.topicIds())

    if (request.`type`().equals(LeaderAndIsrRequestType.FULL)) {
      // There should be no other LeaderAndISR partitions in the merger when a full LeaderAndISR request is sent
      if (!leaderAndIsrPartitionStates.isEmpty) {
        throw new IllegalStateException("A Full LeaderAndISR request is received " +
          "while some LeaderAndISR partitions are already in the ControllerRequest merger")
      }
      isFullLeaderAndIsr = true
    }

    request.partitionStates().forEach{partitionState => {
      val transformedPartitionState = LiCombinedControlTransformer.transformLeaderAndIsrPartition(partitionState, request.maxBrokerEpoch())

      val topicName = partitionState.topicName()
      val topicIdPartition = new TopicIdPartition(aggregateTopicIds.getOrDefault(topicName, Uuid.ZERO_UUID),
        new TopicPartition(topicName, partitionState.partitionIndex()))
      val queuedStates = leaderAndIsrPartitionStates.getOrElseUpdate(topicIdPartition,
        new util.LinkedList[LeaderAndIsrPartitionState]())

      mergeLeaderAndIsrPartitionState(transformedPartitionState, queuedStates)

      // one LeaderAndIsr request renders the previous StopReplica requests non-applicable
      clearStopReplicaPartitionState(topicIdPartition)
    }}
    leaderAndIsrLiveLeaders = request.liveLeaders()
    leaderAndIsrCallback = callback
    // we should stop the merging when a full LeaderAndISR request is encountered
    !isFullLeaderAndIsr
  }

  private def clearLeaderAndIsrPartitionState(topicIdPartition: TopicIdPartition): Unit = {
    leaderAndIsrPartitionStates.remove(topicIdPartition)
  }

  /**
   *@return This method should return true if the merging should continue, and false otherwise.
   */
  private def addUpdateMetadataRequest(request: UpdateMetadataRequest.Builder): Boolean = {
    mergeTopicIds(request.topicIds())

    request.partitionStates().forEach{partitionState => {
      val transformedPartitionState = LiCombinedControlTransformer.transformUpdateMetadataPartition(partitionState)
      val topicName = partitionState.topicName()
      val topicIdPartition = new TopicIdPartition(aggregateTopicIds.getOrDefault(topicName, Uuid.ZERO_UUID),
        new TopicPartition(topicName, partitionState.partitionIndex()))

      updateMetadataPartitionStates.put(topicIdPartition, transformedPartitionState)
    }}

    updateMetadataLiveBrokers.clear()
    request.liveBrokers().forEach{liveBroker =>
      updateMetadataLiveBrokers.add(LiCombinedControlTransformer.transformUpdateMetadataBroker(liveBroker))
    }
    true
  }

  def isStopReplicaReplaceable(newState: StopReplicaPartitionState, currentState: StopReplicaPartitionState): Boolean = {
    newState.brokerEpoch() > currentState.brokerEpoch()
  }

  def mergeStopReplicaPartitionState(incomingState: StopReplicaPartitionState,
    queuedStates: util.LinkedList[StopReplicaPartitionState]): Unit = {
    // keep merging requests from the tail of the queued States
    while (!queuedStates.isEmpty && isStopReplicaReplaceable(incomingState, queuedStates.getLast)) {
      queuedStates.pollLast()
    }
    val inserted = queuedStates.offerLast(incomingState)
    if (!inserted) {
      throw new IllegalStateException(s"Unable to insert LeaderAndIsrPartitionState $incomingState to the merger queue")
    }
  }

  /**
   *@return This method should return true if the merging should continue, and false otherwise.
   */
  private def addStopReplicaRequest(request: StopReplicaRequest.Builder,
                                    callback: AbstractResponse => Unit): Boolean = {
    request.topicStates().forEach{topicState => {
      topicState.partitionStates().forEach{ partitionState =>
        val topicName = topicState.topicName()
        val partition = new TopicIdPartition(aggregateTopicIds.getOrDefault(topicName, Uuid.ZERO_UUID),
          new TopicPartition(topicName, partitionState.partitionIndex()))

        val queuedStates = stopReplicaPartitionStates.getOrElseUpdate(partition,
          new util.LinkedList[StopReplicaPartitionState]())
        val transformedPartitionState = new StopReplicaPartitionState()
          .setTopicName(partition.topicPartition().topic())
          .setPartitionIndex(partition.topicPartition().partition())
          .setDeletePartitions(partitionState.deletePartition())
          .setBrokerEpoch(request.brokerEpoch())

          if (liCombinedControlRequestVersion >= 1) {
            transformedPartitionState.setLeaderEpoch(partitionState.leaderEpoch())
          }

        mergeStopReplicaPartitionState(transformedPartitionState, queuedStates)

        // one stop replica request renders all previous LeaderAndIsr requests non-applicable
        clearLeaderAndIsrPartitionState(partition)
      }
    }}

    // Here a later request's callback may be overwriting a previous request's callback.
    // To preserve correctness, we require that the later callback and the earlier callback
    // must have the same sideeffects when they are provided with the same StopReplica response.
    stopReplicaCallback = callback
    true
  }
  private def clearStopReplicaPartitionState(topicIdPartition: TopicIdPartition): Unit = {
    stopReplicaPartitionStates.remove(topicIdPartition)
  }

  private def pollLatestLeaderAndIsrPartitions() : util.List[LiCombinedControlRequestData.LeaderAndIsrPartitionState] = {
    val latestPartitionStates = new util.ArrayList[LiCombinedControlRequestData.LeaderAndIsrPartitionState]()

    leaderAndIsrPartitionStates.keySet.foreach{
      partition  => {
        val partitionStateList = leaderAndIsrPartitionStates.get(partition).get
        val latestState = partitionStateList.poll()
        // clear the map if the queued states have been depleted for the given partition
        if (partitionStateList.isEmpty) {
          leaderAndIsrPartitionStates.remove(partition)
        }

        latestPartitionStates.add(latestState)
      }
    }

    // clear the isFullLeaderAndISR flag
    isFullLeaderAndIsr = false
    latestPartitionStates
  }

  private def pollLatestStopReplicaPartitions(): util.List[StopReplicaPartitionState] = {
    val latestPartitionStates = new util.ArrayList[StopReplicaPartitionState]()
    for (partition <- stopReplicaPartitionStates.keySet) {
      val partitionStates = stopReplicaPartitionStates.get(partition).get
      val latestState = partitionStates.poll()
      if (partitionStates.isEmpty) {
        stopReplicaPartitionStates.remove(partition)
      }

      latestPartitionStates.add(latestState)
    }
    latestPartitionStates
  }

  private def pollLatestUpdateMetadataInfo(): (util.List[UpdateMetadataPartitionState], util.List[UpdateMetadataBroker]) = {
    // since we don't maintain multiple versions of data for the UpdateMetadata partition states or the live brokers
    // it's guaranteed that, after each pollLatestRequest, the updateMetadata info becomes empty
    val latestPartitionStates = new util.ArrayList[UpdateMetadataPartitionState](updateMetadataPartitionStates.size)
    updateMetadataPartitionStates.foreach{
      case (_, latestState) => latestPartitionStates.add(latestState)
    }
    updateMetadataPartitionStates.clear()

    val liveBrokers = new util.ArrayList[UpdateMetadataBroker](updateMetadataLiveBrokers)
    updateMetadataLiveBrokers.clear()

    (latestPartitionStates, liveBrokers)
  }

  private def hasPendingLeaderAndIsrRequests: Boolean = !leaderAndIsrPartitionStates.isEmpty
  private def hasPendingUpdateMetadataRequests: Boolean = {
    (!updateMetadataPartitionStates.isEmpty) || (!updateMetadataLiveBrokers.isEmpty)
  }
  private def hasPendingStopReplicaRequests: Boolean = !stopReplicaPartitionStates.isEmpty

  def hasPendingRequests(): Boolean = {
    hasPendingLeaderAndIsrRequests || hasPendingUpdateMetadataRequests || hasPendingStopReplicaRequests
  }

  def pollLatestRequest(): LiCombinedControlRequest.Builder = {
    if (controllerState == null) {
      throw new IllegalStateException("No request has been added to the merger")
    } else {
      val (latestUpdateMetadataPartitions, liveBrokers) = pollLatestUpdateMetadataInfo()

      /**
       * note that calling the pollLatestLeaderAndIsrPartitions() method below will clear the isFullLeaderAndIsr field
       * thus we explicitly save the isFullLeaderAndIsr state before constructing the LiCombinedControlRequest
       */
      val fullLeaderAndIsr = isFullLeaderAndIsr
      val latestRequest = new LiCombinedControlRequest.Builder(
        liCombinedControlRequestVersion, controllerState.controllerId, controllerState.controllerEpoch,
        fullLeaderAndIsr, pollLatestLeaderAndIsrPartitions(), leaderAndIsrLiveLeaders,
        latestUpdateMetadataPartitions, liveBrokers,
        pollLatestStopReplicaPartitions(), ImmutableMap.copyOf(aggregateTopicIds)
      )
      if (!hasPendingRequests()) {
        clearAggregateState()
      }
      latestRequest
    }
  }

  def clearAggregateState(): Unit = {
    aggregateTopicIds.clear()
  }

  def triggerCallback(response: AbstractResponse): Unit = {
    // Currently, there is no callback for the UpdateMetadataResponse
    val LiDecomposedControlResponse(leaderAndIsrResponse, _, stopReplicaResponse) = {
      LiDecomposedControlResponseUtils.decomposeResponse(response.asInstanceOf[LiCombinedControlResponse])
    }
    if (leaderAndIsrCallback != null
      && (!leaderAndIsrResponse.data.partitionErrors.isEmpty ||
      !leaderAndIsrResponse.data().topics().isEmpty ||
      leaderAndIsrResponse.error() != Errors.NONE)) {
      // fire callback only if leaderAndIsrResponse is non-empty or contains errors
      leaderAndIsrCallback(leaderAndIsrResponse)
    }
    if (stopReplicaCallback != null
      && (!stopReplicaResponse.partitionErrors().isEmpty || stopReplicaResponse.error() != Errors.NONE)) {
      // fire callback only if stopReplicaResponse is non-empty or contains errors
      stopReplicaCallback(stopReplicaResponse)
    }
  }
}
