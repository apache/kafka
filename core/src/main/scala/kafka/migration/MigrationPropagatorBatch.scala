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
package kafka.migration

import kafka.controller.{AbstractControllerBrokerRequestBatch, ControllerChannelContext, ControllerChannelManager, StateChangeLogger}
import kafka.server.KafkaConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.apache.kafka.server.common.MetadataVersion

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

sealed class MigrationPropagatorBatch(
  config: KafkaConfig,
  metadataProvider: () => ControllerChannelContext,
  metadataVersionProvider: () => MetadataVersion,
  controllerChannelManager: ControllerChannelManager,
  stateChangeLogger: StateChangeLogger
) extends AbstractControllerBrokerRequestBatch(
  config,
  metadataProvider,
  metadataVersionProvider,
  stateChangeLogger,
  kraftController = true,
) {
  override def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest], callback: AbstractResponse => Unit): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

  override def handleLeaderAndIsrResponse(response: LeaderAndIsrResponse, broker: Int): Unit = {
    if (response.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${response.error} in LeaderAndIsr " +
        s"response $response from broker $broker")
      return
    }
    val partitionErrors = response.partitionErrors(
      metadataProvider().topicIds.map { case (id, string) => (string, id) }.asJava)
    val offlineReplicas = new ArrayBuffer[TopicPartition]()
    partitionErrors.forEach{ case(tp, error) =>
      if (error == Errors.KAFKA_STORAGE_ERROR) {
        offlineReplicas += tp
      }
    }
    if (offlineReplicas.nonEmpty) {
      stateChangeLogger.error(s"Found ${offlineReplicas.mkString(",")} on broker $broker as offline")
    }
  }

  override def handleUpdateMetadataResponse(response: UpdateMetadataResponse, broker: Int): Unit = {
    if (response.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${response.error} in UpdateMetadata " +
        s"response $response from broker $broker")
    }
  }

  override def handleStopReplicaResponse(response: StopReplicaResponse, broker: Int,
                                         partitionErrorsForDeletingTopics: Map[TopicPartition, Errors]): Unit = {
    if (response.error() != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${response.error} in StopReplica " +
        s"response $response from broker $broker")
    }
    partitionErrorsForDeletingTopics.foreach{ case(tp, error) =>
      if (error != Errors.NONE) {
        stateChangeLogger.error(s"Received error $error in StopReplica request for partition $tp " +
          s"from broker $broker")
      }
    }
  }
}