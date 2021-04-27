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

package kafka.server

import kafka.api.LeaderAndIsr
import kafka.network.SocketServer
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class StopReplicaRequestTest extends BaseRequestTest {
  override val logDirCount = 2
  override val brokerCount: Int = 1

  val topic = "topic"
  val partitionNum = 2
  val tp0 = new TopicPartition(topic, 0)
  val tp1 = new TopicPartition(topic, 1)

  @Test
  def testStopReplicaRequest(): Unit = {
    createTopic(topic, partitionNum, 1)
    TestUtils.generateAndProduceMessages(servers, topic, 10)

    val server = servers.head
    val offlineDir = server.logManager.getLog(tp1).get.dir.getParent
    server.replicaManager.handleLogDirFailure(offlineDir, sendZkNotification = false)

    val topicId = server.metadataCache.getTopicId(topic)

    for (version <- ApiKeys.STOP_REPLICA.oldestVersion() to ApiKeys.STOP_REPLICA.latestVersion()) {
      val topicStates = Seq(
        new StopReplicaTopicState()
          .setTopicName(tp0.topic())
          .setTopicId(topicId)
          .setPartitionStates(Seq(new StopReplicaPartitionState()
            .setPartitionIndex(tp0.partition())
            .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 2)
            .setDeletePartition(true)).asJava),
        new StopReplicaTopicState()
          .setTopicName(tp1.topic())
          .setTopicId(topicId)
          .setPartitionStates(Seq(new StopReplicaPartitionState()
            .setPartitionIndex(tp1.partition())
            .setLeaderEpoch(LeaderAndIsr.initialLeaderEpoch + 2)
            .setDeletePartition(true)).asJava)
      ).asJava

      for (_ <- 1 to 2) {
        val request = new StopReplicaRequest.Builder(version.toShort,
          server.config.brokerId, server.replicaManager.controllerEpoch, server.kafkaController.brokerEpoch,
          false, topicStates).build()
        val response = sendStopReplicaRequest(request, destination = Some(brokerSocketServer(0)))
        val partitionErrors = response.partitionErrors.asScala
        assertEquals(Some(Errors.NONE.code),
          if (version < 4) {
            partitionErrors.find(pe => pe.topicName == tp0.topic && pe.partitionIndex == tp0.partition).map(_.errorCode)
          } else {
            partitionErrors.find(pe => pe.topicId() == topicId && pe.partitionIndex == tp0.partition).map(_.errorCode)
          })

        assertEquals(Some(Errors.KAFKA_STORAGE_ERROR.code),
          if (version < 4) {
            partitionErrors.find(pe => pe.topicName == tp1.topic && pe.partitionIndex == tp1.partition).map(_.errorCode)
          } else {
            partitionErrors.find(pe => pe.topicId() == topicId && pe.partitionIndex == tp1.partition).map(_.errorCode)
          })
      }
    }
  }

  private def sendStopReplicaRequest(request: StopReplicaRequest, destination: Option[SocketServer]/* = None*/): StopReplicaResponse = {
    connectAndReceive[StopReplicaResponse](request, destination = destination.getOrElse(anySocketServer))
  }

}
