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
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.collection.Seq

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
    server.replicaManager.handleLogDirFailure(offlineDir, notifyController = false)

    val topicStates = Seq(
      new StopReplicaTopicState()
        .setTopicName(tp0.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(tp0.partition())
          .setLeaderEpoch(LeaderAndIsr.InitialLeaderEpoch + 2)
          .setDeletePartition(true)).asJava),
      new StopReplicaTopicState()
        .setTopicName(tp1.topic())
        .setPartitionStates(Seq(new StopReplicaPartitionState()
          .setPartitionIndex(tp1.partition())
          .setLeaderEpoch(LeaderAndIsr.InitialLeaderEpoch + 2)
          .setDeletePartition(true)).asJava)
    ).asJava

    for (_ <- 1 to 2) {
      val request1 = new StopReplicaRequest.Builder(ApiKeys.STOP_REPLICA.latestVersion,
        server.config.brokerId, server.replicaManager.controllerEpoch, server.kafkaController.brokerEpoch,
        false, topicStates).build()
      val response1 = connectAndReceive[StopReplicaResponse](request1, destination = controllerSocketServer)
      val partitionErrors1 = response1.partitionErrors.asScala
      assertEquals(Some(Errors.NONE.code),
        partitionErrors1.find(pe => pe.topicName == tp0.topic && pe.partitionIndex == tp0.partition).map(_.errorCode))
      assertEquals(Some(Errors.KAFKA_STORAGE_ERROR.code),
        partitionErrors1.find(pe => pe.topicName == tp1.topic && pe.partitionIndex == tp1.partition).map(_.errorCode))
    }
  }
}
