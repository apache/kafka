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

import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.junit.Assert._
import org.junit.Test
import collection.JavaConverters._


class StopReplicaRequestTest extends BaseRequestTest {
  override val logDirCount = 2
  override val numBrokers: Int = 1

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

    for (i <- 1 to 2) {
      val request1 = new StopReplicaRequest.Builder(1,
        server.config.brokerId, server.replicaManager.controllerEpoch, server.kafkaController.brokerEpoch,
        true, Set(tp0, tp1).asJava).build()
      val response1 = connectAndSend(request1, ApiKeys.STOP_REPLICA, controllerSocketServer)
      val partitionErrors1 = StopReplicaResponse.parse(response1, request1.version).responses()
      assertEquals(Errors.NONE, partitionErrors1.get(tp0))
      assertEquals(Errors.KAFKA_STORAGE_ERROR, partitionErrors1.get(tp1))
    }
  }

}
