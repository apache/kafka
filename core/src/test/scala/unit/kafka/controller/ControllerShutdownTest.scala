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

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.log4j.Logger
import org.junit.After
import org.junit.Test
import org.scalatest.Assertions.fail

class ControllerShutdownTest extends KafkaServerTestHarness with Logging {
  val log = Logger.getLogger(classOf[ControllerShutdownTest])
  val numNodes = 2
  val topic = "topic1"
  val metrics = new Metrics()

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    this.metrics.close()
  }

  override def generateConfigs = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps)

  @Test
  def testCorrectControllerResignation(): Unit = {
    val initialController = servers.find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    // Create topic with one partition
    createTopic(topic, 1, 1)
    val topicPartition = new TopicPartition("topic1", 0)


    TestUtils.waitUntilTrue(() =>
      initialController.controllerContext.partitionsInState(OnlinePartition).contains(topicPartition),
      s"Partition $topicPartition did not transition to online state")

    val activeController = servers.filter(_.kafkaController.isActive).head
    val controllerEpoch = activeController.kafkaController.epoch
    // manually force shutdown controller
    activeController.kafkaController.shutdown()

    TestUtils.waitUntilTrue(() => {
      servers.filterNot(_ == activeController).exists { server =>
        server.kafkaController.isActive && server.kafkaController.epoch > controllerEpoch
      }
    }, "Failed to find updated controller")

  }
}