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

import java.util.Properties
import java.util.concurrent.CountDownLatch

import kafka.admin.AdminUtils
import kafka.common.TopicAndPartition
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics.Metrics
import org.apache.log4j.Logger
import org.junit.{After, Test}

class ControllerFailoverTest extends KafkaServerTestHarness with Logging {
  val log = Logger.getLogger(classOf[ControllerFailoverTest])
  val numNodes = 2
  val numParts = 1
  val msgQueueSize = 1
  val topic = "topic1"
  val overridingProps = new Properties()
  val metrics = new Metrics()
  overridingProps.put(KafkaConfig.NumPartitionsProp, numParts.toString)

  override def generateConfigs() = TestUtils.createBrokerConfigs(numNodes, zkConnect)
    .map(KafkaConfig.fromProps(_, overridingProps))

  @After
  override def tearDown() {
    super.tearDown()
    this.metrics.close()
  }

  /**
   * See @link{https://issues.apache.org/jira/browse/KAFKA-2300}
   * for the background of this test case
   */
  @Test
  def testHandleIllegalStateException() {
    val initialController = servers.find(_.kafkaController.isActive).map(_.kafkaController).getOrElse {
      fail("Could not find controller")
    }
    val epochMap = servers.map(server => server.config.brokerId -> server.kafkaController.epoch).toMap
    // Create topic with one partition
    AdminUtils.createTopic(servers.head.zkUtils, topic, 1, 1)
    val topicPartition = TopicAndPartition("topic1", 0)
    TestUtils.waitUntilTrue(() =>
      initialController.partitionStateMachine.partitionsInState(OnlinePartition).contains(topicPartition),
      s"Partition $topicPartition did not transition to online state")

    // Wait until we have verified that we have resigned
    val latch = new CountDownLatch(1)
    val illegalStateEvent = ControllerTestUtils.createMockControllerEvent(ControllerState.BrokerChange, { () =>
      initialController.handleIllegalState(new IllegalStateException("Thrown for test purposes"))
      latch.await()
    })
    initialController.eventManager.put(illegalStateEvent)
    // Check that we have shutdown the scheduler (via onControllerResigned)
    TestUtils.waitUntilTrue(() => !initialController.kafkaScheduler.isStarted, "Scheduler was not shutdown")
    TestUtils.waitUntilTrue(() => zkUtils.readDataMaybeNull(ZkUtils.ControllerPath)._1.isEmpty,
      "Controller path was not removed")
    TestUtils.waitUntilTrue(() => initialController.getControllerID == -1, "Controller id was not set to -1")
    latch.countDown()

    TestUtils.waitUntilTrue(() => {
      servers.exists { server =>
        val previousEpoch = epochMap.get(server.config.brokerId).getOrElse {
          fail(s"Missing element in epoch map $epochMap")
        }
        server.kafkaController.isActive && previousEpoch < server.kafkaController.epoch
      }
    }, "Failed to find controller")

  }
}
