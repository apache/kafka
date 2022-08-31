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

package kafka.integration

import kafka.api.IntegrationTestHarness
import kafka.server.HostedPartition.Online
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.Properties

class PartitionLoggingTest extends IntegrationTestHarness{
  override protected def brokerCount: Int = 4
  val topic = "test"
  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    // create the test topic with all the brokers as replicas
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
    createTopic(topic, 1, brokerCount, topicConfig)
  }

  @Test
  def loggingTest(): Unit = {
    // shutdown 2 brokers to make sure the partition go under MinISR
    val brokersToShutdown = servers.filter{s => s.config.brokerId == 2 || s.config.brokerId == 3}

    val tp = new TopicPartition(topic, 0)

    // wait for the partition to enter the UnderMinISR state
    brokersToShutdown.foreach(_.shutdown())
    waitForPartitionUnderMinISRState(tp, true)

    // wait for the partition to exit the UnderMinISR state
    brokersToShutdown.foreach(_.startup())
    waitForPartitionUnderMinISRState(tp, false)
  }


  private def waitForPartitionUnderMinISRState(tp: TopicPartition, shouldBeUnderMinISR: Boolean): Unit = {
    TestUtils.waitUntilTrue(() => {
      val leaderIdOpt = zkClient.getLeaderForPartition(tp)
      assertTrue(leaderIdOpt.isDefined, "Leader should exist for partition [test,0]")
      val leader = servers.filter(s => s.config.brokerId == leaderIdOpt.get).last
      leader.replicaManager.getPartition(tp) match {
        case Online(partition) => {
          partition.isUnderMinIsr == shouldBeUnderMinISR
        }
        case _ => false
      }
    }, "the partition's UnderMinISR state should be " + shouldBeUnderMinISR)
  }
}
