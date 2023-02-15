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

import com.yammer.metrics.core.Gauge
import kafka.api.IntegrationTestHarness
import kafka.metrics.KafkaYammerMetrics
import kafka.server.HostedPartition.Online
import kafka.utils.TestUtils
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

import java.util.Properties
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

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

  @Test
  def loggingOfflinePartitionTest(): Unit = {
    val controllerId = TestUtils.waitUntilControllerElected(zkClient)
    // create the test topic with three replicas
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    var nonControllerBrokerIds = servers.filter(s => s.config.brokerId != controllerId).map(_.config.brokerId)
    if (nonControllerBrokerIds.size > 3) {
      nonControllerBrokerIds = nonControllerBrokerIds.take(3)
    }
    assertEquals(3, nonControllerBrokerIds.size)
    val topicName = "test_3replicas"
    createTopic(topicName, Map(0 -> nonControllerBrokerIds), topicConfig)
    val tp = new TopicPartition(topicName, 0)
    checkPartitionOfflineExists(false, tp)

    // get the brokers that have the replicas
    val partitionStateOpt = zkClient.getTopicPartitionState(tp)
    assertTrue(partitionStateOpt.isDefined, "partitionState should exist for partition [test_3replicas,0]")
    assertEquals(3, partitionStateOpt.get.leaderAndIsr.isr.size, "isr should be 3 for partition [test_3replicas,0]")
    val brokersWithReplica = partitionStateOpt.get.leaderAndIsr.isr

    // shutdown brokers to put the partition offline
    val brokersToShutdown = servers.filter{s => brokersWithReplica.contains(s.config.brokerId)}
    val controller = servers.filter(s => s.config.brokerId == controllerId).head.kafkaController
    brokersToShutdown.map(broker => controller.skipControlledShutdownSafetyCheck(
      broker.config.brokerId, controller.controllerContext.liveBrokerIdAndEpochs(broker.config.brokerId), {case _ => {}}));
    brokersToShutdown(0).shutdown()
    waitForPartitionUnderMinISRState(tp, false)
    brokersToShutdown.takeRight(2).foreach(_.shutdown())
    checkPartitionOfflineExists(true, tp)

    // bring up the brokers and check replicas are alive
    brokersToShutdown.foreach(_.startup())
    waitForPartitionUnderMinISRState(tp, false)
    checkPartitionOfflineExists(false, tp)
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

  private def checkPartitionOfflineExists(offlinePartitionExist: Boolean, tp: TopicPartition): Unit = {
    TestUtils.waitUntilTrue(() => {
      val partitionStateOpt = zkClient.getTopicPartitionState(tp)
      offlinePartitionExist == (partitionStateOpt.get.leaderAndIsr.leader == -1)
    }, "the partition should have leader " + {if (offlinePartitionExist)  "equal -1" else "not equal -1"})

    val offlinePartitionsCountGauge = KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .find { case (k, _) => k.getName.endsWith("OfflinePartitionsCount") }
      .getOrElse(throw new AssertionError( "Unable to find metric OfflinePartitionsCount"))
      ._2.asInstanceOf[Gauge[Int]]

    // there are also other topics, e.g., 5 offset topics, so by shutting down brokers to result in offline partitions,
    // we do not check the exact number of offlinePartitionsCount, but only check whether it is greater than 0
    assertEquals(offlinePartitionExist, offlinePartitionsCountGauge.value() > 0)
  }
}
