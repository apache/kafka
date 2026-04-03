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

package integration.kafka.api

import kafka.api.{IntegrationTestHarness, LeaderAndIsr}
import kafka.log.Log.offsetFromFile
import kafka.log.{LogSegment}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

import java.util.Properties

class CorruptedBrokersTest extends IntegrationTestHarness {
  val brokerCount = 3
  val controllerId = 0
  val broker1Id = 1
  val broker2Id = 2
  val topic = "topic1"
  val partition = 0
  val topicPartition = new TopicPartition(topic, partition)

  serverConfig.setProperty(KafkaConfig.LiDropCorruptedFilesEnableProp, "true")
  serverConfig.setProperty(KafkaConfig.LiLeaderElectionOnCorruptionWaitMsProp, "30000")
  serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
  serverConfig.setProperty(KafkaConfig.MinInSyncReplicasProp, "1")
  serverConfig.setProperty(KafkaConfig.UncleanLeaderElectionEnableProp, "true")

  producerConfig.setProperty(ProducerConfig.LINGER_MS_CONFIG, "0")
  producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "0")
  producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")

  override def modifyConfigs(props: Seq[Properties]): Unit = {
    super.modifyConfigs(props)
    // Make broker 0 controller
    props.head.setProperty(KafkaConfig.PreferredControllerProp, "true")
  }

  @Test
  def isrCleanupOnCorruptedBrokerStartup(): Unit = {
    val electedControllerId = TestUtils.waitUntilControllerElected(zkClient)
    val controller = getController().kafkaController
    assertTrue(electedControllerId == controllerId)

    createTopic(topic, Map(partition -> Seq(broker1Id, broker2Id)))

    val producer = createProducer()
    sendRecords(producer, 5)

    killBroker(broker1Id)
    TestUtils.waitUntilTrue(() => zkClient.getBroker(broker1Id).isEmpty, "Broker 1 failed to shut down.")

    killBroker(broker2Id)
    TestUtils.waitUntilTrue(() => zkClient.getBroker(broker2Id).isEmpty, "Broker 2 failed to shut down.")

    val partitionState = zkClient.getTopicPartitionState(topicPartition)
    assertTrue(partitionState.exists(state =>
      state.leaderAndIsr.isr == List(2) &&
        state.leaderAndIsr.leader == LeaderAndIsr.NoLeader))

    truncateLog(broker1Id, 3)
    truncateLog(broker2Id, 2)

    restartDeadBroker(broker2Id)

    TestUtils.waitUntilTrue(
      () => {
        zkClient.getCorruptedBrokers.get(2).exists(_.clearedFromIsrs)
      }, "Broker 2 did not get cleaned from ISRs")

    restartDeadBroker(broker1Id)

    TestUtils.waitUntilTrue(
      () => {
        zkClient.getCorruptedBrokers.get(1).exists(_.clearedFromIsrs)
      }, "Broker 1 did not get cleaned from ISRs")
  }

  def truncateLog(brokerId: Int, truncationOffset: Int): Unit = {
    val server = serverForId(brokerId).get
    val log = server.logManager.getLog(topicPartition).get
    val lastSegment = log.segments.lastSegment.get
    val segmentFile = lastSegment.log.file

    val baseOffset = offsetFromFile(segmentFile)
    val s2 = LogSegment.open(segmentFile.getParentFile, baseOffset, log.config, Time.SYSTEM)
    s2.truncateTo(truncationOffset)
    s2.close()
  }

  def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], recordCount: Int) {
    val futures = (0 until recordCount).map { index =>
      val record = new ProducerRecord(topic, partition, index.toString.getBytes, index.toString.getBytes)
      producer.send(record)
    }
    futures.foreach(_.get)
  }
}
