/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package integration.kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.{Admin, NewPartitionReassignment}
import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.{Optional, Properties}
import scala.collection._
import scala.jdk.CollectionConverters._

class FetchFromFollowerIntegrationTest extends KafkaServerTestHarness {
  val numNodes = 2
  val numParts = 1
  val initialMessages = 100
  val nMessages = 100

  val topic = "test-fetch-from-follower"
  val leaderBrokerId = 0
  val followerBrokerId = 1
  var admin: Admin = null

  def overridingProps: Properties = {
    val props = new Properties
    props.put(KafkaConfig.NumPartitionsProp, numParts.toString)

    props
  }

  override def generateConfigs: collection.Seq[KafkaConfig] =
    TestUtils.createBrokerConfigs(numNodes, zkConnectOrNull, enableControlledShutdown = false, enableFetchFromFollower = true)
      .map(KafkaConfig.fromProps(_, overridingProps))

  // Create a 2 broker cluster where broker 0 is the leader and 1 is the follower.
  // Create a topic with a single partition. Access brokers via `brokers`
  @BeforeEach
  def initializeFetchFromFollowerCluster(): Unit = {
    createTopic(topic, numParts, numNodes)
    TestUtils.generateAndProduceMessages(brokers, topic, initialMessages)

    // Make broker 1 the follower
    admin = TestUtils.createAdminClient(brokers, listenerName)
    val topicDescription = TestUtils.describeTopic(admin, topic)
    topicDescription.partitions.forEach(partition => {
      // only reelect if leader is not broker 0
      if (partition.leader().id() != leaderBrokerId) {
        val reassignment = Map(new TopicPartition(topic, 0) -> Optional.of(
          new NewPartitionReassignment(List[Integer](leaderBrokerId, followerBrokerId).asJava))).asJava
        admin.alterPartitionReassignments(reassignment).all.get
        TestUtils.waitUntilTrue(() => {
          val reassigned = admin.listPartitionReassignments.reassignments.get
          reassigned.isEmpty
        }, "reassignment did not complete.")
        admin.electLeaders(ElectionType.PREFERRED, Set(new TopicPartition(topic, 0)).asJava).all.get
      }
    })

    // Check the log size for each broker so that we can distinguish between failures caused by replication issues
    // versus failures caused by the metrics
    val topicPartition = new TopicPartition(topic, 0)
    brokers.foreach { broker =>
      val log = broker.logManager.getLog(topicPartition)
      val brokerId = broker.config.brokerId
      val logSize = log.map(_.size)
      assertTrue(logSize.exists(_ > 0), s"Expected broker $brokerId to have a Log for $topicPartition with positive size, actual: $logSize")
    }
  }

  @AfterEach
  def close(): Unit = {
    if (admin != null) {
      admin.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testFollowerCompleteDelayedPurgatoryOnReplication(quorum: String): Unit = {
    TestUtils.generateAndProduceMessages(brokers, topic, nMessages)
    // set fetch.max.wait.ms to a value greater than test utils wait time (15 seconds) to ensure that the
    // delayed fetch purgatory is completed after successfully replicating from the leader.
    val messages = initialMessages + nMessages
    TestUtils.consumeTopicRecords(brokers, topic, messages, rackId = followerBrokerId.toString, fetchMaxWaitMs = 60000)
  }
}
