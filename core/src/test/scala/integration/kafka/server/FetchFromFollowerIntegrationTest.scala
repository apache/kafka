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

import kafka.server.{BaseFetchRequestTest, BrokerTopicStats, KafkaConfig}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.ApiKeys
import org.junit.jupiter.api.{AfterEach, BeforeEach, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties

class FetchFromFollowerIntegrationTest extends BaseFetchRequestTest {
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

  @BeforeEach
  def initializeFetchFromFollowerCluster(): Unit = {
    // Create a 2 broker cluster where broker 0 is the leader and 1 is the follower.

    admin = TestUtils.createAdminClient(brokers, listenerName)
    TestUtils.createTopicWithAdminRaw(
      admin,
      topic,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )
    TestUtils.generateAndProduceMessages(brokers, topic, initialMessages)
  }

  @AfterEach
  def close(): Unit = {
    if (admin != null) {
      admin.close()
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  @Timeout(30)
  def testFollowerCompleteDelayedPurgatoryOnReplication(quorum: String): Unit = {
    TestUtils.generateAndProduceMessages(brokers, topic, nMessages)
    // set fetch.max.wait.ms to a value (45 seconds) greater than the timeout (30 seconds) to ensure that the
    // test only passes when the delayed fetch purgatory is completed after successfully replicating from the leader.

    val totalMessages = initialMessages + nMessages
    val topicPartition = new TopicPartition(topic, 0)
    val offsetMap = Map[TopicPartition, Long](
      topicPartition -> (totalMessages - 1)
    )

    val fetchRequest = createFetchRequest(
      maxResponseBytes = 1000,
      maxPartitionBytes = 1000,
      Seq(topicPartition),
      offsetMap,
      ApiKeys.FETCH.latestVersion(),
      maxWaitMs = 45000,
      minBytes = 1
    )
    sendFetchRequest(followerBrokerId, fetchRequest)
  }
}
