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

import kafka.server.{BaseFetchRequestTest, KafkaConfig}
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests.FetchResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Properties
import scala.jdk.CollectionConverters._

class FetchFromFollowerIntegrationTest extends BaseFetchRequestTest {
  val numNodes = 2
  val numParts = 1

  val topic = "test-fetch-from-follower"
  val leaderBrokerId = 0
  val followerBrokerId = 1

  def overridingProps: Properties = {
    val props = new Properties
    props.put(KafkaConfig.NumPartitionsProp, numParts.toString)
    props
  }

  override def generateConfigs: collection.Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numNodes, zkConnectOrNull, enableControlledShutdown = false, enableFetchFromFollower = true)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  @Timeout(15)
  def testFollowerCompleteDelayedFetchesOnReplication(quorum: String): Unit = {
    // Create a topic with 2 replicas where broker 0 is the leader and 1 is the follower.
    val admin = createAdminClient()
    TestUtils.createTopicWithAdmin(
      admin,
      topic,
      brokers,
      replicaAssignment = Map(0 -> Seq(leaderBrokerId, followerBrokerId))
    )

    val version = ApiKeys.FETCH.latestVersion()
    val topicPartition = new TopicPartition(topic, 0)
    val offsetMap = Map(topicPartition -> 0L)

    // Set fetch.max.wait.ms to a value (20 seconds) greater than the timeout (15 seconds).
    // Send a fetch request before the record is replicated to ensure that the replication
    // triggers purgatory completion.
    val fetchRequest = createConsumerFetchRequest(
      maxResponseBytes = 1000,
      maxPartitionBytes = 1000,
      Seq(topicPartition),
      offsetMap,
      version,
      maxWaitMs = 20000,
      minBytes = 1
    )

    val socket = connect(brokerSocketServer(followerBrokerId))
    try {
      send(fetchRequest, socket)
      TestUtils.generateAndProduceMessages(brokers, topic, numMessages = 1)
      val response = receive[FetchResponse](socket, ApiKeys.FETCH, version)
      assertEquals(Errors.NONE, response.error)
      assertEquals(Map(Errors.NONE -> 2).asJava, response.errorCounts())
    } finally {
      socket.close()
    }
  }
}
