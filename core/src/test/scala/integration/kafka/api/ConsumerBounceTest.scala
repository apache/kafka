/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.util.concurrent._
import java.util.Properties
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupMaxSizeReachedException
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.raft.KRaftConfigs
import org.apache.kafka.server.config.{ReplicationConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, Disabled, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.{Seq, mutable}

/**
 * Integration tests for the consumer that cover basic usage as well as server failures
 */
class ConsumerBounceTest extends AbstractConsumerTest with Logging {
  val maxGroupSize = 5

  // Time to process commit and leave group requests in tests when brokers are available
  val gracefulCloseTimeMs = Some(1000L)
  val executor: ScheduledExecutorService = Executors.newScheduledThreadPool(2)
  val consumerPollers: mutable.Buffer[ConsumerAssignmentPoller] = mutable.Buffer[ConsumerAssignmentPoller]()

  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

  override def generateConfigs: Seq[KafkaConfig] = {
    generateKafkaConfigs()
  }

  val testConfigs = Map[String, String](
    GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG -> "3", // don't want to lose offset
    GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG -> "1",
    GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG -> "10", // set small enough session timeout
    GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG -> "0",

    // Tests will run for CONSUMER and CLASSIC group protocol, so set the group max size property
    // required for each.
    GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SIZE_CONFIG -> maxGroupSize.toString,
    GroupCoordinatorConfig.GROUP_MAX_SIZE_CONFIG -> maxGroupSize.toString,

    ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG -> "false",
    ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG -> "true",
    ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG -> "50",
    KRaftConfigs.BROKER_HEARTBEAT_INTERVAL_MS_CONFIG -> "50",
    KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG -> "300",
  )

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    super.kraftControllerConfigs(testInfo).map(props => {
      testConfigs.foreachEntry((k, v) => props.setProperty(k, v))
      props
    })
  }

  private def generateKafkaConfigs(maxGroupSize: String = maxGroupSize.toString): Seq[KafkaConfig] = {
    val properties = new Properties
    testConfigs.foreachEntry((k, v) => properties.setProperty(k, v))
    FixedPortTestUtils.createBrokerConfigs(brokerCount, enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, properties))
  }

  @AfterEach
  override def tearDown(): Unit = {
    try {
      consumerPollers.foreach(_.shutdown())
      executor.shutdownNow()
      // Wait for any active tasks to terminate to ensure consumer is not closed while being used from another thread
      assertTrue(executor.awaitTermination(5000, TimeUnit.MILLISECONDS), "Executor did not terminate")
    } finally {
      super.tearDown()
    }
  }

  /**
    * If we have a running consumer group of size N, configure consumer.group.max.size = N-1 and restart all brokers,
    * the group should be forced to rebalance when it becomes hosted on a Coordinator with the new config.
    * Then, 1 consumer should be left out of the group.
    */
  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedGroupProtocolNames)
  @MethodSource(Array("getTestGroupProtocolParametersAll"))
  @Disabled // TODO: To be re-enabled once we can make it less flaky (KAFKA-13421)
  def testRollingBrokerRestartsWithSmallerMaxGroupSizeConfigDisruptsBigGroup(groupProtocol: String): Unit = {
    val group = "group-max-size-test"
    val topic = "group-max-size-test"
    val maxGroupSize = 2
    val consumerCount = maxGroupSize + 1
    val partitionCount = consumerCount * 2

    this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "60000")
    if (groupProtocol.equalsIgnoreCase(GroupProtocol.CLASSIC.name)) {
      this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    }
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val partitions = createTopicPartitions(topic, numPartitions = partitionCount, replicationFactor = brokerCount)

    addConsumersToGroupAndWaitForGroupAssignment(consumerCount, mutable.Buffer[Consumer[Array[Byte], Array[Byte]]](),
      consumerPollers, List[String](topic), partitions, group)

    // roll all brokers with a lesser max group size to make sure coordinator has the new config
    val newConfigs = generateKafkaConfigs(maxGroupSize.toString)
    for (serverIdx <- brokerServers.indices) {
      killBroker(serverIdx)
      val config = newConfigs(serverIdx)
      servers(serverIdx) = createBroker(config, time = brokerTime(config.brokerId))
      restartDeadBrokers()
    }

    def raisedExceptions: Seq[Throwable] = {
      consumerPollers.flatten(_.thrownException)
    }

    // we are waiting for the group to rebalance and one member to get kicked
    TestUtils.waitUntilTrue(() => raisedExceptions.nonEmpty,
      msg = "The remaining consumers in the group could not fetch the expected records", 10000L)

    assertEquals(1, raisedExceptions.size)
    assertTrue(raisedExceptions.head.isInstanceOf[GroupMaxSizeReachedException])
  }

  private def createTopicPartitions(topic: String, numPartitions: Int, replicationFactor: Int,
                                    topicConfig: Properties = new Properties): Set[TopicPartition] = {
    createTopic(topic, numPartitions = numPartitions, replicationFactor = replicationFactor, topicConfig = topicConfig)
    Range(0, numPartitions).map(part => new TopicPartition(topic, part)).toSet
  }
}
