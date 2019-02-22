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
package kafka.admin

import org.apache.kafka.common.errors.{InvalidReplicaAssignmentException, InvalidTopicException, TopicExistsException}
import org.apache.kafka.common.metrics.Quota
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.{After, Before, Test}
import java.util.Properties

import kafka.utils._
import kafka.zk.{ConfigEntityZNode, ZooKeeperTestHarness}
import kafka.utils.{Logging, TestUtils, ZkUtils}
import kafka.server.{ConfigType, KafkaConfig, KafkaServer}

import scala.collection.{Map, immutable}
import org.apache.kafka.common.security.JaasUtils

import scala.collection.JavaConverters._

@deprecated("This test has been deprecated and will be removed in a future release.", "1.1.0")
class AdminTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  var servers: Seq[KafkaServer] = Seq()
  var zkUtils: ZkUtils = null

  @Before
  override def setUp() {
    super.setUp()
    zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled))
  }

  @After
  override def tearDown() {
    if (zkUtils != null)
     CoreUtils.swallow(zkUtils.close(), this)
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testManualReplicaAssignment() {
    val brokers = List(0, 1, 2, 3, 4)
    TestUtils.createBrokersInZk(zkClient, brokers)

    // duplicate brokers
    intercept[InvalidReplicaAssignmentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, "test", Map(0->Seq(0,0)))
    }

    // inconsistent replication factor
    intercept[InvalidReplicaAssignmentException] {
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, "test", Map(0->Seq(0,1), 1->Seq(0)))
    }

    // good assignment
    val assignment = Map(0 -> List(0, 1, 2),
                         1 -> List(1, 2, 3))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, "test", assignment)
    val found = zkUtils.getPartitionAssignmentForTopics(Seq("test"))
    assertEquals(assignment, found("test"))
  }

  @Test
  def testTopicCreationInZK() {
    val expectedReplicaAssignment = Map(
      0  -> List(0, 1, 2),
      1  -> List(1, 2, 3),
      2  -> List(2, 3, 4),
      3  -> List(3, 4, 0),
      4  -> List(4, 0, 1),
      5  -> List(0, 2, 3),
      6  -> List(1, 3, 4),
      7  -> List(2, 4, 0),
      8  -> List(3, 0, 1),
      9  -> List(4, 1, 2),
      10 -> List(1, 2, 3),
      11 -> List(1, 3, 4)
    )
    val leaderForPartitionMap = immutable.Map(
      0 -> 0,
      1 -> 1,
      2 -> 2,
      3 -> 3,
      4 -> 4,
      5 -> 0,
      6 -> 1,
      7 -> 2,
      8 -> 3,
      9 -> 4,
      10 -> 1,
      11 -> 1
    )
    val topic = "test"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    // create leaders for all partitions
    TestUtils.makeLeaderForPartition(zkClient, topic, leaderForPartitionMap, 1)
    val actualReplicaList = leaderForPartitionMap.keys.toArray.map(p => p -> zkUtils.getReplicasForPartition(topic, p)).toMap
    assertEquals(expectedReplicaAssignment.size, actualReplicaList.size)
    for(i <- 0 until actualReplicaList.size)
      assertEquals(expectedReplicaAssignment.get(i).get, actualReplicaList(i))

    intercept[TopicExistsException] {
      // shouldn't be able to create a topic that already exists
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, expectedReplicaAssignment)
    }
  }

  @Test
  def testTopicCreationWithCollision() {
    val topic = "test.topic"
    val collidingTopic = "test_topic"
    TestUtils.createBrokersInZk(zkClient, List(0, 1, 2, 3, 4))
    // create the topic
    AdminUtils.createTopic(zkUtils, topic, 3, 1)

    intercept[InvalidTopicException] {
      // shouldn't be able to create a topic that collides
      AdminUtils.createTopic(zkUtils, collidingTopic, 3, 1)
    }
  }

  @Test
  def testConcurrentTopicCreation() {
    val topic = "test.topic"

    // simulate the ZK interactions that can happen when a topic is concurrently created by multiple processes
    val zkMock: ZkUtils = EasyMock.createNiceMock(classOf[ZkUtils])
    EasyMock.expect(zkMock.pathExists(s"/brokers/topics/$topic")).andReturn(false)
    EasyMock.expect(zkMock.getAllTopics).andReturn(Seq("some.topic", topic, "some.other.topic"))
    EasyMock.replay(zkMock)

    intercept[TopicExistsException] {
      AdminUtils.validateCreateOrUpdateTopic(zkMock, topic, Map.empty, new Properties, update = false)
    }
  }

  /**
   * This test simulates a client config change in ZK whose notification has been purged.
   * Basically, it asserts that notifications are bootstrapped from ZK
   */
  @Test
  def testBootstrapClientIdConfig() {
    val clientId = "my-client"
    val props = new Properties()
    props.setProperty("producer_byte_rate", "1000")
    props.setProperty("consumer_byte_rate", "2000")

    // Write config without notification to ZK.
    val configMap = Map[String, String] ("producer_byte_rate" -> "1000", "consumer_byte_rate" -> "2000")
    val map = Map("version" -> 1, "config" -> configMap.asJava)
    zkUtils.updatePersistentPath(ConfigEntityZNode.path(ConfigType.Client, clientId), Json.encodeAsString(map.asJava))

    val configInZk: Map[String, Properties] = AdminUtils.fetchAllEntityConfigs(zkUtils, ConfigType.Client)
    assertEquals("Must have 1 overridden client config", 1, configInZk.size)
    assertEquals(props, configInZk(clientId))

    // Test that the existing clientId overrides are read
    val server = TestUtils.createServer(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnect)))
    servers = Seq(server)
    assertEquals(new Quota(1000, true), server.dataPlaneRequestProcessor.quotas.produce.quota("ANONYMOUS", clientId))
    assertEquals(new Quota(2000, true), server.dataPlaneRequestProcessor.quotas.fetch.quota("ANONYMOUS", clientId))
  }

  @Test
  def testGetBrokerMetadatas() {
    // broker 4 has no rack information
    val brokerList = 0 to 5
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 5 -> "rack3")
    val brokerMetadatas = toBrokerMetadata(rackInfo, brokersWithoutRack = brokerList.filterNot(rackInfo.keySet))
    TestUtils.createBrokersInZk(brokerMetadatas, zkClient)

    val processedMetadatas1 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Disabled)
    assertEquals(brokerList, processedMetadatas1.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas1.map(_.rack))

    val processedMetadatas2 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Safe)
    assertEquals(brokerList, processedMetadatas2.map(_.id))
    assertEquals(List.fill(brokerList.size)(None), processedMetadatas2.map(_.rack))

    intercept[AdminOperationException] {
      AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Enforced)
    }

    val partialList = List(0, 1, 2, 3, 5)
    val processedMetadatas3 = AdminUtils.getBrokerMetadatas(zkUtils, RackAwareMode.Enforced, Some(partialList))
    assertEquals(partialList, processedMetadatas3.map(_.id))
    assertEquals(partialList.map(rackInfo), processedMetadatas3.flatMap(_.rack))

    val numPartitions = 3
    AdminUtils.createTopic(zkUtils, "foo", numPartitions, 2, rackAwareMode = RackAwareMode.Safe)
    val assignment = zkUtils.getReplicaAssignmentForTopics(Seq("foo"))
    assertEquals(numPartitions, assignment.size)
  }
}
