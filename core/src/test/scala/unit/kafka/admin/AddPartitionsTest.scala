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

import kafka.api.TopicMetadata
import kafka.cluster.Broker
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.kafka.common.errors.InvalidReplicaAssignmentException
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness

import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.PartitionInfo

import org.junit.{After, Before, Test}
import org.junit.Assert._

class AddPartitionsTest extends ZooKeeperTestHarness {
  var configs: Seq[KafkaConfig] = null
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]
  var brokers: Seq[Broker] = Seq.empty[Broker]

  val partitionId = 0

  val topic1 = "new-topic1"
  val topic1Assignment = Map(0->Seq(0,1))
  val topic2 = "new-topic2"
  val topic2Assignment = Map(0->Seq(1,2))
  val topic3 = "new-topic3"
  val topic3Assignment = Map(0->Seq(2,3,0,1))
  val topic4 = "new-topic4"
  val topic4Assignment = Map(0->Seq(0,3))
  val topic5 = "new-topic5"
  val topic5Assignment = Map(1->Seq(0,1))

  @Before
  override def setUp() {
    super.setUp()

    configs = (0 until 4).map(i => KafkaConfig.fromProps(TestUtils.createBrokerConfig(i, zkConnect, enableControlledShutdown = false)))
    // start all the servers
    servers = configs.map(c => TestUtils.createServer(c))
    brokers = servers.map(s => TestUtils.createBroker(s.config.brokerId, s.config.hostName, TestUtils.boundPort(s)))

    // create topics first
    createTopic(zkUtils, topic1, partitionReplicaAssignment = topic1Assignment, servers = servers)
    createTopic(zkUtils, topic2, partitionReplicaAssignment = topic2Assignment, servers = servers)
    createTopic(zkUtils, topic3, partitionReplicaAssignment = topic3Assignment, servers = servers)
    createTopic(zkUtils, topic4, partitionReplicaAssignment = topic4Assignment, servers = servers)
  }

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testWrongReplicaCount(): Unit = {
    try {
      AdminUtils.addPartitions(zkUtils, topic1, topic1Assignment, AdminUtils.getBrokerMetadatas(zkUtils), 2,
        Some(Map(0 -> Seq(0, 1), 1 -> Seq(0, 1, 2))))
      fail("Add partitions should fail")
    } catch {
      case _: InvalidReplicaAssignmentException => //this is good
    }
  }

  @Test
  def testMissingPartition0(): Unit = {
    try {
      AdminUtils.addPartitions(zkUtils, topic5, topic5Assignment, AdminUtils.getBrokerMetadatas(zkUtils), 2,
        Some(Map(1 -> Seq(0, 1), 2 -> Seq(0, 1, 2))))
      fail("Add partitions should fail")
    } catch {
      case e: AdminOperationException => //this is good
        assertTrue(e.getMessage.contains("Unexpected existing replica assignment for topic 'new-topic5', partition id 0 is missing"))
    }
  }

  @Test
  def testIncrementPartitions(): Unit = {
    AdminUtils.addPartitions(zkUtils, topic1, topic1Assignment, AdminUtils.getBrokerMetadatas(zkUtils), 3)
    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChanged(zkUtils, topic1, 1)
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkUtils, topic1, 2)
    val leader1FromZk = zkUtils.getLeaderForPartition(topic1, 1).get
    val leader2FromZk = zkUtils.getLeaderForPartition(topic1, 2).get
    assertEquals(leader1, leader1FromZk)
    assertEquals(leader2, leader2FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 2)
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)
    val metadataForTopic1 =
      AdminUtils.fetchTopicMetadata(
              Set(topic1), brokers.map(_.getBrokerEndPoint(listenerName)),
              "AddPartitionsTest-testIncrementPartitions", 2000).get(topic1).getOrElse(List())
    val partitionDataForTopic1 = metadataForTopic1.sortBy(_.partition)
    assertEquals(partitionDataForTopic1.size, 3)
    assertEquals(partitionDataForTopic1(1).partition, 1)
    assertEquals(partitionDataForTopic1(2).partition, 2)
    val replicas = partitionDataForTopic1(1).replicas
    assertEquals(replicas.size, 2)
    assert(replicas.contains(partitionDataForTopic1(1).leader))
  }

  @Test
  def testManualAssignmentOfReplicas(): Unit = {
    // Add 2 partitions
    AdminUtils.addPartitions(zkUtils, topic2, topic2Assignment, AdminUtils.getBrokerMetadatas(zkUtils), 3,
      Some(Map(0 -> Seq(1, 2), 1 -> Seq(0, 1), 2 -> Seq(2, 3))))
    // wait until leader is elected
    val leader1 = waitUntilLeaderIsElectedOrChanged(zkUtils, topic2, 1)
    val leader2 = waitUntilLeaderIsElectedOrChanged(zkUtils, topic2, 2)
    val leader1FromZk = zkUtils.getLeaderForPartition(topic2, 1).get
    val leader2FromZk = zkUtils.getLeaderForPartition(topic2, 2).get
    assertEquals(leader1, leader1FromZk)
    assertEquals(leader2, leader2FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 2)
    val metadataForTopic2 = AdminUtils.fetchTopicMetadata(Set(topic2),
      brokers.map(_.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))),
      "AddPartitionsTest-testManualAssignmentOfReplicas", 2000, 0).topicsMetadata
    val metaDataForTopic2 = metadata.filter(_.topic == topic2)
    val partitionDataForTopic2 = metaDataForTopic2.head.partitionsMetadata.sortBy(_.partitionId)
      "AddPartitionsTest-testManualAssignmentOfReplicas", 2000).get(topic2).getOrElse(List())
    val partitionDataForTopic2 = metadataForTopic2.sortBy(_.partition)
    assertEquals(3, partitionDataForTopic2.size)
    assertEquals(1, partitionDataForTopic2(1).partition)
    assertEquals(2, partitionDataForTopic2(2).partition)
    val replicas = partitionDataForTopic2(1).replicas
    assertEquals(2, replicas.size)
    assertTrue(replicas.head.id == 0 || replicas.head.id == 1)
    assertTrue(replicas(1).id == 0 || replicas(1).id == 1)
  }

  @Test
  def testReplicaPlacementAllServers(): Unit = {
    AdminUtils.addPartitions(zkUtils, topic3, topic3Assignment, AdminUtils.getBrokerMetadatas(zkUtils), 7)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 2)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 3)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 4)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 5)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 6)

    val metadataForTopic3 = AdminUtils.fetchTopicMetadata(Set(topic3),
      brokers.map(_.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))),
      "AddPartitionsTest-testReplicaPlacementAllServers", 2000).get(topic3).getOrElse(List())

    validateLeaderAndReplicas(metadataForTopic3, 0, 2, Set(2, 3, 0, 1))
    validateLeaderAndReplicas(metadataForTopic3, 1, 3, Set(3, 2, 0, 1))
    validateLeaderAndReplicas(metadataForTopic3, 2, 0, Set(0, 3, 1, 2))
    validateLeaderAndReplicas(metadataForTopic3, 3, 1, Set(1, 0, 2, 3))
    validateLeaderAndReplicas(metadataForTopic3, 4, 2, Set(2, 3, 0, 1))
    validateLeaderAndReplicas(metadataForTopic3, 5, 3, Set(3, 0, 1, 2))
    validateLeaderAndReplicas(metadataForTopic3, 6, 0, Set(0, 1, 2, 3))
  }

  @Test
  def testReplicaPlacementPartialServers(): Unit = {
    AdminUtils.addPartitions(zkUtils, topic2, topic2Assignment, AdminUtils.getBrokerMetadatas(zkUtils), 3)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 2)

    val metadataForTopic2 = AdminUtils.fetchTopicMetadata(Set(topic2),
      brokers.map(_.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))),
      "AddPartitionsTest-testReplicaPlacementPartialServers", 2000).get(topic2).getOrElse(List())

    validateLeaderAndReplicas(metadataForTopic2, 0, 1, Set(1, 2))
    validateLeaderAndReplicas(metadataForTopic2, 1, 2, Set(0, 2))
    validateLeaderAndReplicas(metadataForTopic2, 2, 3, Set(1, 3))
  }

  def validateLeaderAndReplicas(metadata: List[PartitionInfo], partitionId: Int, expectedLeaderId: Int, expectedReplicas: Set[Int]) = {
    val partitionOpt = metadata.find(_.partition == partitionId)
    assertTrue(s"Partition $partitionId should exist", partitionOpt.isDefined)
    val partition = partitionOpt.get

    assertTrue("Partition leader should exist", !partition.leader.isEmpty)
    assertEquals("Partition leader id should match", expectedLeaderId, partition.leader.id)

    assertEquals("Replica set should match", expectedReplicas, partition.replicas.map(_.id).toSet)
  }
}
