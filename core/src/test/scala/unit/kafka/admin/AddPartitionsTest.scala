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

import org.junit.Assert._
import org.apache.kafka.common.protocol.SecurityProtocol
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import kafka.utils.{ZkUtils, CoreUtils, TestUtils}
import kafka.cluster.Broker
import kafka.client.ClientUtils
import kafka.server.{KafkaConfig, KafkaServer}
import org.junit.{Test, After, Before}

class AddPartitionsTest extends ZooKeeperTestHarness {
  var configs: Seq[KafkaConfig] = null
  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]
  var brokers: Seq[Broker] = Seq.empty[Broker]

  val partitionId = 0

  val topic1 = "new-topic1"
  val topic2 = "new-topic2"
  val topic3 = "new-topic3"
  val topic4 = "new-topic4"

  @Before
  override def setUp() {
    super.setUp()

    configs = (0 until 4).map(i => KafkaConfig.fromProps(TestUtils.createBrokerConfig(i, zkConnect, enableControlledShutdown = false)))
    // start all the servers
    servers = configs.map(c => TestUtils.createServer(c))
    brokers = servers.map(s => new Broker(s.config.brokerId, s.config.hostName, s.boundPort()))

    // create topics first
    createTopic(zkUtils, topic1, partitionReplicaAssignment = Map(0->Seq(0,1)), servers = servers)
    createTopic(zkUtils, topic2, partitionReplicaAssignment = Map(0->Seq(1,2)), servers = servers)
    createTopic(zkUtils, topic3, partitionReplicaAssignment = Map(0->Seq(2,3,0,1)), servers = servers)
    createTopic(zkUtils, topic4, partitionReplicaAssignment = Map(0->Seq(0,3)), servers = servers)
  }

  @After
  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.rm(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def testTopicDoesNotExist {
    try {
      AdminUtils.addPartitions(zkUtils, "Blah", 1)
      fail("Topic should not exist")
    } catch {
      case e: AdminOperationException => //this is good
      case e2: Throwable => throw e2
    }
  }

  @Test
  def testWrongReplicaCount {
    try {
      AdminUtils.addPartitions(zkUtils, topic1, 2, "0:1,0:1:2")
      fail("Add partitions should fail")
    } catch {
      case e: AdminOperationException => //this is good
      case e2: Throwable => throw e2
    }
  }

  @Test
  def testIncrementPartitions {
    AdminUtils.addPartitions(zkUtils, topic1, 3)
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
    val metadata = ClientUtils.fetchTopicMetadata(Set(topic1), brokers.map(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)), "AddPartitionsTest-testIncrementPartitions",
      2000,0).topicsMetadata
    val metaDataForTopic1 = metadata.filter(p => p.topic.equals(topic1))
    val partitionDataForTopic1 = metaDataForTopic1.head.partitionsMetadata.sortBy(_.partitionId)
    assertEquals(partitionDataForTopic1.size, 3)
    assertEquals(partitionDataForTopic1(1).partitionId, 1)
    assertEquals(partitionDataForTopic1(2).partitionId, 2)
    val replicas = partitionDataForTopic1(1).replicas
    assertEquals(replicas.size, 2)
    assert(replicas.contains(partitionDataForTopic1(1).leader.get))
  }

  @Test
  def testManualAssignmentOfReplicas {
    AdminUtils.addPartitions(zkUtils, topic2, 3, "1:2,0:1,2:3")
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
    val metadata = ClientUtils.fetchTopicMetadata(Set(topic2), brokers.map(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)), "AddPartitionsTest-testManualAssignmentOfReplicas",
      2000,0).topicsMetadata
    val metaDataForTopic2 = metadata.filter(p => p.topic.equals(topic2))
    val partitionDataForTopic2 = metaDataForTopic2.head.partitionsMetadata.sortBy(_.partitionId)
    assertEquals(partitionDataForTopic2.size, 3)
    assertEquals(partitionDataForTopic2(1).partitionId, 1)
    assertEquals(partitionDataForTopic2(2).partitionId, 2)
    val replicas = partitionDataForTopic2(1).replicas
    assertEquals(replicas.size, 2)
    assert(replicas(0).id == 0 || replicas(0).id == 1)
    assert(replicas(1).id == 0 || replicas(1).id == 1)
  }

  @Test
  def testReplicaPlacement {
    AdminUtils.addPartitions(zkUtils, topic3, 7)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 1)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 2)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 3)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 4)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 5)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 6)

    val metadata = ClientUtils.fetchTopicMetadata(Set(topic3), brokers.map(_.getBrokerEndPoint(SecurityProtocol.PLAINTEXT)), "AddPartitionsTest-testReplicaPlacement",
      2000,0).topicsMetadata

    val metaDataForTopic3 = metadata.filter(p => p.topic.equals(topic3)).head
    val partitionsMetadataForTopic3 = metaDataForTopic3.partitionsMetadata.sortBy(_.partitionId)
    val partition1DataForTopic3 = partitionsMetadataForTopic3(1)
    val partition2DataForTopic3 = partitionsMetadataForTopic3(2)
    val partition3DataForTopic3 = partitionsMetadataForTopic3(3)
    val partition4DataForTopic3 = partitionsMetadataForTopic3(4)
    val partition5DataForTopic3 = partitionsMetadataForTopic3(5)
    val partition6DataForTopic3 = partitionsMetadataForTopic3(6)

    assertEquals(partition1DataForTopic3.replicas.size, 4)
    assertEquals(partition1DataForTopic3.replicas(0).id, 3)
    assertEquals(partition1DataForTopic3.replicas(1).id, 2)
    assertEquals(partition1DataForTopic3.replicas(2).id, 0)
    assertEquals(partition1DataForTopic3.replicas(3).id, 1)

    assertEquals(partition2DataForTopic3.replicas.size, 4)
    assertEquals(partition2DataForTopic3.replicas(0).id, 0)
    assertEquals(partition2DataForTopic3.replicas(1).id, 3)
    assertEquals(partition2DataForTopic3.replicas(2).id, 1)
    assertEquals(partition2DataForTopic3.replicas(3).id, 2)

    assertEquals(partition3DataForTopic3.replicas.size, 4)
    assertEquals(partition3DataForTopic3.replicas(0).id, 1)
    assertEquals(partition3DataForTopic3.replicas(1).id, 0)
    assertEquals(partition3DataForTopic3.replicas(2).id, 2)
    assertEquals(partition3DataForTopic3.replicas(3).id, 3)

    assertEquals(partition4DataForTopic3.replicas.size, 4)
    assertEquals(partition4DataForTopic3.replicas(0).id, 2)
    assertEquals(partition4DataForTopic3.replicas(1).id, 3)
    assertEquals(partition4DataForTopic3.replicas(2).id, 0)
    assertEquals(partition4DataForTopic3.replicas(3).id, 1)

    assertEquals(partition5DataForTopic3.replicas.size, 4)
    assertEquals(partition5DataForTopic3.replicas(0).id, 3)
    assertEquals(partition5DataForTopic3.replicas(1).id, 0)
    assertEquals(partition5DataForTopic3.replicas(2).id, 1)
    assertEquals(partition5DataForTopic3.replicas(3).id, 2)

    assertEquals(partition6DataForTopic3.replicas.size, 4)
    assertEquals(partition6DataForTopic3.replicas(0).id, 0)
    assertEquals(partition6DataForTopic3.replicas(1).id, 1)
    assertEquals(partition6DataForTopic3.replicas(2).id, 2)
    assertEquals(partition6DataForTopic3.replicas(3).id, 3)
  }
}
