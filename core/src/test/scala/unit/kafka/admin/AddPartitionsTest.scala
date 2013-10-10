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

import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils._
import junit.framework.Assert._
import kafka.utils.{ZkUtils, Utils, TestUtils}
import kafka.cluster.Broker
import kafka.client.ClientUtils
import kafka.server.{KafkaConfig, KafkaServer}

class AddPartitionsTest extends JUnit3Suite with ZooKeeperTestHarness {
  val brokerId1 = 0
  val brokerId2 = 1
  val brokerId3 = 2
  val brokerId4 = 3

  val port1 = TestUtils.choosePort()
  val port2 = TestUtils.choosePort()
  val port3 = TestUtils.choosePort()
  val port4 = TestUtils.choosePort()

  val configProps1 = TestUtils.createBrokerConfig(brokerId1, port1)
  val configProps2 = TestUtils.createBrokerConfig(brokerId2, port2)
  val configProps3 = TestUtils.createBrokerConfig(brokerId3, port3)
  val configProps4 = TestUtils.createBrokerConfig(brokerId4, port4)

  var servers: Seq[KafkaServer] = Seq.empty[KafkaServer]
  var brokers: Seq[Broker] = Seq.empty[Broker]

  val partitionId = 0

  val topic1 = "new-topic1"
  val topic2 = "new-topic2"
  val topic3 = "new-topic3"
  val topic4 = "new-topic4"

  override def setUp() {
    super.setUp()
    // start all the servers
    val server1 = TestUtils.createServer(new KafkaConfig(configProps1))
    val server2 = TestUtils.createServer(new KafkaConfig(configProps2))
    val server3 = TestUtils.createServer(new KafkaConfig(configProps3))
    val server4 = TestUtils.createServer(new KafkaConfig(configProps4))

    servers ++= List(server1, server2, server3, server4)
    brokers = servers.map(s => new Broker(s.config.brokerId, s.config.hostName, s.config.port))

    // create topics with 1 partition, 2 replicas, one on each broker
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic1, Map(0->Seq(0,1)))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic2, Map(0->Seq(1,2)))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic3, Map(0->Seq(2,3,0,1)))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic4, Map(0->Seq(0,3)))


    // wait until leader is elected
    var leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic1, partitionId, 500)
    var leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic2, partitionId, 500)
    var leader3 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, partitionId, 500)
    var leader4 = waitUntilLeaderIsElectedOrChanged(zkClient, topic4, partitionId, 500)

    debug("Leader for " + topic1  + " is elected to be: %s".format(leader1.getOrElse(-1)))
    debug("Leader for " + topic2 + " is elected to be: %s".format(leader1.getOrElse(-1)))
    debug("Leader for " + topic3 + "is elected to be: %s".format(leader1.getOrElse(-1)))
    debug("Leader for " + topic4 + "is elected to be: %s".format(leader1.getOrElse(-1)))

    assertTrue("Leader should get elected", leader1.isDefined)
    assertTrue("Leader should get elected", leader2.isDefined)
    assertTrue("Leader should get elected", leader3.isDefined)
    assertTrue("Leader should get elected", leader4.isDefined)

    assertTrue("Leader could be broker 0 or broker 1 for " + topic1, (leader1.getOrElse(-1) == 0) || (leader1.getOrElse(-1) == 1))
    assertTrue("Leader could be broker 1 or broker 2 for " + topic2, (leader2.getOrElse(-1) == 1) || (leader1.getOrElse(-1) == 2))
    assertTrue("Leader could be broker 2 or broker 3 for " + topic3, (leader3.getOrElse(-1) == 2) || (leader1.getOrElse(-1) == 3))
    assertTrue("Leader could be broker 3 or broker 4 for " + topic4, (leader4.getOrElse(-1) == 0) || (leader1.getOrElse(-1) == 3))
  }

  override def tearDown() {
    servers.map(server => server.shutdown())
    servers.map(server => Utils.rm(server.config.logDirs))
    super.tearDown()
  }

  def testTopicDoesNotExist {
    try {
      AdminUtils.addPartitions(zkClient, "Blah", 1)
      fail("Topic should not exist")
    } catch {
      case e: AdminOperationException => //this is good
      case e2: Throwable => throw e2
    }
  }

  def testWrongReplicaCount {
    try {
      AdminUtils.addPartitions(zkClient, topic1, 2, "0:1,0:1:2")
      fail("Add partitions should fail")
    } catch {
      case e: AdminOperationException => //this is good
      case e2: Throwable => throw e2
    }
  }

  def testIncrementPartitions {
    AdminUtils.addPartitions(zkClient, topic1, 3)
    // wait until leader is elected
    var leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 1, 500)
    var leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic1, 2, 500)
    val leader1FromZk = ZkUtils.getLeaderForPartition(zkClient, topic1, 1).get
    val leader2FromZk = ZkUtils.getLeaderForPartition(zkClient, topic1, 2).get
    assertEquals(leader1.get, leader1FromZk)
    assertEquals(leader2.get, leader2FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 1, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic1, 2, 1000)
    val metadata = ClientUtils.fetchTopicMetadata(Set(topic1), brokers, "AddPartitionsTest-testIncrementPartitions",
      2000,0).topicsMetadata
    val metaDataForTopic1 = metadata.filter(p => p.topic.equals(topic1))
    val partitionDataForTopic1 = metaDataForTopic1.head.partitionsMetadata
    assertEquals(partitionDataForTopic1.size, 3)
    assertEquals(partitionDataForTopic1(1).partitionId, 1)
    assertEquals(partitionDataForTopic1(2).partitionId, 2)
    val replicas = partitionDataForTopic1(1).replicas
    assertEquals(replicas.size, 2)
    assert(replicas.contains(partitionDataForTopic1(1).leader.get))
  }

  def testManualAssignmentOfReplicas {
    AdminUtils.addPartitions(zkClient, topic2, 3, "1:2,0:1,2:3")
    // wait until leader is elected
    var leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic2, 1, 500)
    var leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic2, 2, 500)
    val leader1FromZk = ZkUtils.getLeaderForPartition(zkClient, topic2, 1).get
    val leader2FromZk = ZkUtils.getLeaderForPartition(zkClient, topic2, 2).get
    assertEquals(leader1.get, leader1FromZk)
    assertEquals(leader2.get, leader2FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 1, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic2, 2, 1000)
    val metadata = ClientUtils.fetchTopicMetadata(Set(topic2), brokers, "AddPartitionsTest-testManualAssignmentOfReplicas",
      2000,0).topicsMetadata
    val metaDataForTopic2 = metadata.filter(p => p.topic.equals(topic2))
    val partitionDataForTopic2 = metaDataForTopic2.head.partitionsMetadata
    assertEquals(partitionDataForTopic2.size, 3)
    assertEquals(partitionDataForTopic2(1).partitionId, 1)
    assertEquals(partitionDataForTopic2(2).partitionId, 2)
    val replicas = partitionDataForTopic2(1).replicas
    assertEquals(replicas.size, 2)
    assert(replicas(0).id == 0 || replicas(0).id == 1)
    assert(replicas(1).id == 0 || replicas(1).id == 1)
  }

  def testReplicaPlacement {
    AdminUtils.addPartitions(zkClient, topic3, 7)
    // wait until leader is elected
    var leader1 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 1, 500)
    var leader2 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 2, 500)
    var leader3 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 3, 500)
    var leader4 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 4, 500)
    var leader5 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 5, 500)
    var leader6 = waitUntilLeaderIsElectedOrChanged(zkClient, topic3, 6, 500)

    val leader1FromZk = ZkUtils.getLeaderForPartition(zkClient, topic3, 1).get
    val leader2FromZk = ZkUtils.getLeaderForPartition(zkClient, topic3, 2).get
    val leader3FromZk = ZkUtils.getLeaderForPartition(zkClient, topic3, 3).get
    val leader4FromZk = ZkUtils.getLeaderForPartition(zkClient, topic3, 4).get
    val leader5FromZk = ZkUtils.getLeaderForPartition(zkClient, topic3, 5).get
    val leader6FromZk = ZkUtils.getLeaderForPartition(zkClient, topic3, 6).get

    assertEquals(leader1.get, leader1FromZk)
    assertEquals(leader2.get, leader2FromZk)
    assertEquals(leader3.get, leader3FromZk)
    assertEquals(leader4.get, leader4FromZk)
    assertEquals(leader5.get, leader5FromZk)
    assertEquals(leader6.get, leader6FromZk)

    // read metadata from a broker and verify the new topic partitions exist
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 1, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 2, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 3, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 4, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 5, 1000)
    TestUtils.waitUntilMetadataIsPropagated(servers, topic3, 6, 1000)

    val metadata = ClientUtils.fetchTopicMetadata(Set(topic3), brokers, "AddPartitionsTest-testReplicaPlacement",
      2000,0).topicsMetadata

    val metaDataForTopic3 = metadata.filter(p => p.topic.equals(topic3)).head
    val partition1DataForTopic3 = metaDataForTopic3.partitionsMetadata(1)
    val partition2DataForTopic3 = metaDataForTopic3.partitionsMetadata(2)
    val partition3DataForTopic3 = metaDataForTopic3.partitionsMetadata(3)
    val partition4DataForTopic3 = metaDataForTopic3.partitionsMetadata(4)
    val partition5DataForTopic3 = metaDataForTopic3.partitionsMetadata(5)
    val partition6DataForTopic3 = metaDataForTopic3.partitionsMetadata(6)

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