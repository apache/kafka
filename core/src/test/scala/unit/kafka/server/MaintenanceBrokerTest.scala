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

package kafka.server

import java.util.{Optional, Properties}

import kafka.server.KafkaConfig.fromProps
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils
import kafka.utils.TestUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol

import scala.collection.JavaConverters._
import org.junit.Assert._
import org.junit.{After, Test}

import scala.collection.Map

/**
  * This is the main test which ensure maintenance broker work correctly.
  */
class MaintenanceBrokerTest extends ZooKeeperTestHarness {

  var brokers: Seq[KafkaServer] = null

  @After
  override def tearDown() {
    shutdownServers(brokers)
    super.tearDown()
  }

  @Test
  def testTopicCreatedByZkclientShouldHonorMaintenanceBrokers(): Unit = {

    brokers = (0 to 2).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }

    // create topic using zkclient
    TestUtils.createTopic(zkClient, "topic1", 3, 2, brokers)
    (0 to 2).foreach {
      brokerId =>
        assertTrue("topic1 should be in broker " + brokerId, !ensureTopicNotInBrokers("topic1", Set(brokerId)))
    }

    // setting broker 1 to not take new topic partitions
    setMaintenanceBrokers(Seq(1))

    TestUtils.createTopic(zkClient, "topic2", 3, 2, brokers)
    assertTrue("topic2 should not be in broker 1", ensureTopicNotInBrokers("topic2", Set(1)))

    // setting broker 1 and 2 to not take new topic partitions
    setMaintenanceBrokers(Seq(1, 2))

    TestUtils.createTopic(zkClient, "topic3", 3, 1, brokers)

    assertTrue("topic3 should not be in broker 1 and 2", ensureTopicNotInBrokers("topic3", Set(1, 2)))
    assertTrue("topic3 should in broker 0", !ensureTopicNotInBrokers("topic3", Set(0)))
  }

  @Test
  def testTopicCreatedByAdminClientShouldHonorMaintenanceBrokers(): Unit = {

    brokers = (0 to 2).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }

    val brokerList = TestUtils.bootstrapServers(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    val adminClientConfig = new Properties
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val client = AdminClient.create(adminClientConfig)

    // create topic using admin client
    val future1 = client.createTopics(Seq("topic1").map(new NewTopic(_, 3, 2.toShort)).asJava,
      new CreateTopicsOptions()).all()
    future1.get()

    (0 to 2).foreach {
      brokerId =>
        assertTrue("topic1 should be in broker " + brokerId, !ensureTopicNotInBrokers("topic1", Set(brokerId)))
    }

    TestUtils.waitUntilControllerElected(zkClient)

    // setting broker 1 to not take new topic partitions
    setMaintenanceBrokers(Seq(1))

    val future2 = client.createTopics(Seq("topic2").map(new NewTopic(_, 3, 2.toShort)).asJava,
      new CreateTopicsOptions()).all()
    future2.get()

    assertTrue("topic2 should not be in broker 1", ensureTopicNotInBrokers("topic2", Set(1)))

    // setting broker 1 and 2 to not take new topic partitions
    setMaintenanceBrokers(Seq(1, 2))

    val future3 = client.createTopics(Seq("topic3").map(new NewTopic(_, 3, 1.toShort)).asJava,
      new CreateTopicsOptions()).all()
    future3.get()

    assertTrue("topic3 should not be in broker 1 and 2", ensureTopicNotInBrokers("topic3", Set(1, 2)))
    assertTrue("topic3 should be in broker 0", !ensureTopicNotInBrokers("topic3", Set(0)))

    // create topic with #replicas > #non-maintenance brokers
    val future4 = client.createTopics(Seq("topic4").map(new NewTopic(_, 3, 3.toShort)).asJava,
      new CreateTopicsOptions()).all()
    future4.get()

    (0 to 2).foreach {
      brokerId =>
        assertTrue("topic4 should be in broker " + brokerId + " because #replicas > #non-maintenance brokers",
          !ensureTopicNotInBrokers("topic4", Set(brokerId)))
    }

    // clear maintenance broker
    setMaintenanceBrokers(Seq.empty[Int])

    // create topic using admin client
    val future5 = client.createTopics(Seq("topic5").map(new NewTopic(_, 3, 1.toShort)).asJava,
      new CreateTopicsOptions()).all()
    future5.get()

    (0 to 2).foreach {
      brokerId =>
        assertTrue("topic5 should be in broker " + brokerId, !ensureTopicNotInBrokers("topic5", Set(brokerId)))
    }

    client.close()
  }

  @Test
  def testAddPartitionByAdminClientShouldHonorMaintenanceBrokers(): Unit = {

    brokers = (0 to 2).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }

    val brokerList = TestUtils.bootstrapServers(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
    val adminClientConfig = new Properties
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val client = AdminClient.create(adminClientConfig)

    TestUtils.waitUntilControllerElected(zkClient)

    // setting broker 1 to not take new topic partitions
    setMaintenanceBrokers(Seq(1))

    val future1 = client.createTopics(Seq("topic1").map(new NewTopic(_, 3, 2.toShort)).asJava,
      new CreateTopicsOptions()).all()
    future1.get()

    assertTrue("topic1 should not be in broker 1", ensureTopicNotInBrokers("topic1", Set(1)))

    val future2 = client.createPartitions(Map("topic1" -> NewPartitions.increaseTo(5)).asJava).all()
    future2.get()

    assertTrue("topic1 should not be in broker 1 after increasing partition count",
      ensureTopicNotInBrokers("topic1", Set(1)))

    client.close()
  }

  @Test
  def testTopicCreatedInZkShouldBeRearrangedForMaintenanceBrokers(): Unit = {

    brokers = (0 to 2).map { id => createServer(fromProps(createBrokerConfig(id, zkConnect))) }
    TestUtils.waitUntilControllerElected(zkClient)

    TestUtils.createTopic(zkClient, "topic1", Map(0 -> List(0, 1), 1 -> List(1, 2)), brokers)

    // setting broker 1 to not take new topic partitions
    setMaintenanceBrokers(Seq(1))

    TestUtils.createTopic(zkClient, "topic2", Map(0 -> List(0, 1), 1 -> List(1, 2)), brokers)

    assertTrue("new topic topic2 should be rearranged and not be in broker 1", ensureTopicNotInBrokers("topic2", Set(1)))
    assertTrue("old topic topic1 should still in broker 1", !ensureTopicNotInBrokers("topic1", Set(1)))
  }

  def ensureTopicNotInBrokers(topic: String, brokerIds: Set[Int]): Boolean = {
    val topicAssignment = zkClient.getReplicaAssignmentForTopics(Set(topic))
    topicAssignment.map(_._2).flatten.toSet.intersect(brokerIds).isEmpty
  }

  def createBrokers(brokerIds: Seq[Int]): Unit = {
    brokerIds.foreach { id =>
      brokers = brokers :+ createServer(fromProps(createBrokerConfig(id, zkConnect)))
    }
  }

  def setMaintenanceBrokers(brokerIds: Seq[Int]): Unit = {
    var propstring = brokerIds.mkString(",")
    adminZkClient.changeBrokerConfig(None,
      propsWith((DynamicConfig.Broker.MaintenanceBrokerListProp, propstring)))

    val controllerId = TestUtils.waitUntilControllerElected(zkClient)

    TestUtils.waitUntilTrue(() => brokers(controllerId).config.getMaintenanceBrokerList == brokerIds,
      s"wait until broker $propstring is masked as maintenance broker not taking new partitions", 5000)

  }

}
