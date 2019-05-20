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
package kafka.admin

import java.io.File
import java.util.Properties

import kafka.admin.ReassignPartitionsCommand.Throttle
import kafka.log.LogConfig
import kafka.log.LogConfig._
import kafka.server.{ConfigType, DynamicConfig, KafkaConfig, KafkaServer}
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils._
import kafka.utils.{CoreUtils, Logging, TestUtils}
import kafka.zk.{AdminZkClient, KafkaZkClient, ZooKeeperTestHarness}
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock}
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertNull, assertTrue}
import org.scalatest.Assertions.fail

import scala.collection.JavaConverters._
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

class ReassignPartitionsCommandTest extends ZooKeeperTestHarness with Logging {
  var servers: Seq[KafkaServer] = Seq()
  var calls = 0

  @After
  override def tearDown() {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def shouldFindMovingReplicas() {
    val control = new TopicPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)

    //Given partition 0 moves from broker 100 -> 102. Partition 1 does not move.
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101), control)
    val proposed = Map(new TopicPartition("topic1", 0) -> Seq(101, 102), control)

    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        assertEquals(Set("0:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp))) //Should only be follower-throttle the moving replica
        assertEquals(Set("0:100","0:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp))) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    }

    val admin = new TestAdminZkClient(zkClient)
    assigner.assignThrottledReplicas(existing, proposed, admin)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasWhenProposedIsSubsetOfExisting() {
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)

    //Given we have more existing partitions than we are proposing
    val existingSuperset = Map(
      new TopicPartition("topic1", 0) -> Seq(100, 101),
      new TopicPartition("topic1", 1) -> Seq(100, 102),
      new TopicPartition("topic1", 2) -> Seq(100, 101),
      new TopicPartition("topic2", 0) -> Seq(100, 101, 102),
      new TopicPartition("topic3", 0) -> Seq(100, 101, 102)
    )
    val proposedSubset = Map(
      new TopicPartition("topic1", 0) -> Seq(101, 102),
      new TopicPartition("topic1", 1) -> Seq(102),
      new TopicPartition("topic1", 2) -> Seq(100, 101, 102)
    )

    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        assertEquals(Set("0:102","2:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp)))
        assertEquals(Set("0:100","0:101","2:100","2:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp)))
        assertEquals("topic1", topic)
        calls += 1
      }

      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    }

    val admin = new TestAdminZkClient(zkClient)
    //Then replicas should assign correctly (based on the proposed map)
    assigner.assignThrottledReplicas(existingSuperset, proposedSubset, admin)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultiplePartitions() {
    val control = new TopicPartition("topic1", 2) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)

    //Given partitions 0 & 1 moves from broker 100 -> 102. Partition 2 does not move.
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101), new TopicPartition("topic1", 1) -> Seq(100, 101), control)
    val proposed = Map(new TopicPartition("topic1", 0) -> Seq(101, 102), new TopicPartition("topic1", 1) -> Seq(101, 102), control)

    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        assertEquals(Set("0:102","1:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp))) //Should only be follower-throttle the moving replica
        assertEquals(Set("0:100","0:101","1:100","1:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp))) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }

      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    }

    val admin = new TestAdminZkClient(zkClient)
    //When
    assigner.assignThrottledReplicas(existing, proposed, admin)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopics() {
    val control = new TopicPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)

    //Given topics 1 -> move from broker 100 -> 102, topics 2 -> move from broker 101 -> 100
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101), new TopicPartition("topic2", 0) -> Seq(101, 102), control)
    val proposed = Map(new TopicPartition("topic1", 0) -> Seq(101, 102), new TopicPartition("topic2", 0) -> Seq(100, 102), control)

    //Then
    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        topic match {
          case "topic1" =>
            assertEquals(Set("0:100","0:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp)))
            assertEquals(Set("0:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp)))
          case "topic2" =>
            assertEquals(Set("0:101","0:102"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp)))
            assertEquals(Set("0:100"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp)))
          case _ => fail(s"Unexpected topic $topic")
        }
        calls += 1
      }
      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    }

    val admin = new TestAdminZkClient(zkClient)
    //When
    assigner.assignThrottledReplicas(existing, proposed, admin)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopicsAndPartitions() {
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)

    //Given
    val existing = Map(
      new TopicPartition("topic1", 0) -> Seq(100, 101),
      new TopicPartition("topic1", 1) -> Seq(100, 101),
      new TopicPartition("topic2", 0) -> Seq(101, 102),
      new TopicPartition("topic2", 1) -> Seq(101, 102)
    )
    val proposed = Map(
      new TopicPartition("topic1", 0) -> Seq(101, 102), //moves to 102
      new TopicPartition("topic1", 1) -> Seq(101, 102), //moves to 102
      new TopicPartition("topic2", 0) -> Seq(100, 102), //moves to 100
      new TopicPartition("topic2", 1) -> Seq(101, 100)  //moves to 100
    )

    //Then
    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        topic match {
          case "topic1" =>
            assertEquals(Set("0:102","1:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp)))
            assertEquals(Set("0:100","0:101","1:100","1:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp)))
          case "topic2" =>
            assertEquals(Set("0:100","1:100"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp)))
            assertEquals(Set("0:101","0:102","1:101","1:102"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp)))
          case _ => fail(s"Unexpected topic $topic")
        }
        calls += 1
      }

      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    }

    val admin = new TestAdminZkClient(zkClient)

    //When
    assigner.assignThrottledReplicas(existing, proposed, admin)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindTwoMovingReplicasInSamePartition() {
    val control = new TopicPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)

    //Given partition 0 has 2 moves from broker 102 -> 104 & 103 -> 105
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101, 102, 103), control)
    val proposed = Map(new TopicPartition("topic1", 0) -> Seq(100, 101, 104, 105), control)

    // Then
    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties) = {
        assertEquals(Set("0:104","0:105"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp))) //Should only be follower-throttle the moving replicas
        assertEquals(Set("0:100","0:101","0:102","0:103"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp))) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }

      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {new Properties}
    }

    val admin = new TestAdminZkClient(zkClient)
    //When
    assigner.assignThrottledReplicas(existing, proposed, admin)
    assertEquals(1, calls)
  }

  @Test
  def shouldNotOverwriteEntityConfigsWhenUpdatingThrottledReplicas(): Unit = {
    val control = new TopicPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null, null)
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101), control)
    val proposed = Map(new TopicPartition("topic1", 0) -> Seq(101, 102), control)

    //Given partition there are existing properties
    val existingProperties = propsWith("some-key", "some-value")

    //Then the dummy property should still be there
    class TestAdminZkClient(val zkClient: KafkaZkClient) extends AdminZkClient(zkClient) {
      override def changeTopicConfig(topic: String, configChange: Properties): Unit = {
        assertEquals("some-value", configChange.getProperty("some-key"))
        calls += 1
      }

      override def fetchEntityConfig(entityType: String, entityName: String): Properties = {
        existingProperties
      }
    }

    val admin = new TestAdminZkClient(zkClient)

    //When
    assigner.assignThrottledReplicas(existing, proposed, admin)
    assertEquals(1, calls)
  }

  @Test
  def shouldSetQuotaLimit(): Unit = {
    //Given
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101))
    val proposed = mutable.Map(new TopicPartition("topic1", 0) -> Seq(101, 102))

    //Setup
    val zk = stubZKClient(existing)
    val admin: AdminZkClient = createMock(classOf[AdminZkClient])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    val assigner = new ReassignPartitionsCommand(zk, None, proposed, Map.empty, admin)
    expect(admin.fetchEntityConfig(anyString(), anyString())).andStubReturn(new Properties)
    expect(admin.changeBrokerConfig(anyObject().asInstanceOf[List[Int]], capture(propsCapture))).anyTimes()
    replay(admin)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then
    for (actual <- propsCapture.getValues.asScala) {
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //3 brokers
  }

  @Test
  def shouldUpdateQuotaLimit(): Unit = {
    //Given
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101))
    val proposed = mutable.Map(new TopicPartition("topic1", 0) -> Seq(101, 102))

    //Setup
    val zk = stubZKClient(existing)
    val admin: AdminZkClient = createMock(classOf[AdminZkClient])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    val assigner = new ReassignPartitionsCommand(zk, None, proposed, Map.empty, admin)
    expect(admin.changeBrokerConfig(anyObject().asInstanceOf[List[Int]], capture(propsCapture))).anyTimes()

    //Expect the existing broker config to be changed from 10/100 to 1000
    val existingConfigs = CoreUtils.propsWith(
      (DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "10"),
      (DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "100")
    )
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), is("100"))).andReturn(copyOf(existingConfigs))
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), is("101"))).andReturn(copyOf(existingConfigs))
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), is("102"))).andReturn(copyOf(existingConfigs))
    replay(admin)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then
    for (actual <- propsCapture.getValues.asScala) {
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //three brokers
  }

  @Test
  def shouldNotOverwriteExistingPropertiesWhenLimitIsAdded(): Unit = {
    //Given
    val existing = Map(new TopicPartition("topic1", 0) -> Seq(100, 101))
    val proposed = mutable.Map(new TopicPartition("topic1", 0) -> Seq(101, 102))

    //Setup
    val zk = stubZKClient(existing)
    val admin: AdminZkClient = createMock(classOf[AdminZkClient])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    val assigner = new ReassignPartitionsCommand(zk, None, proposed, Map.empty, admin)
    expect(admin.changeBrokerConfig(anyObject().asInstanceOf[List[Int]], capture(propsCapture))).anyTimes()

    //Given there is some existing config
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), anyString())).andReturn(
      propsWith("useful.key", "useful.value")).atLeastOnce()

    replay(admin)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then other property remains
    for (actual <- propsCapture.getValues.asScala) {
      assertEquals("useful.value", actual.getProperty("useful.key"))
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //3 brokers
  }

  @Test
  def shouldRemoveThrottleLimitFromAllBrokers(): Unit = {
    //Given 3 brokers, but with assignment only covering 2 of them
    val brokers = Seq(100, 101, 102)
    val status = mutable.Map(new TopicPartition("topic1", 0) -> ReassignmentCompleted)
    val existingBrokerConfigs = propsWith(
      (DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "10"),
      (DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "100"),
      ("useful.key", "value")
    )

    //Setup
    val zk = stubZKClient(brokers = brokers)
    val admin: AdminZkClient = createMock(classOf[AdminZkClient])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    expect(admin.fetchEntityConfig(is(ConfigType.Topic), anyString())).andStubReturn(new Properties)
    expect(admin.changeBrokerConfig(anyObject().asInstanceOf[Seq[Int]], capture(propsCapture))).anyTimes()
    //Stub each invocation as EasyMock caches the return value which can be mutated
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), is("100"))).andReturn(copyOf(existingBrokerConfigs))
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), is("101"))).andReturn(copyOf(existingBrokerConfigs))
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), is("102"))).andReturn(copyOf(existingBrokerConfigs))
    replay(admin)

    //When
    ReassignPartitionsCommand.removeThrottle(zk, status, Map.empty, admin)

    //Then props should have gone (dummy remains)
    for (capture <- propsCapture.getValues.asScala) {
      assertEquals("value", capture.get("useful.key"))
      assertNull(capture.get(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
      assertNull(capture.get(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //3 brokers
  }

  @Test
  def shouldRemoveThrottleReplicaListBasedOnProposedAssignment(): Unit = {
    //Given two topics with existing config
    val status = mutable.Map(new TopicPartition("topic1", 0) -> ReassignmentCompleted,
                             new TopicPartition("topic2", 0) -> ReassignmentCompleted)
    val existingConfigs = CoreUtils.propsWith(
      (LogConfig.LeaderReplicationThrottledReplicasProp, "1:100:2:100"),
      (LogConfig.FollowerReplicationThrottledReplicasProp, "1:101,2:101"),
      ("useful.key", "value")
    )

    //Setup
    val zk = stubZKClient(brokers = Seq(100, 101))
    val admin: AdminZkClient = createMock(classOf[AdminZkClient])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    expect(admin.fetchEntityConfig(is(ConfigType.Broker), anyString())).andStubReturn(new Properties)
    expect(admin.fetchEntityConfig(is(ConfigType.Topic), is("topic1"))).andStubReturn(copyOf(existingConfigs))
    expect(admin.fetchEntityConfig(is(ConfigType.Topic), is("topic2"))).andStubReturn(copyOf(existingConfigs))

    //Should change both topics
    expect(admin.changeTopicConfig(is("topic1"), capture(propsCapture)))
    expect(admin.changeTopicConfig(is("topic2"), capture(propsCapture)))

    replay(admin)

    //When
    ReassignPartitionsCommand.removeThrottle(zk, status, Map.empty, admin)

    //Then props should have gone (dummy remains)
    for (actual <- propsCapture.getValues.asScala) {
      assertEquals("value", actual.getProperty("useful.key"))
      assertNull(actual.getProperty(LogConfig.LeaderReplicationThrottledReplicasProp))
      assertNull(actual.getProperty(LogConfig.FollowerReplicationThrottledReplicasProp))
    }
    assertEquals(2, propsCapture.getValues.size) //2 topics
  }

  @Test
  def testPartitionReassignmentWithLeaderInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // reassign partition 0
    val newReplicas = Seq(0, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, None, Map(topicAndPartition -> newReplicas), adminZkClient = adminZkClient)
    assertTrue("Partition reassignment attempt failed for [test, 0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, Map(topicAndPartition -> newReplicas))
          .getOrElse(topicAndPartition, fail(s"Failed to get reassignment status for $topicAndPartition")) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    // in sync replicas should not have any replica that is not in the new assigned replicas
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
  }

  @Test
  def testPartitionReassignmentWithLeaderNotInNewReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // reassign partition 0
    val newReplicas = Seq(1, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, None, Map(topicAndPartition -> newReplicas), adminZkClient = adminZkClient)
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, Map(topicAndPartition -> newReplicas))
          .getOrElse(topicAndPartition, fail(s"Failed to get reassignment status for $topicAndPartition")) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
  }

  @Test
  def testPartitionReassignmentNonOverlappingReplicas() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, None, Map(topicAndPartition -> newReplicas),  adminZkClient = adminZkClient)
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(zkClient, Map(topicAndPartition -> newReplicas))
          .getOrElse(topicAndPartition, fail(s"Failed to get reassignment status for $topicAndPartition")) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    assertEquals("Partition should have been reassigned to 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
  }

  @Test
  def testReassigningNonExistingPartition() {
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, None, Map(topicAndPartition -> newReplicas), adminZkClient = adminZkClient)
    assertFalse("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    val reassignedPartitions = zkClient.getPartitionReassignment
    assertFalse("Partition should not be reassigned", reassignedPartitions.contains(topicAndPartition))
  }

  @Test
  def testResumePartitionReassignmentThatWasCompleted() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create the topic
    adminZkClient.createTopicWithAssignment(topic, config = new Properties, expectedReplicaAssignment)
    // put the partition in the reassigned path as well
    // reassign partition 0
    val newReplicas = Seq(0, 1)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, None, Map(topicAndPartition -> newReplicas), adminZkClient = adminZkClient)
    reassignPartitionsCommand.reassignPartitions()
    // create brokers
    servers = TestUtils.createBrokerConfigs(2, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))

    // wait until reassignment completes
    TestUtils.waitUntilTrue(() => !zkClient.reassignPartitionsInProgress(),
                            "Partition reassignment should complete")
    val assignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    assertEquals("Partition should have been reassigned to 0, 1", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    // ensure that there are no under replicated partitions
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
  }

  private def getBrokersWithPartitionDir(servers: Iterable[KafkaServer], topic: String, partitionId: Int): Set[Int] = {
    servers.filter(server => new File(server.config.logDirs.head, topic + "-" + partitionId).exists)
           .map(_.config.brokerId)
           .toSet
  }

  //Override eq as is for brevity
  def is[T](v: T): T = EasyMock.eq(v)

  @Before
  def setup(): Unit = {
    calls = 0
  }

  def stubZKClient(existingAssignment: Map[TopicPartition, Seq[Int]] = Map[TopicPartition, Seq[Int]](),
                   brokers: Seq[Int] = Seq[Int]()): KafkaZkClient = {
    val zkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    expect(zkClient.getReplicaAssignmentForTopics(anyObject().asInstanceOf[Set[String]])).andStubReturn(existingAssignment)
    expect(zkClient.getAllBrokersInCluster).andStubReturn(brokers.map(TestUtils.createBroker(_, "", 1)))
    replay(zkClient)
    zkClient
  }

  def toReplicaSet(throttledReplicasString: Any): Set[String] = {
    throttledReplicasString.toString.split(",").toSet
  }
}
