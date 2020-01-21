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
import kafka.server.{DynamicConfig, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{KafkaZkClient, ZooKeeperTestHarness}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, DescribeReplicaLogDirsResult}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock}
import org.junit.{After, Before, Test}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.scalatest.Assertions.fail

import scala.collection.JavaConverters._
import org.apache.kafka.common.{TopicPartition, TopicPartitionReplica}

import scala.collection.mutable
import scala.collection.Seq
import scala.util.Try

// Even though we test with a mock ServiceClient where possible, use the ZooKeeperTestHarness
// for use by the TestUtils below.
class ReassignPartitionsCommandTest extends ZooKeeperTestHarness with Logging {
  var servers: Seq[KafkaServer] = Seq()
  var calls = 0
  val TestTopicName = "topic1"
  val TestTopicAlternateName = "topic2"

  @Before
  def setup(): Unit = {
    calls = 0
  }

  @After
  override def tearDown(): Unit = {
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  // Base Test Service Client with minimal functionality, just enough for our tests to
  // override
  class TestServiceClientBase extends ReassignCommandService {
    override def getBrokerIdsInCluster: Seq[Int] = throw new UnsupportedOperationException()

    override def getBrokerMetadatas(rackAwareMode: RackAwareMode, brokerList: Option[Seq[Int]]): Seq[BrokerMetadata] =
      throw new UnsupportedOperationException()

    override def updateBrokerConfigs(broker: Int, configs: collection.Map[String, String]): Boolean =
      throw new UnsupportedOperationException()

    override def updateTopicConfigs(topic: String, configs: collection.Map[String, String]): Boolean =
      throw new UnsupportedOperationException()

    override def getPartitionsForTopics(topics: Set[String]): collection.Map[String, Seq[Int]] =
      throw new UnsupportedOperationException()

    override def getReplicaLogDirsForTopics(topics: collection.Set[TopicPartitionReplica]): collection.Map[TopicPartitionReplica, DescribeReplicaLogDirsResult.ReplicaLogDirInfo] =
      throw new UnsupportedOperationException()

    override def alterPartitionAssignment(topics: collection.Map[TopicPartition, Seq[Int]], timeoutMs: Long): Unit =
      throw new UnsupportedOperationException()

    override def alterReplicaLogDirs(topics: collection.Map[TopicPartitionReplica, String], timeoutMs: Long): collection.Set[TopicPartitionReplica] =
      throw new UnsupportedOperationException()

    override def getReplicaAssignmentForTopics(topics: Set[String]): collection.Map[TopicPartition, Seq[Int]] =
      throw new UnsupportedOperationException()

    override def reassignInProgress: Boolean = throw new UnsupportedOperationException()

    override def getOngoingReassignments: collection.Map[TopicPartition, Seq[Int]] =
      throw new UnsupportedOperationException()

    override def close(): Unit = Unit
  }

  @Test
  def shouldFindMovingReplicas(): Unit = {
    val control = new TopicPartition(TestTopicName, 1) -> Seq(100, 102)

    //Given partition 0 moves from broker 100 -> 102. Partition 1 does not move.
    val existing = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101), control)
    val proposed = Map(new TopicPartition(TestTopicName, 0) -> Seq(101, 102), control)

    class TestServiceClient() extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        assertEquals(TestTopicName, topic)
        assertEquals(Set("0:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get)) //Should only be follower-throttle the moving replica
        assertEquals(Set("0:100","0:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get)) //Should leader-throttle all existing (pre move) replicas

        calls += 1
        true
      }
    }

    val service = new TestServiceClient()
    val assigner = ReassignPartitionsCommand(service, null, null)

    assigner.assignThrottledReplicas(existing, proposed)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasWhenProposedIsSubsetOfExisting(): Unit = {

    //Given we have more existing partitions than we are proposing
    val existingSuperset = Map(
      new TopicPartition(TestTopicName, 0) -> Seq(100, 101),
      new TopicPartition(TestTopicName, 1) -> Seq(100, 102),
      new TopicPartition(TestTopicName, 2) -> Seq(100, 101),
      new TopicPartition(TestTopicAlternateName, 0) -> Seq(100, 101, 102),
      new TopicPartition("topic3", 0) -> Seq(100, 101, 102)
    )
    val proposedSubset = Map(
      new TopicPartition(TestTopicName, 0) -> Seq(101, 102),
      new TopicPartition(TestTopicName, 1) -> Seq(102),
      new TopicPartition(TestTopicName, 2) -> Seq(100, 101, 102)
    )

    class TestServiceClient extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        assertEquals(TestTopicName, topic)
        assertEquals(Set("0:102","2:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get)) //Should only be follower-throttle the moving replica
        assertEquals(Set("0:100","0:101","2:100","2:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get)) //Should leader-throttle all existing (pre move) replicas

        calls += 1
        true
      }
    }

    val service = new TestServiceClient
    val assigner = ReassignPartitionsCommand(service, null, null)
    //Then replicas should assign correctly (based on the proposed map)
    assigner.assignThrottledReplicas(existingSuperset, proposedSubset)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultiplePartitions(): Unit = {
    val control = new TopicPartition(TestTopicName, 2) -> Seq(100, 102)

    //Given partitions 0 & 1 moves from broker 100 -> 102. Partition 2 does not move.
    val existing = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101), new TopicPartition(TestTopicName, 1) -> Seq(100, 101), control)
    val proposed = Map(new TopicPartition(TestTopicName, 0) -> Seq(101, 102), new TopicPartition(TestTopicName, 1) -> Seq(101, 102), control)

    class TestServiceClient extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        assertEquals(TestTopicName, topic)
        assertEquals(Set("0:102","1:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get)) //Should only be follower-throttle the moving replica
        assertEquals(Set("0:100","0:101","1:100","1:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get)) //Should leader-throttle all existing (pre move) replicas

        calls += 1
        true
      }
    }

    val service = new TestServiceClient
    val assigner = ReassignPartitionsCommand(service, null, null)
    //When
    assigner.assignThrottledReplicas(existing, proposed)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopics(): Unit = {
    val control = new TopicPartition(TestTopicName, 1) -> Seq(100, 102)

    //Given topics 1 -> move from broker 100 -> 102, topics 2 -> move from broker 101 -> 100
    val existing = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101), new TopicPartition(TestTopicAlternateName, 0) -> Seq(101, 102), control)
    val proposed = Map(new TopicPartition(TestTopicName, 0) -> Seq(101, 102), new TopicPartition(TestTopicAlternateName, 0) -> Seq(100, 102), control)

    //Then
    class TestServiceClient extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        topic match {
          case TestTopicName =>
            assertEquals(Set("0:100", "0:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get))
            assertEquals(Set("0:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get))
          case TestTopicAlternateName =>
            assertEquals(Set("0:101", "0:102"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get))
            assertEquals(Set("0:100"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get))
          case _ => fail(s"Unexpected topic $topic")
        }
        calls += 1
        true
      }
    }

    val service = new TestServiceClient
    val assigner = ReassignPartitionsCommand(service, null, null)
    //When
    assigner.assignThrottledReplicas(existing, proposed)
    assertEquals(2, calls)
  }


  @Test
  def shouldFindMovingReplicasMultipleTopicsAndPartitions(): Unit = {
    //Given
    val existing = Map(
      new TopicPartition(TestTopicName, 0) -> Seq(100, 101),
      new TopicPartition(TestTopicName, 1) -> Seq(100, 101),
      new TopicPartition(TestTopicAlternateName, 0) -> Seq(101, 102),
      new TopicPartition(TestTopicAlternateName, 1) -> Seq(101, 102)
    )
    val proposed = Map(
      new TopicPartition(TestTopicName, 0) -> Seq(101, 102), //moves to 102
      new TopicPartition(TestTopicName, 1) -> Seq(101, 102), //moves to 102
      new TopicPartition(TestTopicAlternateName, 0) -> Seq(100, 102), //moves to 100
      new TopicPartition(TestTopicAlternateName, 1) -> Seq(101, 100)  //moves to 100
    )

    //Then
    class TestServiceClient extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        topic match {
          case TestTopicName =>
            assertEquals(Set("0:102","1:102"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get))
            assertEquals(Set("0:100","0:101","1:100","1:101"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get))
          case TestTopicAlternateName =>
            assertEquals(Set("0:100","1:100"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get))
            assertEquals(Set("0:101","0:102","1:101","1:102"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get))
          case _ => fail(s"Unexpected topic $topic")
        }
        calls += 1
        true
      }
    }

    val service = new TestServiceClient
    val assigner = ReassignPartitionsCommand(service, null, null)
    //When
    assigner.assignThrottledReplicas(existing, proposed)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindTwoMovingReplicasInSamePartition(): Unit = {
    val control = new TopicPartition(TestTopicName, 1) -> Seq(100, 102)

    //Given partition 0 has 2 moves from broker 102 -> 104 & 103 -> 105
    val existing = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101, 102, 103), control)
    val proposed = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101, 104, 105), control)

    // Then
    class TestServiceClient extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        assertEquals(TestTopicName, topic)
        assertEquals(Set("0:104","0:105"), toReplicaSet(configChange.get(FollowerReplicationThrottledReplicasProp).get)) //Should only be follower-throttle the moving replicas
        assertEquals(Set("0:100","0:101","0:102","0:103"), toReplicaSet(configChange.get(LeaderReplicationThrottledReplicasProp).get)) //Should leader-throttle all existing (pre move) replicas
        calls += 1

        true
      }
    }

    val service = new TestServiceClient
    val assigner = ReassignPartitionsCommand(service, null, null)

    //When
    assigner.assignThrottledReplicas(existing, proposed)
    assertEquals(1, calls)
  }

  @Test
  def shouldNotOverwriteEntityConfigsWhenUpdatingThrottledReplicas(): Unit = {
    val control = new TopicPartition(TestTopicName, 1) -> Seq(100, 102)
    val existing = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101), control)
    val proposed = Map(new TopicPartition(TestTopicName, 0) -> Seq(101, 102), control)
    
    //Then the dummy property should still be there
    class TestServiceClient extends TestServiceClientBase {
      override def updateTopicConfigs(topic: String, configChange: collection.Map[String, String]): Boolean = {
        assertEquals(TestTopicName, topic)
        assertFalse(configChange.contains("some-key"))
        assert(configChange.contains(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replicas
        assert(configChange.contains(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1

        true
      }
    }

    val service = new TestServiceClient
    val assigner = ReassignPartitionsCommand(service, null, null)

    //When
    assigner.assignThrottledReplicas(existing, proposed)
    assertEquals(1, calls)
  }

  @Test
  def shouldSetQuotaLimit(): Unit = {
    //Given
    val existing = Map(new TopicPartition(TestTopicName, 0) -> Seq(100, 101))
    val proposed = mutable.Map(new TopicPartition(TestTopicName, 0) -> Seq(101, 102))

    //Setup
    val service: ReassignCommandService = createMock(classOf[ReassignCommandService])
    val propsCapture: Capture[mutable.Map[String, String]] = newCapture(CaptureType.ALL)
    val assigner = ReassignPartitionsCommand(service,  proposed, Map.empty[TopicPartitionReplica, String])
    expect(service.getReplicaAssignmentForTopics(anyObject.asInstanceOf[Set[String]])).andStubReturn(existing)
    expect(service.updateBrokerConfigs(anyObject().asInstanceOf[Int], capture(propsCapture))).andReturn(true).anyTimes()

    replay(service)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then
    for (actual <- propsCapture.getValues.asScala) {
      assertEquals("1000", actual(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("1000", actual(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
      // No extra properties should be updated
      assertEquals(2, actual.size)
    }
    assertEquals(3, propsCapture.getValues.size) //3 brokers
  }

  @Test
  def shouldRemoveThrottleLimitFromAllBrokers(): Unit = {
    //Given 3 brokers, but with assignment only covering 2 of them
    val brokers = Seq(100, 101, 102)
    val status = mutable.Map(new TopicPartition(TestTopicName, 0) -> ReassignmentCompleted)

    //Setup
    val service: ReassignCommandService = createMock(classOf[ReassignCommandService])
    val brokerPropsCapture: Capture[Map[String, String]] = newCapture(CaptureType.ALL)
    val topicPropsCapture: Capture[Map[String, String]] = newCapture(CaptureType.ALL)
    expect(service.getBrokerIdsInCluster).andStubReturn(brokers)
    expect(service.updateBrokerConfigs(anyObject().asInstanceOf[Int], capture(brokerPropsCapture))).andReturn(true).anyTimes()
    expect(service.updateTopicConfigs(is(TestTopicName), capture(topicPropsCapture))).andReturn(true).anyTimes()
    replay(service)

    //When
    ReassignPartitionsCommand.removeThrottle(service, status, Map.empty[TopicPartitionReplica, ReassignmentStatus])

    //The throttle props ONLY should have been set to "" for the brokers
    for (capture <- brokerPropsCapture.getValues.asScala) {
      assertEquals("", capture(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
      assertEquals("", capture(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("", capture(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp))
      assertEquals(3, capture.size) // Only these three properties
    }
    assertEquals(3, brokerPropsCapture.getValues.size) //3 brokers

    // The throttle props should ALSO be cleared for the topic
    for (capture <- topicPropsCapture.getValues.asScala) {
      assertEquals("", capture(LogConfig.FollowerReplicationThrottledReplicasProp))
      assertEquals("", capture(LogConfig.LeaderReplicationThrottledReplicasProp))
      assertEquals(2, capture.size)
    }
    assertEquals(1, topicPropsCapture.getValues.size) // 1 topic

  }

  @Test
  def shouldRemoveThrottleReplicaListBasedOnProposedAssignment(): Unit = {
    val brokers = Seq(100, 101)

    //Given two topics being reassigned
    val status = mutable.Map(new TopicPartition(TestTopicName, 0) -> ReassignmentCompleted,
                             new TopicPartition(TestTopicAlternateName, 0) -> ReassignmentCompleted)

    //Setup
    val service: ReassignCommandService = createMock(classOf[ReassignCommandService])
    val brokerPropsCapture: Capture[Map[String, String]] = newCapture(CaptureType.ALL)
    val topicPropsCapture: Capture[Map[String, String]] = newCapture(CaptureType.ALL)
    expect(service.getBrokerIdsInCluster).andStubReturn(brokers)
    expect(service.updateBrokerConfigs(anyObject().asInstanceOf[Int], capture(brokerPropsCapture))).andReturn(true).anyTimes()

    // Should change configs of both topics
    expect(service.updateTopicConfigs(is(TestTopicName), capture(topicPropsCapture))).andReturn(true).anyTimes()
    expect(service.updateTopicConfigs(is(TestTopicAlternateName), capture(topicPropsCapture))).andReturn(true).anyTimes()

    replay(service)

    //When
    ReassignPartitionsCommand.removeThrottle(service, status, Map.empty[TopicPartitionReplica, ReassignmentStatus])

    //The throttle props ONLY should have been set to "" for the brokers
    for (capture <- brokerPropsCapture.getValues.asScala) {
      assertEquals("", capture(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
      assertEquals("", capture(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("", capture(DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp))
      assertEquals(3, capture.size) // Only these three properties
    }
    assertEquals(2, brokerPropsCapture.getValues.size) //2 brokers

    //Then topic props should have gone (dummy remains)
    for (actual <- topicPropsCapture.getValues.asScala) {
      assertEquals("", actual(LogConfig.LeaderReplicationThrottledReplicasProp))
      assertEquals("", actual(LogConfig.FollowerReplicationThrottledReplicasProp))
    }
    assertEquals(2, topicPropsCapture.getValues.size) //2 topics
  }

  @Test
  def testPartitionReassignmentWithLeaderInNewReplicas(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // Admin client
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)))
    val adminClient = AdminClient.create(props)
    // Create serviceClient here for test uses below
    val serviceClient = new AdminClientReassignCommandService(adminClient)

    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // reassign partition 0
    val newReplicas = Seq(0, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = ReassignPartitionsCommand(serviceClient, Map(topicAndPartition -> newReplicas), Map.empty[TopicPartitionReplica, String])
    assertTrue("Partition reassignment attempt failed for [test, 0]", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
      ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(serviceClient, Map(topicAndPartition -> newReplicas))
        .getOrElse(topicAndPartition, fail(s"Failed to get reassignment status for $topicAndPartition")) == ReassignmentCompleted
    },
      "Partition reassignment should complete")
    val assignedReplicas = serviceClient.getReplicaAssignmentForTopics(Set(topic)).get(new TopicPartition(topic, partitionToBeReassigned)).get
    // in sync replicas should not have any replica that is not in the new assigned replicas
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
      "New replicas should exist on brokers")

    serviceClient.close()
  }

  @Test
  def testPartitionReassignmentWithLeaderNotInNewReplicas(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // Admin client
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)))
    val adminClient = AdminClient.create(props)
    val serviceClient = new AdminClientReassignCommandService(adminClient)

    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // reassign partition 0
    val newReplicas = Seq(1, 2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = ReassignPartitionsCommand(serviceClient,  Map(topicAndPartition -> newReplicas), Map.empty[TopicPartitionReplica, String])
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(serviceClient, Map(topicAndPartition -> newReplicas))
          .getOrElse(topicAndPartition, fail(s"Failed to get reassignment status for $topicAndPartition")) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = serviceClient.getReplicaAssignmentForTopics(Set(topic)).get(new TopicPartition(topic, partitionToBeReassigned)).get
    // val assignedReplicas = zkClient.getReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    assertEquals("Partition should have been reassigned to 0, 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    serviceClient.close()
  }


  @Test
  def testPartitionReassignmentNonOverlappingReplicas(): Unit = {
    val expectedReplicaAssignment = Map(0  -> List(0, 1))
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // Admin client
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)))
    val adminClient = AdminClient.create(props)
    val serviceClient = new AdminClientReassignCommandService(adminClient)
    // create the topic
    TestUtils.createTopic(zkClient, topic, expectedReplicaAssignment, servers)
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = ReassignPartitionsCommand(serviceClient,  Map(topicAndPartition -> newReplicas), Map.empty[TopicPartitionReplica, String])
    assertTrue("Partition reassignment failed for test, 0", reassignPartitionsCommand.reassignPartitions())
    // wait until reassignment is completed
    TestUtils.waitUntilTrue(() => {
        ReassignPartitionsCommand.checkIfPartitionReassignmentSucceeded(serviceClient, Map(topicAndPartition -> newReplicas))
          .getOrElse(topicAndPartition, fail(s"Failed to get reassignment status for $topicAndPartition")) == ReassignmentCompleted
      },
      "Partition reassignment should complete")
    val assignedReplicas = serviceClient.getReplicaAssignmentForTopics(Set(topic)).get(new TopicPartition(topic, partitionToBeReassigned)).get
    //zkClient.getReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    assertEquals("Partition should have been reassigned to 2, 3", newReplicas, assignedReplicas)
    checkForPhantomInSyncReplicas(zkClient, topic, partitionToBeReassigned, assignedReplicas)
    ensureNoUnderReplicatedPartitions(zkClient, topic, partitionToBeReassigned, assignedReplicas, servers)
    TestUtils.waitUntilTrue(() => getBrokersWithPartitionDir(servers, topic, 0) == newReplicas.toSet,
                            "New replicas should exist on brokers")
    serviceClient.close()
  }

  @Test
  def testReassigningNonExistingPartition(): Unit = {
    val topic = "test"
    // create brokers
    servers = TestUtils.createBrokerConfigs(4, zkConnect, false).map(b => TestUtils.createServer(KafkaConfig.fromProps(b)))
    // Admin client
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestUtils.bootstrapServers(servers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)))
    val adminClient = AdminClient.create(props)
    val serviceClient = new AdminClientReassignCommandService(adminClient)
    // reassign partition 0
    val newReplicas = Seq(2, 3)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = ReassignPartitionsCommand(serviceClient,  Map(topicAndPartition -> newReplicas), Map.empty[TopicPartitionReplica, String])
    assertFalse("Partition reassignment failed for test, 0", Try(reassignPartitionsCommand.reassignPartitions()).isSuccess)
    val reassignedPartitions = zkClient.getPartitionReassignment
    assertFalse("Partition should not be reassigned", reassignedPartitions.contains(topicAndPartition))
    serviceClient.close()
  }


  @Test
  def testResumePartitionReassignmentThatWasCompleted(): Unit = {
    // XXX: This test only works with the ZK client because Admin clients require the cluster to be up when issuing
    // a reassignment.
    val initialAssignment = Map(0  -> List(0, 2))
    val topic = "test"
    // create the topic
    adminZkClient.createTopicWithAssignment(topic, config = new Properties, initialAssignment)
    // put the partition in the reassigned path as well
    // reassign partition 0
    val newReplicas = Seq(0, 1)
    val partitionToBeReassigned = 0
    val topicAndPartition = new TopicPartition(topic, partitionToBeReassigned)
    val reassignPartitionsCommand = ReassignPartitionsCommand(zkClient, None, Map(topicAndPartition -> newReplicas), adminZkClientOpt = Some(adminZkClient))
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

  def stubZKClient(existingAssignment: Map[TopicPartition, Seq[Int]] = Map[TopicPartition, Seq[Int]](),
                   brokers: Seq[Int] = Seq[Int]()): KafkaZkClient = {
    val zkClient: KafkaZkClient = createMock(classOf[KafkaZkClient])
    expect(zkClient.getReplicaAssignmentForTopics(anyObject().asInstanceOf[Set[String]])).andStubReturn(existingAssignment)
    expect(zkClient.getSortedBrokerList).andStubReturn(brokers.sorted)
    replay(zkClient)
    zkClient
  }

  def toReplicaSet(throttledReplicasString: Any): Set[String] = {
    throttledReplicasString.toString.split(",").toSet
  }

}
