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
package kafka.api


import java.util.{Optional}
import kafka.integration.KafkaServerTestHarness
import kafka.log.LogConfig

import kafka.server.{Defaults, KafkaConfig}
import kafka.utils.TestUtils._
import kafka.utils.{TestInfoUtils, TestUtils}

import org.apache.kafka.clients.admin._

import org.apache.kafka.common.config.{ConfigResource}
import org.apache.kafka.common.errors._

import org.apache.kafka.common.{ElectionType, TopicPartition}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.annotation.nowarn
import scala.collection.Seq

import scala.jdk.CollectionConverters._


/**
 * An integration test of the KafkaAdminClient.
 *
 * Also see [[org.apache.kafka.clients.admin.KafkaAdminClientTest]] for unit tests of the admin client.
 */
class PlaintextAdminIntegrationTest extends BaseAdminIntegrationTest {

  val topic = "topic"
  val partition = 0
  val topicPartition = new TopicPartition(topic, partition)











  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
      topicPartition: TopicPartition,
      result: ElectLeadersResult
    ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
//    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
//    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
//    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
      topicPartition: TopicPartition,
      result: ElectLeadersResult
    ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders1(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders2(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders3(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders4(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders5(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders6(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders7(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders8(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("kraft"))
  def testElectPreferredLeaders9(quorum: String): Unit = {
    client = Admin.create(createConfig)

    val prefer0 = Seq(0, 1, 2)
    val prefer1 = Seq(1, 2, 0)
    val prefer2 = Seq(2, 0, 1)

    val partition1 = new TopicPartition("elect-preferred-leaders-topic-1", 0)
    createTopicWithAssignment(partition1.topic, Map[Int, Seq[Int]](partition1.partition -> prefer0))

    val partition2 = new TopicPartition("elect-preferred-leaders-topic-2", 0)
    createTopicWithAssignment(partition2.topic, Map[Int, Seq[Int]](partition2.partition -> prefer0))

    def preferredLeader(topicPartition: TopicPartition): Int = {
      val partitionMetadata = getTopicMetadata(client, topicPartition.topic).partitions.get(topicPartition.partition)
      val preferredLeaderMetadata = partitionMetadata.replicas.get(0)
      preferredLeaderMetadata.id
    }

    /** Changes the <i>preferred</i> leader without changing the <i>current</i> leader. */
    def changePreferredLeader(newAssignment: Seq[Int]): Unit = {
      val preferred = newAssignment.head
      val prior1 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition1.topic, partition1.partition(), listenerName).get.id()
      val prior2 = brokers.head.metadataCache.getPartitionLeaderEndpoint(partition2.topic, partition2.partition(), listenerName).get.id()

      var m = Map.empty[TopicPartition, Optional[NewPartitionReassignment]]
      if (prior1 != preferred)
        m += partition1 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      if (prior2 != preferred)
        m += partition2 -> Optional.of(new NewPartitionReassignment(newAssignment.map(Int.box).asJava))
      client.alterPartitionReassignments(m.asJava).all().get()

      try {
        TestUtils.waitUntilTrue(
          () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
          s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
          10000)
      } catch {
        case e: Throwable =>
          System.err.print(s"Ex $preferred:${preferredLeader(partition1)},${preferredLeader(partition2)}")
          System.err.flush()
          TestUtils.waitUntilTrue(
            () => preferredLeader(partition1) == preferred && preferredLeader(partition2) == preferred,
            s"Expected preferred leader to become $preferred, but is ${preferredLeader(partition1)} and ${preferredLeader(partition2)}",
            10000)

          fail("passed 2nd try!")
      }

      // Check the leader hasn't moved
      TestUtils.assertLeader(client, partition1, prior1)
      TestUtils.assertLeader(client, partition2, prior2)
    }

    // Check current leaders are 0
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Noop election
    var electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    val exception = electResult.partitions.get.get(partition1).get
    assertEquals(classOf[ElectionNotNeededException], exception.getClass)
    TestUtils.assertLeader(client, partition1, 0)

    // Noop election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertTrue(electResult.partitions.get.isEmpty)
    TestUtils.assertLeader(client, partition1, 0)
    TestUtils.assertLeader(client, partition2, 0)

    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)

    // meaningful election
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition1).isPresent)
    TestUtils.assertLeader(client, partition1, 1)

    // topic 2 unchanged
    assertFalse(electResult.partitions.get.containsKey(partition2))
    TestUtils.assertLeader(client, partition2, 0)

    // meaningful election with null partitions
    electResult = client.electLeaders(ElectionType.PREFERRED, null)
    assertEquals(Set(partition2), electResult.partitions.get.keySet.asScala)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 1)

    def assertUnknownTopicOrPartition(
                                       topicPartition: TopicPartition,
                                       result: ElectLeadersResult
                                     ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[UnknownTopicOrPartitionException], exception.getClass)
      if (isKRaftTest()) {
        assertEquals(s"No such topic as ${topicPartition.topic()}", exception.getMessage)
      } else {
        assertEquals("The partition does not exist.", exception.getMessage)
      }
    }

    // unknown topic
    val unknownPartition = new TopicPartition("topic-does-not-exist", 0)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition).asJava)
    assertEquals(Set(unknownPartition).asJava, electResult.partitions.get.keySet)
    assertUnknownTopicOrPartition(unknownPartition, electResult)
    TestUtils.assertLeader(client, partition1, 1)
    TestUtils.assertLeader(client, partition2, 1)

    // Now change the preferred leader to 2
    changePreferredLeader(prefer2)

    // mixed results
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(unknownPartition, partition1).asJava)
    assertEquals(Set(unknownPartition, partition1).asJava, electResult.partitions.get.keySet)
    TestUtils.assertLeader(client, partition1, 2)
    TestUtils.assertLeader(client, partition2, 1)
    assertUnknownTopicOrPartition(unknownPartition, electResult)

    // elect preferred leader for partition 2
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition2).asJava)
    assertEquals(Set(partition2).asJava, electResult.partitions.get.keySet)
    assertFalse(electResult.partitions.get.get(partition2).isPresent)
    TestUtils.assertLeader(client, partition2, 2)

    System.err.print("! ")
    System.err.flush()
    //    System.out.println("!!! change to 1")
    // Now change the preferred leader to 1
    changePreferredLeader(prefer1)
    System.err.print("!!")
    System.err.flush()
    //    System.out.println("!!! kill")
    // but shut it down...
    killBroker(1)
    //    System.out.println("!!! wait")
    TestUtils.waitForBrokersOutOfIsr(client, Set(partition1, partition2), Set(1))

    def assertPreferredLeaderNotAvailable(
                                           topicPartition: TopicPartition,
                                           result: ElectLeadersResult
                                         ): Unit = {
      val exception = result.partitions.get.get(topicPartition).get
      assertEquals(classOf[PreferredLeaderNotAvailableException], exception.getClass)
      if (isKRaftTest()) {
        assertTrue(exception.getMessage.contains(
          "The preferred leader was not available."),
          s"Unexpected message: ${exception.getMessage}")
      } else {
        assertTrue(exception.getMessage.contains(
          s"Failed to elect leader for partition $topicPartition under strategy PreferredReplicaPartitionLeaderElectionStrategy"),
          s"Unexpected message: ${exception.getMessage}")
      }
    }

    // ... now what happens if we try to elect the preferred leader and it's down?
    val shortTimeout = new ElectLeadersOptions().timeoutMs(10000)
    electResult = client.electLeaders(ElectionType.PREFERRED, Set(partition1).asJava, shortTimeout)
    assertEquals(Set(partition1).asJava, electResult.partitions.get.keySet)

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    // preferred leader unavailable with null argument
    electResult = client.electLeaders(ElectionType.PREFERRED, null, shortTimeout)
    assertTrue(Set(partition1, partition2).subsetOf(electResult.partitions.get.keySet.asScala))

    assertPreferredLeaderNotAvailable(partition1, electResult)
    TestUtils.assertLeader(client, partition1, 2)

    assertPreferredLeaderNotAvailable(partition2, electResult)
    TestUtils.assertLeader(client, partition2, 2)
  }


}

object PlaintextAdminIntegrationTest {

  @nowarn("cat=deprecation")
  def checkValidAlterConfigs(
    admin: Admin,
    test: KafkaServerTestHarness,
    topicResource1: ConfigResource,
    topicResource2: ConfigResource
  ): Unit = {
    // Alter topics
    var topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.FlushMsProp, "1000")
    ).asJava

    var topicConfigEntries2 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.9"),
      new ConfigEntry(LogConfig.CompressionTypeProp, "lz4")
    ).asJava

    var alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were updated correctly
    test.ensureConsistentKRaftMetadata()
    var describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2).asJava)
    var configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals("1000", configs.get(topicResource1).get(LogConfig.FlushMsProp).value)
    assertEquals(Defaults.MessageMaxBytes.toString,
      configs.get(topicResource1).get(LogConfig.MaxMessageBytesProp).value)
    assertEquals((Defaults.LogRetentionHours * 60 * 60 * 1000).toString,
      configs.get(topicResource1).get(LogConfig.RetentionMsProp).value)

    assertEquals("0.9", configs.get(topicResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals("lz4", configs.get(topicResource2).get(LogConfig.CompressionTypeProp).value)

    // Alter topics with validateOnly=true
    topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.MaxMessageBytesProp, "10")
    ).asJava

    topicConfigEntries2 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "0.3")
    ).asJava

    alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2).asJava, alterResult.values.keySet)
    alterResult.all.get

    // Verify that topics were not updated due to validateOnly = true
    test.ensureConsistentKRaftMetadata()
    describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2).asJava)
    configs = describeResult.all.get

    assertEquals(2, configs.size)

    assertEquals(Defaults.MessageMaxBytes.toString,
      configs.get(topicResource1).get(LogConfig.MaxMessageBytesProp).value)
    assertEquals("0.9", configs.get(topicResource2).get(LogConfig.MinCleanableDirtyRatioProp).value)
  }

  @nowarn("cat=deprecation")
  def checkInvalidAlterConfigs(
    test: KafkaServerTestHarness,
    admin: Admin
  ): Unit = {
    // Create topics
    val topic1 = "invalid-alter-configs-topic-1"
    val topicResource1 = new ConfigResource(ConfigResource.Type.TOPIC, topic1)
    createTopicWithAdmin(admin, topic1, test.brokers, numPartitions = 1, replicationFactor = 1)

    val topic2 = "invalid-alter-configs-topic-2"
    val topicResource2 = new ConfigResource(ConfigResource.Type.TOPIC, topic2)
    createTopicWithAdmin(admin, topic2, test.brokers, numPartitions = 1, replicationFactor = 1)

    val topicConfigEntries1 = Seq(
      new ConfigEntry(LogConfig.MinCleanableDirtyRatioProp, "1.1"), // this value is invalid as it's above 1.0
      new ConfigEntry(LogConfig.CompressionTypeProp, "lz4")
    ).asJava

    var topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.CompressionTypeProp, "snappy")).asJava

    val brokerResource = new ConfigResource(ConfigResource.Type.BROKER, test.brokers.head.config.brokerId.toString)
    val brokerConfigEntries = Seq(new ConfigEntry(KafkaConfig.ZkConnectProp, "localhost:2181")).asJava

    // Alter configs: first and third are invalid, second is valid
    var alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava)

    assertEquals(Set(topicResource1, topicResource2, brokerResource).asJava, alterResult.values.keySet)
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource1), classOf[InvalidConfigurationException])
    alterResult.values.get(topicResource2).get
    assertFutureExceptionTypeEquals(alterResult.values.get(brokerResource), classOf[InvalidRequestException])

    // Verify that first and third resources were not updated and second was updated
    test.ensureConsistentKRaftMetadata()
    var describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2, brokerResource).asJava)
    var configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString,
      configs.get(topicResource1).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.CompressionType,
      configs.get(topicResource1).get(LogConfig.CompressionTypeProp).value)

    assertEquals("snappy", configs.get(topicResource2).get(LogConfig.CompressionTypeProp).value)

    assertEquals(Defaults.CompressionType, configs.get(brokerResource).get(KafkaConfig.CompressionTypeProp).value)

    // Alter configs with validateOnly = true: first and third are invalid, second is valid
    topicConfigEntries2 = Seq(new ConfigEntry(LogConfig.CompressionTypeProp, "gzip")).asJava

    alterResult = admin.alterConfigs(Map(
      topicResource1 -> new Config(topicConfigEntries1),
      topicResource2 -> new Config(topicConfigEntries2),
      brokerResource -> new Config(brokerConfigEntries)
    ).asJava, new AlterConfigsOptions().validateOnly(true))

    assertEquals(Set(topicResource1, topicResource2, brokerResource).asJava, alterResult.values.keySet)
    assertFutureExceptionTypeEquals(alterResult.values.get(topicResource1), classOf[InvalidConfigurationException])
    alterResult.values.get(topicResource2).get
    assertFutureExceptionTypeEquals(alterResult.values.get(brokerResource), classOf[InvalidRequestException])

    // Verify that no resources are updated since validate_only = true
    test.ensureConsistentKRaftMetadata()
    describeResult = admin.describeConfigs(Seq(topicResource1, topicResource2, brokerResource).asJava)
    configs = describeResult.all.get
    assertEquals(3, configs.size)

    assertEquals(Defaults.LogCleanerMinCleanRatio.toString,
      configs.get(topicResource1).get(LogConfig.MinCleanableDirtyRatioProp).value)
    assertEquals(Defaults.CompressionType,
      configs.get(topicResource1).get(LogConfig.CompressionTypeProp).value)

    assertEquals("snappy", configs.get(topicResource2).get(LogConfig.CompressionTypeProp).value)

    assertEquals(Defaults.CompressionType, configs.get(brokerResource).get(KafkaConfig.CompressionTypeProp).value)
  }
}
