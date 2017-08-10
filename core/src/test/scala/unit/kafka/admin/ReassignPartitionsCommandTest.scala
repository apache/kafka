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

import java.util.Properties

import kafka.admin.ReassignPartitionsCommand.Throttle
import kafka.common.TopicAndPartition
import kafka.log.LogConfig
import kafka.log.LogConfig._
import kafka.server.{ConfigType, DynamicConfig}
import kafka.utils.CoreUtils._
import kafka.utils.TestUtils._
import kafka.utils.{CoreUtils, Logging, TestUtils, ZkUtils}
import org.easymock.EasyMock._
import org.easymock.{Capture, CaptureType, EasyMock}
import org.junit.{Before, Test}
import org.junit.Assert.{assertEquals, assertNull, fail}

import scala.collection.{Seq, mutable}
import scala.collection.JavaConversions._

class ReassignPartitionsCommandTest extends Logging {
  var calls = 0

  @Test
  def shouldFindMovingReplicas() {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null)

    //Given partition 0 moves from broker 100 -> 102. Partition 1 does not move.
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), control)


    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("0:102", configChange.get(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replica
        assertEquals("0:100,0:101", configChange.get(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
    }

    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasWhenProposedIsSubsetOfExisting() {
    val assigner = new ReassignPartitionsCommand(null, null, null, null)

    //Given we have more existing partitions than we are proposing
    val existingSuperset = Map(
      TopicAndPartition("topic1", 0) -> Seq(100, 101),
      TopicAndPartition("topic1", 1) -> Seq(100, 102),
      TopicAndPartition("topic1", 2) -> Seq(100, 101),
      TopicAndPartition("topic2", 0) -> Seq(100, 101, 102),
      TopicAndPartition("topic3", 0) -> Seq(100, 101, 102)
    )
    val proposedSubset = Map(
      TopicAndPartition("topic1", 0) -> Seq(101, 102),
      TopicAndPartition("topic1", 1) -> Seq(102),
      TopicAndPartition("topic1", 2) -> Seq(100, 101, 102)
    )

    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("0:102,2:102", configChange.get(FollowerReplicationThrottledReplicasProp))
        assertEquals("0:100,0:101,2:100,2:101", configChange.get(LeaderReplicationThrottledReplicasProp))
        assertEquals("topic1", topic)
        calls += 1
      }
    }

    //Then replicas should assign correctly (based on the proposed map)
    assigner.assignThrottledReplicas(existingSuperset, proposedSubset, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultiplePartitions() {
    val control = TopicAndPartition("topic1", 2) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null)

    //Given partitions 0 & 1 moves from broker 100 -> 102. Partition 2 does not move.
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), TopicAndPartition("topic1", 1) -> Seq(100, 101), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), TopicAndPartition("topic1", 1) -> Seq(101, 102), control)

    // Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("0:102,1:102", configChange.get(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replica
        assertEquals("0:100,0:101,1:100,1:101", configChange.get(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopics() {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null)

    //Given topics 1 -> move from broker 100 -> 102, topics 2 -> move from broker 101 -> 100
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), TopicAndPartition("topic2", 0) -> Seq(101, 102), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), TopicAndPartition("topic2", 0) -> Seq(100, 102), control)

    //Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        topic match {
          case "topic1" =>
            assertEquals("0:100,0:101", configChange.get(LeaderReplicationThrottledReplicasProp))
            assertEquals("0:102", configChange.get(FollowerReplicationThrottledReplicasProp))
          case "topic2" =>
            assertEquals("0:101,0:102", configChange.get(LeaderReplicationThrottledReplicasProp))
            assertEquals("0:100", configChange.get(FollowerReplicationThrottledReplicasProp))
          case _ => fail(s"Unexpected topic $topic")
        }
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindMovingReplicasMultipleTopicsAndPartitions() {
    val assigner = new ReassignPartitionsCommand(null, null, null, null)

    //Given
    val existing = Map(
      TopicAndPartition("topic1", 0) -> Seq(100, 101),
      TopicAndPartition("topic1", 1) -> Seq(100, 101),
      TopicAndPartition("topic2", 0) -> Seq(101, 102),
      TopicAndPartition("topic2", 1) -> Seq(101, 102)
    )
    val proposed = Map(
      TopicAndPartition("topic1", 0) -> Seq(101, 102), //moves to 102
      TopicAndPartition("topic1", 1) -> Seq(101, 102), //moves to 102
      TopicAndPartition("topic2", 0) -> Seq(100, 102), //moves to 100
      TopicAndPartition("topic2", 1) -> Seq(101, 100)  //moves to 100
    )

    //Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        topic match {
          case "topic1" =>
            assertEquals("0:102,1:102", configChange.get(FollowerReplicationThrottledReplicasProp))
            assertEquals("0:100,0:101,1:100,1:101", configChange.get(LeaderReplicationThrottledReplicasProp))
          case "topic2" =>
            assertEquals("0:100,1:100", configChange.get(FollowerReplicationThrottledReplicasProp))
            assertEquals("0:101,0:102,1:101,1:102", configChange.get(LeaderReplicationThrottledReplicasProp))
          case _ => fail(s"Unexpected topic $topic")
        }
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(2, calls)
  }

  @Test
  def shouldFindTwoMovingReplicasInSamePartition() {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null)

    //Given partition 0 has 2 moves from broker 102 -> 104 & 103 -> 105
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101, 102, 103), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101, 104, 105), control)

    // Then
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties) = {
        assertEquals("0:104,0:105", configChange.get(FollowerReplicationThrottledReplicasProp)) //Should only be follower-throttle the moving replicas
        assertEquals("0:100,0:101,0:102,0:103", configChange.get(LeaderReplicationThrottledReplicasProp)) //Should leader-throttle all existing (pre move) replicas
        calls += 1
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldNotOverwriteEntityConfigsWhenUpdatingThrottledReplicas(): Unit = {
    val control = TopicAndPartition("topic1", 1) -> Seq(100, 102)
    val assigner = new ReassignPartitionsCommand(null, null, null, null)
    val existing = Map(TopicAndPartition("topic1", 0) -> Seq(100, 101), control)
    val proposed = Map(TopicAndPartition("topic1", 0) -> Seq(101, 102), control)

    //Given partition there are existing properties
    val existingProperties = propsWith("some-key", "some-value")

    //Then the dummy property should still be there
    val mock = new TestAdminUtils {
      override def changeTopicConfig(zkUtils: ZkUtils, topic: String, configChange: Properties): Unit = {
        assertEquals("some-value", configChange.getProperty("some-key"))
        calls += 1
      }

      override def fetchEntityConfig(zkUtils: ZkUtils, entityType: String, entityName: String): Properties = {
        existingProperties
      }
    }

    //When
    assigner.assignThrottledReplicas(existing, proposed, mock)
    assertEquals(1, calls)
  }

  @Test
  def shouldSetQuotaLimit(): Unit = {
    //Given
    val existing = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(100, 101))
    val proposed = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(101, 102))

    //Setup
    val zk = stubZK(existing)
    val admin = createMock(classOf[AdminUtilities])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    val assigner = new ReassignPartitionsCommand(zk, None, proposed, Map.empty, admin)
    expect(admin.fetchEntityConfig(is(zk), anyString(), anyString())).andStubReturn(new Properties)
    expect(admin.changeBrokerConfig(is(zk), anyObject().asInstanceOf[List[Int]], capture(propsCapture))).anyTimes()
    replay(admin)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then
    for (actual <- propsCapture.getValues) {
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //3 brokers
  }

  @Test
  def shouldUpdateQuotaLimit(): Unit = {
    //Given
    val existing = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(100, 101))
    val proposed = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(101, 102))

    //Setup
    val zk = stubZK(existing)
    val admin = createMock(classOf[AdminUtilities])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    val assigner = new ReassignPartitionsCommand(zk, None, proposed, Map.empty, admin)
    expect(admin.changeBrokerConfig(is(zk), anyObject().asInstanceOf[List[Int]], capture(propsCapture))).anyTimes()

    //Expect the existing broker config to be changed from 10/100 to 1000
    val existingConfigs = CoreUtils.propsWith(
      (DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "10"),
      (DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "100")
    )
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), is("100"))).andReturn(copyOf(existingConfigs))
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), is("101"))).andReturn(copyOf(existingConfigs))
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), is("102"))).andReturn(copyOf(existingConfigs))
    replay(admin)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then
    for (actual <- propsCapture.getValues) {
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
      assertEquals("1000", actual.getProperty(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //three brokers
  }

  @Test
  def shouldNotOverwriteExistingPropertiesWhenLimitIsAdded(): Unit = {
    //Given
    val existing = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(100, 101))
    val proposed = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(101, 102))

    //Setup
    val zk = stubZK(existing)
    val admin = createMock(classOf[AdminUtilities])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    val assigner = new ReassignPartitionsCommand(zk, None, proposed, Map.empty, admin)
    expect(admin.changeBrokerConfig(is(zk), anyObject().asInstanceOf[List[Int]], capture(propsCapture))).anyTimes()

    //Given there is some existing config
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), anyString())).andReturn(
      propsWith("useful.key", "useful.value")).atLeastOnce()

    replay(admin)

    //When
    assigner.maybeLimit(Throttle(1000))

    //Then other property remains
    for (actual <- propsCapture.getValues) {
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
    val proposed = mutable.Map(TopicAndPartition("topic1", 0) -> Seq(100, 101))
    val status = mutable.Map(TopicAndPartition("topic1", 0) -> ReassignmentCompleted)
    val existingBrokerConfigs = propsWith(
      (DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "10"),
      (DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "100"),
      ("useful.key", "value")
    )

    //Setup
    val zk = stubZK(brokers = brokers)
    val admin = createMock(classOf[AdminUtilities])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Topic), anyString())).andStubReturn(new Properties)
    expect(admin.changeBrokerConfig(is(zk), anyObject().asInstanceOf[Seq[Int]], capture(propsCapture))).anyTimes()
    //Stub each invocation as EasyMock caches the return value which can be mutated
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), is("100"))).andReturn(copyOf(existingBrokerConfigs))
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), is("101"))).andReturn(copyOf(existingBrokerConfigs))
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), is("102"))).andReturn(copyOf(existingBrokerConfigs))
    replay(admin)

    //When
    ReassignPartitionsCommand.removeThrottle(zk, proposed, status, admin)

    //Then props should have gone (dummy remains)
    for (capture <- propsCapture.getValues) {
      assertEquals("value", capture.get("useful.key"))
      assertNull(capture.get(DynamicConfig.Broker.FollowerReplicationThrottledRateProp))
      assertNull(capture.get(DynamicConfig.Broker.LeaderReplicationThrottledRateProp))
    }
    assertEquals(3, propsCapture.getValues.size) //3 brokers
  }

  @Test
  def shouldRemoveThrottleReplicaListBasedOnProposedAssignment(): Unit = {

    //Given two topics with existing config
    val proposed = mutable.Map(
      TopicAndPartition("topic1", 0) -> Seq(100, 101),
      TopicAndPartition("topic2", 0) -> Seq(100, 101)
    )
    val status = mutable.Map(TopicAndPartition("topic1", 0) -> ReassignmentCompleted)
    val existingConfigs = CoreUtils.propsWith(
      (LogConfig.LeaderReplicationThrottledReplicasProp, "1:100:2:100"),
      (LogConfig.FollowerReplicationThrottledReplicasProp, "1:101,2:101"),
      ("useful.key", "value")
    )

    //Setup
    val zk = stubZK(brokers = Seq(100, 101))
    val admin = createMock(classOf[AdminUtilities])
    val propsCapture: Capture[Properties] = newCapture(CaptureType.ALL)
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Broker), anyString())).andStubReturn(new Properties)
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Topic), is("topic1"))).andStubReturn(copyOf(existingConfigs))
    expect(admin.fetchEntityConfig(is(zk), is(ConfigType.Topic), is("topic2"))).andStubReturn(copyOf(existingConfigs))

    //Should change both topics
    expect(admin.changeTopicConfig(is(zk), is("topic1"), capture(propsCapture)))
    expect(admin.changeTopicConfig(is(zk), is("topic2"), capture(propsCapture)))

    replay(admin)

    //When
    ReassignPartitionsCommand.removeThrottle(zk, proposed, status, admin)

    //Then props should have gone (dummy remains)
    for (actual <- propsCapture.getValues) {
      assertEquals("value", actual.getProperty("useful.key"))
      assertNull(actual.getProperty(LogConfig.LeaderReplicationThrottledReplicasProp))
      assertNull(actual.getProperty(LogConfig.FollowerReplicationThrottledReplicasProp))
    }
    assertEquals(2, propsCapture.getValues.size) //2 topics
  }

  //Override eq as is for brevity
  def is[T](v: T): T = EasyMock.eq(v)

  @Before
  def setup(): Unit = {
    calls = 0
  }

  def stubZK(existingAssignment: mutable.Map[TopicAndPartition, Seq[Int]] = mutable.Map[TopicAndPartition, Seq[Int]](),
             brokers: Seq[Int] = Seq[Int]()): ZkUtils = {
    val zk = createMock(classOf[ZkUtils])
    expect(zk.getReplicaAssignmentForTopics(anyObject().asInstanceOf[Seq[String]])).andStubReturn(existingAssignment)
    expect(zk.getAllBrokersInCluster()).andStubReturn(brokers.map(TestUtils.createBroker(_, "", 1)))
    replay(zk)
    zk
  }
}
