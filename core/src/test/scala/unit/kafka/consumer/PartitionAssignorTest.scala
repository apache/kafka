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

package kafka.consumer

import org.easymock.EasyMock
import org.I0Itec.zkclient.ZkClient
import org.apache.zookeeper.data.Stat
import kafka.utils.{TestUtils, Logging, ZkUtils, Json}
import org.junit.Assert._
import kafka.common.TopicAndPartition
import kafka.consumer.PartitionAssignorTest.StaticSubscriptionInfo
import kafka.consumer.PartitionAssignorTest.Scenario
import kafka.consumer.PartitionAssignorTest.WildcardSubscriptionInfo
import org.junit.Test

@deprecated("This test has been deprecated and will be removed in a future release.", "0.11.0.0")
class PartitionAssignorTest extends Logging {

  @Test
  def testRoundRobinPartitionAssignor() {
    val assignor = new RoundRobinAssignor

    /** various scenarios with only wildcard consumers */
    (1 to PartitionAssignorTest.TestCaseCount).foreach { _ =>
      val consumerCount = 1.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxConsumerCount + 1))
      val topicCount = PartitionAssignorTest.MinTopicCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxTopicCount + 1))

      val topicPartitionCounts = Map((1 to topicCount).map(topic => {
        ("topic-" + topic, PartitionAssignorTest.MinPartitionCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxPartitionCount)))
      }):_*)

      val subscriptions = Map((1 to consumerCount).map { consumer =>
        val streamCount = 1.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxStreamCount + 1))
        ("g1c" + consumer, WildcardSubscriptionInfo(streamCount, ".*", isWhitelist = true))
      }:_*)
      val scenario = Scenario("g1", topicPartitionCounts, subscriptions)
      val zkUtils = PartitionAssignorTest.setupZkClientMock(scenario)
      EasyMock.replay(zkUtils.zkClient)
      PartitionAssignorTest.assignAndVerify(scenario, assignor, zkUtils, verifyAssignmentIsUniform = true)
    }
  }

  @Test
  def testRoundRobinPartitionAssignorStaticSubscriptions() {
    val assignor = new RoundRobinAssignor

    /** test static subscription scenarios */
    (1 to PartitionAssignorTest.TestCaseCount).foreach (testCase => {
      val consumerCount = 1.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxConsumerCount + 1))
      val topicCount = PartitionAssignorTest.MinTopicCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxTopicCount + 1))

      val topicPartitionCounts = Map((1 to topicCount).map(topic => {
        ("topic-" + topic, PartitionAssignorTest.MinPartitionCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxPartitionCount)))
      }).toSeq:_*)

      val subscriptions = Map((1 to consumerCount).map(consumer => {
        val streamCounts = Map((1 to topicCount).map(topic => {
            ("topic-" + topic, 1)
          }).toSeq:_*)
        ("g1c" + consumer, StaticSubscriptionInfo(streamCounts))
      }).toSeq:_*)
      val scenario = Scenario("g1", topicPartitionCounts, subscriptions)
      val zkUtils = PartitionAssignorTest.setupZkClientMock(scenario)
      EasyMock.replay(zkUtils.zkClient)
      PartitionAssignorTest.assignAndVerify(scenario, assignor, zkUtils, verifyAssignmentIsUniform = true)
    })
  }

  @Test
  def testRoundRobinPartitionAssignorUnbalancedStaticSubscriptions() {
    val assignor = new RoundRobinAssignor
    val minConsumerCount = 5

    /** test unbalanced static subscription scenarios */
    (1 to PartitionAssignorTest.TestCaseCount).foreach (testCase => {
      val consumerCount = minConsumerCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxConsumerCount + 1))
      val topicCount = 10

      val topicPartitionCounts = Map((1 to topicCount).map(topic => {
        ("topic-" + topic, 10)
      }).toSeq:_*)

      val subscriptions = Map((1 to consumerCount).map(consumer => {
        // Exclude some topics from some consumers
        val topicRange = (1 to topicCount - consumer % minConsumerCount)
        val streamCounts = Map(topicRange.map(topic => {
            ("topic-" + topic, 3)
          }).toSeq:_*)
        ("g1c" + consumer, StaticSubscriptionInfo(streamCounts))
      }).toSeq:_*)
      val scenario = Scenario("g1", topicPartitionCounts, subscriptions)
      val zkUtils = PartitionAssignorTest.setupZkClientMock(scenario)
      EasyMock.replay(zkUtils.zkClient)
      PartitionAssignorTest.assignAndVerify(scenario, assignor, zkUtils)
    })
  }

  @Test
  def testRangePartitionAssignor() {
    val assignor = new RangeAssignor
    (1 to PartitionAssignorTest.TestCaseCount).foreach { _ =>
      val consumerCount = 1.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxConsumerCount + 1))
      val topicCount = PartitionAssignorTest.MinTopicCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxTopicCount + 1))

      val topicPartitionCounts = Map((1 to topicCount).map(topic => {
        ("topic-" + topic, PartitionAssignorTest.MinPartitionCount.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxPartitionCount)))
      }):_*)

      val subscriptions = Map((1 to consumerCount).map { consumer =>
        val streamCounts = Map((1 to topicCount).map(topic => {
            val streamCount = 1.max(TestUtils.random.nextInt(PartitionAssignorTest.MaxStreamCount + 1))
            ("topic-" + topic, streamCount)
          }):_*)
        ("g1c" + consumer, StaticSubscriptionInfo(streamCounts))
      }:_*)
      val scenario = Scenario("g1", topicPartitionCounts, subscriptions)
      val zkUtils = PartitionAssignorTest.setupZkClientMock(scenario)
      EasyMock.replay(zkUtils.zkClient)

      PartitionAssignorTest.assignAndVerify(scenario, assignor, zkUtils)
    }
  }
}

private object PartitionAssignorTest extends Logging {

  private val TestCaseCount = 3
  private val MaxConsumerCount = 10
  private val MaxStreamCount = 8
  private val MaxTopicCount = 100
  private val MinTopicCount = 0
  private val MaxPartitionCount = 120
  private val MinPartitionCount = 8

  private trait SubscriptionInfo {
    def registrationString: String
  }

  private case class StaticSubscriptionInfo(streamCounts: Map[String, Int]) extends SubscriptionInfo {
    def registrationString =
      Json.encode(Map("version" -> 1,
                      "subscription" -> streamCounts,
                      "pattern" -> "static",
                      "timestamp" -> 1234.toString))

    override def toString = {
      "Stream counts: " + streamCounts
    }
  }

  private case class WildcardSubscriptionInfo(streamCount: Int, regex: String, isWhitelist: Boolean)
          extends SubscriptionInfo {
    def registrationString =
      Json.encode(Map("version" -> 1,
                      "subscription" -> Map(regex -> streamCount),
                      "pattern" -> (if (isWhitelist) "white_list" else "black_list")))

    override def toString = {
      "\"%s\":%d (%s)".format(regex, streamCount, if (isWhitelist) "whitelist" else "blacklist")
    }
  }

  private case class Scenario(group: String,
                              topicPartitionCounts: Map[String, Int],
                              /* consumerId -> SubscriptionInfo */
                              subscriptions: Map[String, SubscriptionInfo]) {
    override def toString = {
      "\n" +
      "Group                  : %s\n".format(group) +
      "Topic partition counts : %s\n".format(topicPartitionCounts) +
      "Consumer assignment : %s\n".format(subscriptions)
    }
  }

  private def setupZkClientMock(scenario: Scenario) = {
    val consumers = java.util.Arrays.asList(scenario.subscriptions.keys.toSeq:_*)

    val zkClient = EasyMock.createStrictMock(classOf[ZkClient])
    val zkUtils = ZkUtils(zkClient, false)
    EasyMock.checkOrder(zkClient, false)

    EasyMock.expect(zkClient.getChildren("/consumers/%s/ids".format(scenario.group))).andReturn(consumers)
    EasyMock.expectLastCall().anyTimes()

    scenario.subscriptions.foreach { case(consumerId, subscriptionInfo) =>
      EasyMock.expect(zkClient.readData("/consumers/%s/ids/%s".format(scenario.group, consumerId), new Stat()))
              .andReturn(subscriptionInfo.registrationString)
      EasyMock.expectLastCall().anyTimes()
    }

    scenario.topicPartitionCounts.foreach { case(topic, partitionCount) =>
      val replicaAssignment = Map((0 until partitionCount).map(partition => (partition.toString, Seq(0))):_*)
      EasyMock.expect(zkClient.readData("/brokers/topics/%s".format(topic), new Stat()))
              .andReturn(zkUtils.replicaAssignmentZkData(replicaAssignment))
      EasyMock.expectLastCall().anyTimes()
    }

    EasyMock.expect(zkUtils.zkClient.getChildren("/brokers/topics")).andReturn(
      java.util.Arrays.asList(scenario.topicPartitionCounts.keys.toSeq:_*))
    EasyMock.expectLastCall().anyTimes()

    zkUtils
  }

  private def assignAndVerify(scenario: Scenario, assignor: PartitionAssignor, zkUtils: ZkUtils,
                              verifyAssignmentIsUniform: Boolean = false) {
    val assignments = scenario.subscriptions.map { case (consumer, _)  =>
      val ctx = new AssignmentContext("g1", consumer, excludeInternalTopics = true, zkUtils)
      assignor.assign(ctx).get(consumer)
    }

    // check for uniqueness (i.e., any partition should be assigned to exactly one consumer stream)
    val globalAssignment = collection.mutable.Map[TopicAndPartition, ConsumerThreadId]()
    assignments.foreach(assignment => {
      assignment.foreach { case(topicPartition, owner) =>
        val previousOwnerOpt = globalAssignment.put(topicPartition, owner)
        assertTrue("Scenario %s: %s is assigned to two owners.".format(scenario, topicPartition), previousOwnerOpt.isEmpty)
      }
    })

    // check for coverage (i.e., all given partitions are owned)
    val assignedPartitions = globalAssignment.keySet
    val givenPartitions = scenario.topicPartitionCounts.flatMap{ case (topic, partitionCount) =>
      (0 until partitionCount).map(partition => TopicAndPartition(topic, partition))
    }.toSet
    assertTrue("Scenario %s: the list of given partitions and assigned partitions are different.".format(scenario),
      givenPartitions == assignedPartitions)

    // check for uniform assignment
    if (verifyAssignmentIsUniform) {
      val partitionCountForStream = partitionCountPerStream(globalAssignment)
      if (partitionCountForStream.nonEmpty) {
        val maxCount = partitionCountForStream.valuesIterator.max
        val minCount = partitionCountForStream.valuesIterator.min
        assertTrue("Scenario %s: assignment is not uniform (partition counts per stream are in the range [%d, %d])"
          .format(scenario, minCount, maxCount), (maxCount - minCount) <= 1)
      }
    }
  }

  /** For each consumer stream, count the number of partitions that it owns. */
  private def partitionCountPerStream(assignment: collection.Map[TopicAndPartition, ConsumerThreadId]) = {
    val ownedCounts = collection.mutable.Map[ConsumerThreadId, Int]()
    assignment.foreach { case (_, owner) =>
      val updatedCount = ownedCounts.getOrElse(owner, 0) + 1
      ownedCounts.put(owner, updatedCount)
    }
    ownedCounts
  }
}

