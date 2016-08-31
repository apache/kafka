/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package unit.kafka.admin

import java.util.concurrent.Executors

import kafka.admin.{AdminUtils, ReassignPartitionsCommand}
import kafka.log.LogConfig
import kafka.server.{ConfigType, KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.ZkUtils._
import kafka.utils.{TestUtils, CoreUtils, Logging, ZkUtils}
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Before, Test}
import org.junit.Assert.assertEquals
import unit.kafka.admin.ReplicationQuotaUtils._
import scala.collection.Seq


class ReassignPartitionsClusterTest extends ZooKeeperTestHarness with Logging {
  val partitionId = 0
  var servers: Seq[KafkaServer] = null
  val topicName = "my-topic"

  @Before
  override def setUp() {
    super.setUp()
  }

  def startBrokers(brokerIds: Seq[Int]) {
    servers = brokerIds.map(i => createBrokerConfig(i, zkConnect))
      .map(c => createServer(KafkaConfig.fromProps(c)))
  }

  @After
  override def tearDown() {
    servers.foreach(_.shutdown())
    servers.foreach(server => CoreUtils.delete(server.config.logDirs))
    super.tearDown()
  }

  @Test
  def shouldMoveSinglePartition {
    //Given a single replica on server 100
    startBrokers(Seq(100, 101))
    val partition = 0
    createTopic(zkUtils, topicName, Map(partition -> Seq(100)), servers = servers)

    //When we move the replica on 100 to broker 101
    ReassignPartitionsCommand.executeAssignment(zkUtils, s"""{"version":1,"partitions":[{"topic":"$topicName","partition":0,"replicas":[101]}]}""",awaitCompletion = true)
    waitForReasignmentToComplete()

    //Then the replica should be on 101
    assertEquals(zkUtils.getPartitionAssignmentForTopics(Seq(topicName)).get(topicName).get(partition), Seq(101))
  }

  @Test
  def shouldExpandCluster() {
    //Given partitions on 2 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101),
      1 -> Seq(100, 101),
      2 -> Seq(100, 101)
    ), servers = servers)

    //When rebalancing
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, brokers, json(topicName), true)._1
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment))
    waitForReasignmentToComplete()

    //Then the replicas should span all three brokers
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(actual.values.flatten.toSeq.distinct.sorted, Seq(100, 101, 102))
  }

  @Test
  def shouldShrinkCluster() {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101),
      1 -> Seq(101, 102),
      2 -> Seq(102, 100)
    ), servers = servers)

    //When rebalancing
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, Array(100, 101), json(topicName), true)._1
    ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment))
    waitForReasignmentToComplete()

    //Then replicas should only span the first two brokers
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(actual.values.flatten.toSeq.distinct.sorted, Seq(100, 101))
  }

  @Test
  def shouldChangeThrottleOnRerun() {
    //Given partitions on 3 of 3 brokers
    val brokers = Array(100, 101, 102)
    startBrokers(brokers)
    createTopic(zkUtils, topicName, Map(
      0 -> Seq(100, 101)
    ), servers = servers)

    //Given throttle set so replication will take at least 20 sec (we won't wait this long)
    val initialThrottle: Long = 1000 * 1000
    addMessages(num = 200, size = 100 * 1000, topicName, servers)

    //Start rebalance (use a separate thread as it'll be slow)
    val newAssignment = ReassignPartitionsCommand.generateAssignment(zkUtils, Array(101, 102), json(topicName), true)._1

    inThread() {
      ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), initialThrottle)
      println("Runnable 1 completed")
    }

    //Check throttle config
    checkThrottleConfigAddedToZK(initialThrottle, servers, topicName)

    //Now re-reun the same assignment with a larger throttle (again use a thread so we can check ZK whilst it runs)
    val newThrottle = initialThrottle * 1000

    inThread() {
      ReassignPartitionsCommand.executeAssignment(zkUtils, ZkUtils.formatAsReassignmentJson(newAssignment), newThrottle)
    }

    //Check throttle was changed
    checkThrottleConfigAddedToZK(newThrottle, servers, topicName)

    //Await completion
    waitForReasignmentToComplete()

    //Check removed
    checkThrottleConfigRemovedFromZK(topicName, servers)

    //Check move occurred
    val actual = zkUtils.getPartitionAssignmentForTopics(Seq(topicName))(topicName)
    assertEquals(actual.values.flatten.toSeq.distinct.sorted, Seq(101, 102))
  }

  def waitForReasignmentToComplete() {
    waitUntilTrue(() => !zkUtils.pathExists(ReassignPartitionsPath), s"Znode ${ZkUtils.ReassignPartitionsPath} wasn't deleted")
  }

  def json(topic: String): String = {
    s"""{"topics": [{"topic": "$topic"}],"version":1}"""
  }

  def inThread()(fun: => Unit) = {
    Utils.newThread("ReplicationTestHelperThread", CoreUtils.runnable(fun), false).start
  }
}
