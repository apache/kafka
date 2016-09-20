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
package kafka.admin

import kafka.utils.{Logging, TestUtils}
import kafka.zk.ZooKeeperTestHarness
import org.junit.Test
import org.junit.Assert.assertEquals

class ReassignPartitionsIntegrationTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testRackAwareReassign() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2", 2 -> "rack2", 3 -> "rack1", 4 -> "rack3", 5 -> "rack3")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkClient)

    val numPartitions = 18
    val replicationFactor = 3

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--disable-rack-aware",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkClient, createOpts)

    val topicJson = """{"topics": [{"topic": "foo"}], "version":1}"""
    val (proposedAssignment, currentAssignment, _) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = false)

    val assignment = proposedAssignment map { case (topicPartition, replicas) =>
      (topicPartition.partition, replicas)
    }
    checkReplicaDistribution(assignment, rackInfo, rackInfo.size, numPartitions, replicationFactor)
  }

  @Test
  def testRackAwareReassignBalanceReportWithUnbalancedConfig() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack1", 3 -> "rack2")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkClient)

    val numPartitions = 10
    val replicationFactor = 3

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--disable-rack-aware",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkClient, createOpts)

    val topicJson = """{"topics": [{"topic": "foo"}], "version":1}"""
    val (_, _, balanceReport1) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = false)

    // since there are three brokers in one rack and one in the other rack, the rack with one broker can get a maximum of 10
    // replicas because otherwise two replicas of the same partition will be on the same broker. This means the three other
    // brokers will get 7, 7, 6 replicas each in the most balanced case. But that is not the best balance overall, since one
    // broker has quite a few more replicas (10 compared to 7 and 6). Therefore, there will be a broker balance report.
    assertEquals(true, balanceReport1.nonEmpty)

    val (_, _, balanceReport2) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = true)

    // if rack-aware is disabled there will be no broker balance report.
    assertEquals(true, balanceReport2.isEmpty)
  }

  @Test
  def testRackAwareReassignBalanceReportWithBalancedConfig() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack1", 2 -> "rack2", 3 -> "rack2")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkClient)

    val numPartitions = 15
    val replicationFactor = 3

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--disable-rack-aware",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkClient, createOpts)

    val topicJson = """{"topics": [{"topic": "foo"}], "version":1}"""
    val (_, _, balanceReport1) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = false)

    assertEquals(true, balanceReport1.isEmpty)

    val (_, _, balanceReport2) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = true)

    // if rack-aware is disabled there will be no broker balance report.
    assertEquals(true, balanceReport2.isEmpty)
  }

  @Test
  def testRackAwareReassignBalanceReportWithMinimalBalancedConfig() {
    val rackInfo = Map(0 -> "rack1", 1 -> "rack2")
    TestUtils.createBrokersInZk(toBrokerMetadata(rackInfo), zkClient)

    val numPartitions = 1
    val replicationFactor = 1

    // create a non rack aware assignment topic first
    val createOpts = new kafka.admin.TopicCommand.TopicCommandOptions(Array(
      "--partitions", numPartitions.toString,
      "--replication-factor", replicationFactor.toString,
      "--disable-rack-aware",
      "--topic", "foo"))
    kafka.admin.TopicCommand.createTopic(zkClient, createOpts)

    val topicJson = """{"topics": [{"topic": "foo"}], "version":1}"""
    val (_, _, balanceReport1) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = false)

    assertEquals(true, balanceReport1.isEmpty)

    val (_, _, balanceReport2) = ReassignPartitionsCommand.generateAssignment(zkClient,
      rackInfo.keys.toSeq.sorted, topicJson, disableRackAware = true)

    // if rack-aware is disabled there will be no broker balance report.
    assertEquals(true, balanceReport2.isEmpty)
  }
}
