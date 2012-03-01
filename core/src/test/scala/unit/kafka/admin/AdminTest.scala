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

import junit.framework.Assert._
import org.junit.Test
import org.scalatest.junit.JUnit3Suite
import kafka.zk.ZooKeeperTestHarness
import kafka.utils.TestUtils

class AdminTest extends JUnit3Suite with ZooKeeperTestHarness {

  @Test
  def testReplicaAssignment() {
    val brokerList = List("0", "1", "2", "3", "4")

    // test 0 replication factor
    try {
      AdminUtils.assignReplicasToBrokers(brokerList, 10, 0)
      fail("shouldn't allow replication factor 0")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // test wrong replication factor
    try {
      AdminUtils.assignReplicasToBrokers(brokerList, 10, 6)
      fail("shouldn't allow replication factor larger than # of brokers")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // correct assignment
    {
      val expectedAssignment = Array(
        List("0", "1", "2"),
        List("1", "2", "3"),
        List("2", "3", "4"),
        List("3", "4", "0"),
        List("4", "0", "1"),
        List("0", "2", "3"),
        List("1", "3", "4"),
        List("2", "4", "0"),
        List("3", "0", "1"),
        List("4", "1", "2")
        )

      val actualAssignment = AdminUtils.assignReplicasToBrokers(brokerList, 10, 3, 0)
      val e = (expectedAssignment.toList == actualAssignment.toList)
      assertTrue(expectedAssignment.toList == actualAssignment.toList)
    }
  }

  @Test
  def testManualReplicaAssignment() {
    val brokerList = Set("0", "1", "2", "3", "4")

    // duplicated brokers
    try {
      val replicationAssignmentStr = "0,0,1:1,2,3"
      CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      fail("replication assginment shouldn't have duplicated brokers")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // non-exist brokers
    try {
      val replicationAssignmentStr = "0,1,2:1,2,7"
      CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      fail("replication assginment shouldn't contain non-exist brokers")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // inconsistent replication factor
    try {
      val replicationAssignmentStr = "0,1,2:1,2"
      CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      fail("all partitions should have the same replication factor")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }

    // good assignment
    {
      val replicationAssignmentStr = "0:1:2,1:2:3"
      val expectedReplicationAssignment = Array(
        List("0", "1", "2"),
        List("1", "2", "3")
      )
      val actualReplicationAssignment = CreateTopicCommand.getManualReplicaAssignment(replicationAssignmentStr, brokerList)
      assertTrue(expectedReplicationAssignment.toList == actualReplicationAssignment.toList)
    }
  }

  @Test
  def testTopicCreationInZK() {
    val expectedReplicaAssignment = Array(
      List("0", "1", "2"),
      List("1", "2", "3"),
      List("2", "3", "4"),
      List("3", "4", "0"),
      List("4", "0", "1"),
      List("0", "2", "3"),
      List("1", "3", "4"),
      List("2", "4", "0"),
      List("3", "0", "1"),
      List("4", "1", "2"),
      List("1", "2", "3"),
      List("1", "3", "4")      
      )
    TestUtils.createBrokersInZk(zookeeper.client, List(0, 1, 2, 3, 4))

    val topic = "test"
    // create the topic
    AdminUtils.createReplicaAssignmentPathInZK(topic, expectedReplicaAssignment, zookeeper.client)
    val actualReplicaAssignment = AdminUtils.getTopicMetaDataFromZK(List(topic), zookeeper.client).head
                                  .get.partitionsMetadata.map(p => p.replicas)
    val actualReplicaList = actualReplicaAssignment.map(r => r.map(b => b.id.toString).toList).toList
    expectedReplicaAssignment.toList.zip(actualReplicaList).foreach(l => assertEquals(l._1, l._2))

    try {
      AdminUtils.createReplicaAssignmentPathInZK(topic, expectedReplicaAssignment, zookeeper.client)
      fail("shouldn't be able to create a topic already exists")
    }
    catch {
      case e: AdministrationException => // this is good
      case e2 => throw e2
    }
  }

  @Test
  def testGetTopicMetadata() {
    val expectedReplicaAssignment = Array(
      List("0", "1", "2"),
      List("1", "2", "3")
    )
    val topic = "auto-topic"
    TestUtils.createBrokersInZk(zookeeper.client, List(0, 1, 2, 3))
    AdminUtils.createReplicaAssignmentPathInZK(topic, expectedReplicaAssignment, zookeeper.client)

    val newTopicMetadata = AdminUtils.getTopicMetaDataFromZK(List(topic), zookeeper.client).head
    newTopicMetadata match {
      case Some(metadata) => assertEquals(topic, metadata.topic)
        assertNotNull("partition metadata list cannot be null", metadata.partitionsMetadata)
        assertEquals("partition metadata list length should be 2", 2, metadata.partitionsMetadata.size)
        assertEquals("leader of partition 0 should be 0", 0, metadata.partitionsMetadata.head.leader.get.id)
        assertEquals("leader of partition 1 should be 1", 1, metadata.partitionsMetadata.last.leader.get.id)
        val actualReplicaAssignment = metadata.partitionsMetadata.map(p => p.replicas)
        val actualReplicaList = actualReplicaAssignment.map(r => r.map(b => b.id.toString).toList).toList
        assertEquals(expectedReplicaAssignment.toList, actualReplicaList)
      case None => fail("Topic " + topic + " should've been automatically created")
    }
  }
}
