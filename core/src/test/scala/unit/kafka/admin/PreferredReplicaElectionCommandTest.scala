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
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.TestUtils._
import kafka.utils.{Logging, TestUtils}
import kafka.zk.{ReassignPartitionsZNode, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin.{AdminClientConfig, AdminClient => JAdminClient}
import org.apache.kafka.common.TopicPartition
import org.junit.Assert.assertEquals
import org.junit.{After, Test}

import scala.collection.{Map, Seq, Set}

class PreferredReplicaElectionCommandTest extends ZooKeeperTestHarness with Logging {
  var servers: Seq[KafkaServer] = Seq()
  var adminClient: JAdminClient = null

  @After
  override def tearDown() {
    if (adminClient != null) {
      adminClient.close()
      adminClient = null
    }
    TestUtils.shutdownServers(servers)
    super.tearDown()
  }

  @Test
  def testPreferredReplicaJsonData() {
    // write preferred replica json data to zk path
    val partitionsForPreferredReplicaElection = Set(new TopicPartition("test", 1), new TopicPartition("test2", 1))
    PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, partitionsForPreferredReplicaElection)
    // try to read it back and compare with what was written
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    assertEquals("Preferred replica election ser-de failed", partitionsForPreferredReplicaElection,
      partitionsUndergoingPreferredReplicaElection)
  }

  def createAdminClient(servers: Seq[KafkaServer]): JAdminClient = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBrokerListStrFromServers(servers))
    props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    JAdminClient.create(props)
  }

  def waitForReassignmentToComplete(pause: Long = 100L) {
    waitUntilTrue(() => !zkClient.reassignPartitionsInProgress,
      s"Znode ${ReassignPartitionsZNode.path} wasn't deleted", pause = pause)
  }

  @Test
  def testBasicPreferredReplicaElection() {
    val expectedReplicaAssignment = Map(0  -> List(0, 1, 2))
    val topic = "test"
    val partition = 0
    val preferredReplica = 2
    // create brokers
    val brokerRack = Map(0 -> "rack0", 1 -> "rack1", 2 -> "rack2")
    val serverConfigs = TestUtils.createBrokerConfigs(3, zkConnect, false, rackInfo = brokerRack).map(KafkaConfig.fromProps)
    servers = serverConfigs.reverseMap(s => TestUtils.createServer(s))

    // create the topic
    adminZkClient.createOrUpdateTopicPartitionAssignmentPathInZK(topic, expectedReplicaAssignment)

    // Reassign the partition
    adminClient = createAdminClient(servers)
    val topicJson: String = s"""{"version":1,"partitions":[{"topic":"$topic","partition":$partition,"replicas":[2, 1, 0]}]}"""
    ReassignPartitionsCommand.executeAssignment(zkClient, Some(adminClient), topicJson, Throttle(-1, -1))
    waitForReassignmentToComplete()

    val currentLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, oldLeaderOpt = None)
    // trigger preferred replica election
    val preferredReplicaElection = new PreferredReplicaLeaderElectionCommand(zkClient, Set(new TopicPartition(topic, partition)))
    preferredReplicaElection.moveLeaderToPreferredReplica()
    val newLeader = TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, partition, oldLeaderOpt = Some(currentLeader))
    assertEquals("Preferred replica election failed", preferredReplica, newLeader)
  }
}
