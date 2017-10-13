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

import scala.collection.JavaConversions._
import org.junit.Assert._
import org.junit.Test
import kafka.admin.BrokerCommand.{BrokerCommandOptions, NodeWithTopicPartitions}
import org.apache.kafka.clients.admin.TopicDescription
import kafka.cluster.Broker
import kafka.utils.Logging

import scala.collection.Map
//import kafka.utils.ZkUtils._
//import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{Node, PartitionInfo, TopicPartitionInfo}
//import org.apache.kafka.common.requests.MetadataResponse.{TopicMetadata, PartitionMetadata}

import scala.collection.JavaConverters._

import scala.collection.Seq

//class BrokerCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {
class BrokerCommandTest extends Logging with RackAwareTest {

  @Test
  def testApplyBrokerFilter() = {
    val allNodes = getTestNodes()

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--broker", "99"))
    assertEquals(cmdOptions0.brokers.size, 1)
    val filteredBrokers0 = BrokerCommand.filterNodes(allNodes, cmdOptions0.brokers, List[String](), List[String]())
    assertEquals(filteredBrokers0.size, 0)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--broker", "1"))
    assertEquals(cmdOptions1.brokers.size, 1)
    val filteredBrokers1 = BrokerCommand.filterNodes(allNodes, cmdOptions1.brokers, List[String](), List[String]())
    assertEquals(filteredBrokers1.size, 1)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--broker", "1", "--broker", "2"))
    assertEquals(cmdOptions2.brokers.size, 2)
    val filteredBrokers2 = BrokerCommand.filterNodes(allNodes, cmdOptions2.brokers, List[String](), List[String]())
    assertEquals(filteredBrokers2.size, 2)

    val cmdOptions3 = new BrokerCommandOptions(Array[String]("--broker", "1", "--broker", "2", "--broker", "99"))
    assertEquals(cmdOptions3.brokers.size, 3)
    val filteredBrokers3 = BrokerCommand.filterNodes(allNodes, cmdOptions3.brokers, List[String](), List[String]())
    assertEquals(filteredBrokers3.size, 2)
  }

  @Test
  def testApplyHostFilter() = {
    val allNodes = getTestNodes()

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--host", "unknown"))
    assertEquals(cmdOptions0.hosts.size, 1)
    val filteredHosts0 = BrokerCommand.filterNodes(allNodes, List[Int](), cmdOptions0.hosts, List[String]())
    assertEquals(filteredHosts0.size, 0)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--host", "host1"))
    assertEquals(cmdOptions1.hosts.size, 1)
    val filteredHosts1 = BrokerCommand.filterNodes(allNodes, List[Int](), cmdOptions1.hosts, List[String]())
    assertEquals(filteredHosts1.size, 1)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--host", "host1", "--host", "host2"))
    assertEquals(cmdOptions2.hosts.size, 2)
    val filteredHosts2 = BrokerCommand.filterNodes(allNodes, List[Int](), cmdOptions2.hosts, List[String]())
    assertEquals(filteredHosts2.size, 2)

    val cmdOptions3 = new BrokerCommandOptions(Array[String]("--host", "host1", "--host", "host2", "--host", "unknown"))
    assertEquals(cmdOptions3.hosts.size, 3)
    val filteredHosts3 = BrokerCommand.filterNodes(allNodes, List[Int](), cmdOptions3.hosts, List[String]())
    assertEquals(filteredHosts3.size, 2)
  }

  @Test
  def testApplyRackFilter() = {
    val allNodes = getTestNodes()

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--rack", "unknown"))
    assertEquals(cmdOptions0.racks.size, 1)
    val filteredRacks0 = BrokerCommand.filterNodes(allNodes, List[Int](), List[String](), cmdOptions0.racks)
    assertEquals(filteredRacks0.size, 0)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--rack", "rack1"))
    assertEquals(cmdOptions1.racks.size, 1)
    val filteredRacks1 = BrokerCommand.filterNodes(allNodes, List[Int](), List[String](), cmdOptions1.racks)
    assertEquals(filteredRacks1.size, 1)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--rack", "rack1", "--rack", "rack2"))
    assertEquals(cmdOptions2.racks.size, 2)
    val filteredRacks2 = BrokerCommand.filterNodes(allNodes, List[Int](), List[String](), cmdOptions2.racks)
    assertEquals(filteredRacks2.size, 2)

    val cmdOptions3 = new BrokerCommandOptions(Array[String]("--rack", "rack1", "--rack", "rack2", "--rack", "unknown"))
    assertEquals(cmdOptions3.racks.size, 3)
    val filteredRacks3 = BrokerCommand.filterNodes(allNodes, List[Int](), List[String](), cmdOptions3.racks)
    assertEquals(filteredRacks3.size, 2)
  }

  @Test
  def testApplyTopicFilter() = {
    val allTopics = getTopicDescriptions()

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--topic", "unknown"))
    assertEquals(cmdOptions0.topics.size, 1)
    val filteredTopics0 = BrokerCommand.filterTopicDescriptions(allTopics, cmdOptions0.topics)
    assertEquals(filteredTopics0.size, 0)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--topic", "topicA"))
    assertEquals(cmdOptions1.topics.size, 1)
    val filteredTopics1 = BrokerCommand.filterTopicDescriptions(allTopics, cmdOptions1.topics)
    assertEquals(filteredTopics1.size, 1)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--topic", "topicA", "--topic", "topicB"))
    assertEquals(cmdOptions2.topics.size, 2)
    val filteredTopics2 = BrokerCommand.filterTopicDescriptions(allTopics, cmdOptions2.topics)
    assertEquals(filteredTopics2.size, 2)

    val cmdOptions3 = new BrokerCommandOptions(Array[String]("--topic", "topicA", "--topic", "topicB", "--topic", "unknown"))
    assertEquals(cmdOptions3.topics.size, 3)
    val filteredTopics3 = BrokerCommand.filterTopicDescriptions(allTopics, cmdOptions3.topics)
    assertEquals(filteredTopics3.size, 2)
  }

  @Test
  def testmergeNodeTopicDescriptions() = {
    val allNodes = getTestNodes()
    val topicDescriptions = getTopicDescriptions()
    val mergedInfo: Map[Int, NodeWithTopicPartitions] = BrokerCommand.mergeNodeTopicDescriptions(allNodes, topicDescriptions)
    assertEquals(mergedInfo.keys.size, 3)
    val leaders = mergedInfo.values.map(m => m.topicPartitions.values)
    assertEquals(leaders.size, 3)
    val broker3Info = mergedInfo(3)
    assertEquals(broker3Info.node.id, 3)
    assertEquals(broker3Info.topicPartitions.keySet, Set[String]("topicA", "topicB", "topicC"))
    val topicAData = broker3Info.topicPartitions("topicA")
    // Checking if broker3/node3 topicA has 3 partitions (viz. 0, 1, 2) and that partition #1 as being the leader)
    assert(topicAData.mkString(" ").equals("0 +1 2"))
  }


  def getTestNodes(): List[Node] = {
    val node1 = new Node(1, "host1", 9092, "rack1")
    val node2 = new Node(2, "host2", 9092, "rack2")
    val node3 = new Node(3, "host3", 9092, "rack3")
    List[Node](node1, node2, node3)
  }

  def getTopicDescriptions(): Seq[TopicDescription] = {
    val allNodes = getTestNodes()
    val nodes123: List[Node] = List[Node](allNodes(0), allNodes(1), allNodes(2))

    val nodes321: List[Node] = List[Node](allNodes(2), allNodes(1), allNodes(0))

    val nodes231: List[Node] = List[Node](allNodes(1), allNodes(2), allNodes(0))

    val partition0: TopicPartitionInfo = new TopicPartitionInfo(0, nodes123(0), nodes123, nodes123)
    val partition1: TopicPartitionInfo = new TopicPartitionInfo(1, nodes321(0), nodes321, nodes321)
    val partition2: TopicPartitionInfo = new TopicPartitionInfo(2, nodes231(0), nodes231, nodes231)

    // Each topic consists of 3 partitions as defined above
    val topicDescritionA: TopicDescription = new TopicDescription("topicA", false,
      List[TopicPartitionInfo](partition0, partition1, partition2))

    val topicDescritionB: TopicDescription = new TopicDescription("topicB", false,
      List[TopicPartitionInfo](partition0, partition1, partition2))

    val topicDescritionC: TopicDescription = new TopicDescription("topicC", false,
      List[TopicPartitionInfo](partition0, partition1, partition2))

    Seq[TopicDescription](topicDescritionA, topicDescritionB, topicDescritionC)
  }

}
