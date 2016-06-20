package unit.kafka.admin

import scala.collection.JavaConversions._

import org.junit.Assert._
import org.junit.Test

import kafka.admin.{BrokerCommand, RackAwareTest}
import kafka.admin.BrokerCommand.BrokerCommandOptions
import kafka.cluster.Broker
import kafka.utils.Logging
//import kafka.utils.ZkUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.Node
import org.apache.kafka.common.requests.MetadataResponse.{TopicMetadata, PartitionMetadata}

import scala.collection.JavaConverters._

import scala.collection.Seq

class BrokerCommandTest extends ZooKeeperTestHarness with Logging with RackAwareTest {

  @Test
  def testApplyBrokerFilter() = {
    val allBrokers = getTestBrokers()
    assertEquals(3, allBrokers.size)

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--broker", "99"))
    val filteredBrokers0 = BrokerCommand.applyBrokerFilter(allBrokers, cmdOptions0)
    assertEquals(0, filteredBrokers0.size)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--broker", "1"))
    val filteredBrokers1 = BrokerCommand.applyBrokerFilter(allBrokers, cmdOptions1)
    assertEquals(1, filteredBrokers1.size)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--broker", "1", "--broker", "2"))
    val filteredBrokers2 = BrokerCommand.applyBrokerFilter(allBrokers, cmdOptions2)
    assertEquals(2, filteredBrokers2.size)

    val cmdOptions3 = new BrokerCommandOptions(Array[String]("--broker", "1", "--broker", "2", "--broker", "99"))
    val filteredBrokers3 = BrokerCommand.applyBrokerFilter(allBrokers, cmdOptions3)
    assertEquals(2, filteredBrokers3.size)
  }

  @Test
  def testApplyHostFilter() = {
    val allBrokers = getTestBrokers()
    assertEquals(3, allBrokers.size)

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--host", "unknown"))
    val filteredBrokers0 = BrokerCommand.applyHostFilter(allBrokers, cmdOptions0)
    assertEquals(0, filteredBrokers0.size)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--host", "host1"))
    val filteredBrokers1 = BrokerCommand.applyHostFilter(allBrokers, cmdOptions1)
    assertEquals(1, filteredBrokers1.size)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--host", "host1", "--host", "host2"))
    val filteredBrokers2 = BrokerCommand.applyHostFilter(allBrokers, cmdOptions2)
    assertEquals(2, filteredBrokers2.size)
  }

  @Test
  def testApplyRackFilter() = {
    val allBrokers = getTestBrokers()
    assertEquals(3, allBrokers.size)

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--rack", "unknown"))
    val filteredBrokers0 = BrokerCommand.applyRackFilter(allBrokers, cmdOptions0)
    assertEquals(0, filteredBrokers0.size)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--rack", "rack1"))
    val filteredBrokers1 = BrokerCommand.applyRackFilter(allBrokers, cmdOptions1)
    assertEquals(1, filteredBrokers1.size)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--rack", "rack1", "--rack", "rack2"))
    val filteredBrokers2 = BrokerCommand.applyRackFilter(allBrokers, cmdOptions2)
    assertEquals(2, filteredBrokers2.size)
  }

  @Test
  def testApplyTopicFilter() = {
    val topicMetadata = getTestTopicMetadata()
    assertEquals(2, topicMetadata.size)

    val cmdOptions0 = new BrokerCommandOptions(Array[String]("--topic", "unknown"))
    val filteredTopicMetadata = BrokerCommand.applyTopicFilter(topicMetadata, cmdOptions0)
    assertEquals(0, filteredTopicMetadata.size)

    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--topic", "topic1"))
    val filteredTopicMetadata1 = BrokerCommand.applyTopicFilter(topicMetadata, cmdOptions1)
    assertEquals(1, filteredTopicMetadata1.size)

    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--topic", "topic1", "--topic", "topic2"))
    val filteredTopicMetadata2 = BrokerCommand.applyTopicFilter(topicMetadata, cmdOptions2)
    assertEquals(2, filteredTopicMetadata2.size)
  }

  @Test
  def testMergeBrokerTopicMetadata() = {
    val allBrokers = getTestBrokers()
    val allTopicMetadata = getTestTopicMetadata()
    allTopicMetadata.foreach(tm => tm.topic())
    val merged = BrokerCommand.mergeBrokerTopicMetadata(allBrokers, allTopicMetadata)
    assertEquals(3, merged.size)
    // Check that the keys of merged data is the set of broker-ids
    val inputBrokerIds = allBrokers.map(b => b.id)
    val mergedBrokerIds = merged.keys.toSet
    assertEquals(inputBrokerIds, mergedBrokerIds)
    val inputTopics = allTopicMetadata.map(topicMeta => topicMeta.topic())
    val mergedTopics = merged.values.map(m => m.topicPartitions.keys).flatten.toSet
    // Check that the topics before and after merge is identical
    assertEquals(inputTopics, mergedTopics)
    val sampleTopicMetada = merged(1)
    val leaderPartitions = sampleTopicMetada.topicPartitions("topic1").toList.collect{case p:BrokerCommand.LeaderPartition => p}
    assertEquals(2, leaderPartitions.size)
  }

  @Test
  def testPrintInfo() = {
    val LINE_SEPARATOR = String.format("%n")
    val allBrokers = getTestBrokers()
    val allTopicMetadata = getTestTopicMetadata()
    allTopicMetadata.foreach(tm => tm.topic())
    val merged = BrokerCommand.mergeBrokerTopicMetadata(allBrokers, allTopicMetadata)
    val cmdOptions0 = new BrokerCommandOptions(Array[String](""))
    val output0 = BrokerCommand.printInfo(merged, cmdOptions0)
    assertEquals(450 + merged.keys.size*(LINE_SEPARATOR.length) ,output0.length)
    val cmdOptions1 = new BrokerCommandOptions(Array[String]("--topic-details"))
    val output1 = BrokerCommand.printInfo(merged, cmdOptions1)
    assertEquals(648 + merged.keys.size*(LINE_SEPARATOR.length) ,output1.length)
    val cmdOptions2 = new BrokerCommandOptions(Array[String]("--partition-details"))
    val output2 = BrokerCommand.printInfo(merged, cmdOptions2)
    assertEquals(654 + merged.keys.size*(LINE_SEPARATOR.length) ,output2.length)
    val cmdOptions3 = new BrokerCommandOptions(Array[String]("--details"))
    val output3 = BrokerCommand.printInfo(merged, cmdOptions3)
    assertEquals(852 + merged.keys.size*(LINE_SEPARATOR.length) ,output3.length)
  }

  def getTestBrokers(): Set[Broker] = {
    val broker1 =
      """{
        "version": 3,
        "host": "host1",
        "port": 1234,
        "jmx_port": 4321,
        "timestamp": "123456789",
        "endpoints": ["PLAINTEXT://host1:9092", "SSL://host1:9093"],
        "rack": "rack1"
        }""".stripMargin

    val broker2 =
      """{
        "version": 3,
        "host": "host2",
        "port": 1234,
        "jmx_port": 4321,
        "timestamp": "123456789",
        "endpoints": ["PLAINTEXT://host2:9092", "SSL://host2:9093"],
        "rack": "rack2"
        }""".stripMargin

    val broker3 =
      """{
        "version": 3,
        "host": "host3",
        "port": 1234,
        "jmx_port": 4321,
        "timestamp": "123456789",
        "endpoints": ["PLAINTEXT://host3:9092", "SSL://host3:9093"],
        "rack": "rack3"
        }""".stripMargin
    val b1 = Broker.createBroker(1, broker1)
    val b2 = Broker.createBroker(2, broker2)
    val b3 = Broker.createBroker(3, broker3)
    Set[Broker](b1, b2, b3)
  }

  def getTestNodes(): Seq[Node] = {
    val node1 = new Node(1, "host1", 9092, "rack1")
    val node2 = new Node(2, "host2", 9092, "rack2")
    val node3 = new Node(3, "host3", 9092, "rack3")
    Seq[Node](node1, node2, node3)
  }

//  def getTestPartitionMetadata(): Seq[PartitionMetadata] = {
  def getTestPartitionMetadata():Seq[PartitionMetadata] = {
  val nodes = getTestNodes()
  val partition0 = new PartitionMetadata(Errors.NONE,
      0, nodes(0),
      List(nodes(0), nodes(1), nodes(2)),
      List(nodes(0), nodes(1),  nodes(2)))
  val partition1 = new PartitionMetadata(Errors.NONE,
      1,
      nodes(1),
    List(nodes(0), nodes(1), nodes(2)),
    List(nodes(0), nodes(1),  nodes(2)))
  val partition2 = new PartitionMetadata(Errors.NONE,
      2,
      nodes(2),
    List(nodes(0), nodes(1), nodes(2)),
    List(nodes(0), nodes(1),  nodes(2)))
  val partition3 = new PartitionMetadata(Errors.NONE,
      3,
      nodes(0),
    List(nodes(0), nodes(1), nodes(2)),
    List(nodes(0), nodes(1),  nodes(2)))
  val partition4 = new PartitionMetadata(Errors.NONE,
      4,
      nodes(1),
    List(nodes(0), nodes(1), nodes(2)),
    List(nodes(0), nodes(1),  nodes(2)))
  val partition5 = new PartitionMetadata(Errors.NONE,
      5,
      nodes(2),
    List(nodes(0), nodes(1), nodes(2)),
    List(nodes(0), nodes(1),  nodes(2)))
  Seq[PartitionMetadata](partition0, partition1, partition2, partition3, partition4, partition5)
  }

  def getTestTopicMetadata(): Set[TopicMetadata] = {
    val partitions = getTestPartitionMetadata()
    val topic1 = "topic1"
    val topic2 = "topic2"
    val topic1meta = new TopicMetadata(Errors.NONE, topic1, false, partitions.asJava)
    val topic2meta = new TopicMetadata(Errors.NONE, topic2, false, partitions.asJava)
    Set[TopicMetadata](topic1meta, topic2meta)
 }

}