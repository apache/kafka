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

import java.util.Properties

import scala.collection._
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import joptsimple._
import org.apache.kafka.common.{Node, TopicPartitionInfo}
import org.apache.kafka.clients.admin.{AdminClientConfig, AdminClient => JAdminClient}
import org.apache.kafka.clients.admin.{DescribeClusterResult, DescribeTopicsResult, TopicDescription}
import kafka.utils.{CommandLineUtils, Json}

object BrokerCommand {

  def main(args: Array[String]): Unit = {
    val ZKTIMEOUT = 30000
    val SESSIONTIMEOUT = 30000

    // Parse and validate commandline options
    val cmdOptions = new BrokerCommandOptions(args)
    cmdOptions.checkArgs()

    // topics, brokerIds,  hosts and racks to filter the cluster metadata
    val topics: List[String] = cmdOptions.topics
    val brokers: List[Int] = cmdOptions.brokers
    val hosts: List[String] = cmdOptions.hosts
    val racks: List[String] = cmdOptions.racks

    // Using the new AdminClient
    val adminClient: JAdminClient = createAdminClient(cmdOptions)
    val cluster: DescribeClusterResult = adminClient.describeCluster()
    val nodes = cluster.nodes.get.asScala.toList
    val topicList = adminClient.listTopics().names.get // All topics
    val topicDescriptions = adminClient.describeTopics(topicList).values.asScala.values.map(_.get) // Partition details for all topics

    val filteredNodes = filterNodes(nodes, brokers, hosts, racks)

    val filteredTopicDescriptions: Iterable[TopicDescription] = filterTopicDescriptions(topicDescriptions, topics)

    val mergedInfo = mergeNodeTopicDescriptions(filteredNodes, filteredTopicDescriptions)
     //    nodeDetails.foreach(println)
    val clusterId: String = cluster.clusterId.get
    val controllerNode: Node = cluster.controller.get
    val controller =  s"Controller BrokerId: ${controllerNode.id}  Hostname: ${controllerNode.host}"
    println(s"""Cluster: ${clusterId}\n${controller}\n\n${printBrokerInfo(mergedInfo, cmdOptions)}""")
  }

  def mergeNodeTopicDescriptions(nodes: List[Node], topicDescriptions: Iterable[TopicDescription]): Map[Int, NodeWithTopicPartitions]= {

    // The stmt below only initializes nodeDetails with node info, not partition details
    val nodeDetails: Map[Int, NodeWithTopicPartitions] =
    nodes.map(node => (node.id, NodeWithTopicPartitions(node))).toMap

    val brokerIds = nodeDetails.keys.toSet

    // Iterate through topic partition details and use the replicas list to populate nodeDetails
    for (td <- topicDescriptions) {
      val topicName: String = td.name
      for (partitionInfo <- td.partitions.asScala) {
        val partition: Int = partitionInfo.partition
        val leader: Int = if (partitionInfo.leader.isEmpty) -1 else partitionInfo.leader.id
        val isr = partitionInfo.isr.asScala.map(_.id).toSet
        for (brokerId: Int <- partitionInfo.replicas.asScala.map(_.id)) {
          if (brokerIds.contains(brokerId)) {
            val brokerNode = nodeDetails(brokerId)
            brokerNode.addTopic(topicName)
            brokerNode.addTopicPartitionInfo(topicName, partitionInfo)
          }
        }
      }

    }
    nodeDetails
  }

  // Admin client only needs bootstrap server (broker) info.
  private def createAdminClient(opts: BrokerCommandOptions): JAdminClient = {
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-brokers")
    JAdminClient.create(props)
  }

  // Filter cluster nodes for brokers, hosts and racks specified on commandline
  def filterNodes(nodes: List[Node], brokers: List[Int], hosts: List[String], racks: List[String]): List[Node] = {
    val nodes1 = if (brokers.isEmpty) nodes else nodes.filter(n => brokers.contains(n.id))
    val nodes2 = if (hosts.isEmpty) nodes1 else nodes.filter(n => hosts.contains(n.host))
    val nodes3 = if (racks.isEmpty) nodes2 else nodes.filter(n => racks.contains(n.rack))
    nodes3
  }

  // filter topic details for topics specified on the commandline
  def filterTopicDescriptions(topicDescription: Iterable[TopicDescription], topics: List[String]): Iterable[TopicDescription] = {
    if (topics.isEmpty) topicDescription else topicDescription.filter(td => topics.contains(td.name))
  }

  def printBrokerInfo(nodeWithTopicPartitions: Map[Int, NodeWithTopicPartitions], cmdOptions: BrokerCommandOptions): String = {
    // Sorting output by broker-id
    val KV_SEPERATOR = ": "
    val FIELD_SEPERATOR = "\t"
    val BROKERID = "BrokerId"
    val HOSTNAME = "Hostname"
    val RACK = "Rack"
    val MISSING_RACK = "N/A"
    val TOPICS = "Topics"
    val PARTITIONS = "Partitions"
    val LEADER = "Leaders"
    val INSYNC = "InSync"
    val TRAILING = "Trailing"
    val TOPIC_DETAILS = "Topic Details"
    val PARTITION_DETAILS = "Partition Details"
    val LINE_SEPARATOR = String.format("%n")

    val sortedNodeWithTopicPartition = SortedMap[Int, NodeWithTopicPartitions](nodeWithTopicPartitions.toArray: _*)

    val topicDetailsRequired: Boolean = cmdOptions.topicDetailsRequired
    val partitionDetailsRequired: Boolean = cmdOptions.partitionDetailsRequired

    var output: String = ""
    sortedNodeWithTopicPartition.values.foreach(singleNodeWithTopicPartitions => {
      val brokerId = singleNodeWithTopicPartitions.brokerId
      val node = singleNodeWithTopicPartitions.node
      val topicPartitions = singleNodeWithTopicPartitions.topicPartitions
      output += BROKERID + KV_SEPERATOR + brokerId + FIELD_SEPERATOR
      val host = node.host
      output += HOSTNAME + KV_SEPERATOR + host + FIELD_SEPERATOR
      val rack: String = if (node.hasRack) node.rack else MISSING_RACK
      output += RACK + KV_SEPERATOR + rack + FIELD_SEPERATOR
      output += TOPICS + KV_SEPERATOR + topicPartitions.keys.size.toString + FIELD_SEPERATOR
      val allPartitions = topicPartitions.values.flatten
      output += PARTITIONS + KV_SEPERATOR + allPartitions.size.toString + FIELD_SEPERATOR
      output += LEADER + KV_SEPERATOR + allPartitions.collect { case p: LeaderPartition => p }.size.toString + FIELD_SEPERATOR
      output += INSYNC + KV_SEPERATOR + allPartitions.collect { case p: InSyncPartition => p }.size.toString + FIELD_SEPERATOR
      output += TRAILING + KV_SEPERATOR + allPartitions.collect { case p: TrailingPartition => p }.size.toString + FIELD_SEPERATOR
      if (topicDetailsRequired) {
        output += TOPIC_DETAILS + KV_SEPERATOR + topicPartitions.map(tp => (tp._1, s"${tp._1} with ${tp._2.length} partitions")).values.mkString(", ") + FIELD_SEPERATOR
      }
      if (partitionDetailsRequired) {
        output += PARTITION_DETAILS + KV_SEPERATOR + topicPartitions.map(tp => (tp._1, "(" + tp._1 + ": " + tp._2.mkString(",") + ")")).values.mkString(", ") + FIELD_SEPERATOR
      }
      output += LINE_SEPARATOR
    })
    output.toString
  }

  class BrokerCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: the server(s) to use for bootstrapping")
      .withRequiredArg
      .describedAs("The server(s) to use for bootstrapping")
      .ofType(classOf[String])
    val brokerOpt = parser.accepts("broker", "Filter for a broker. Option can be used multiple times for multiple broker-ids")
      .withRequiredArg()
      .describedAs("broker")
      .ofType(classOf[Int])
    val hostOpt = parser.accepts("host", "Filter for a hostname. Option can be used multiple times for multiple hostnames")
      .withRequiredArg()
      .describedAs("host")
      .ofType(classOf[String])
    val rackOpt = parser.accepts("rack", "Filter for a rack. Option can be used multiple times for multiple racks")
      .withRequiredArg()
      .describedAs("rack")
      .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "Filter for a topic. Option can be used multiple times for multiple topics")
      .withRequiredArg()
      .describedAs("topic")
      .ofType(classOf[String])
    val detailsOpt = parser.accepts("details", "if specified, shows detailed listing of topics and partitions")
    val topicDetailsOpt = parser.accepts("topic-details", "if specified, shows topics and partition counts in each topic")
    val partitionDetailsOpt = parser.accepts("partition-details", "if specified, shows partitions in each topic")

    val options = parser.parse(args: _*)

    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(bootstrapServerOpt, brokerOpt, hostOpt, rackOpt, topicOpt, detailsOpt)

    def checkArgs() {
      // Ensure that "zookeeper" option is specified only once.
      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)
      val clusterUrl = options.valuesOf(bootstrapServerOpt)
      if (clusterUrl.size != 1) {
        CommandLineUtils.printUsageAndDie(parser, s"Error: cluster should be specified only once: ${args.mkString(" ")}")
      }
    }

    private def optionValues[T](option: OptionSpec[T]): List[T] = {
      options.valuesOf(option).asScala.toList
    }

    def topics: List[String] = {
      optionValues[String](topicOpt)
    }

    def brokers: List[Int] = {
      optionValues[Int](brokerOpt)
    }

    def hosts: List[String] = {
      optionValues[String](hostOpt)
    }

    def racks: List[String] = {
      optionValues[String](rackOpt)
    }

    def partitionDetailsRequired: Boolean = {
      options.has(partitionDetailsOpt)
    }

    def topicDetailsRequired: Boolean = {
      options.has(topicDetailsOpt)
    }
  }

  // PartitionWithStatus and the derived case classes help
  // encapsulate partition replica and its state (viz. leader, insync or under-replicated/trailing).
  // Each replica state has its own "toString"
  class PartitionWithStatus(partition: Int)

  case class LeaderPartition(partition: Int) extends PartitionWithStatus(partition) {
    override def toString: String = {
      s"+${partition}"
    }
  }

  case class InSyncPartition(partition: Int) extends PartitionWithStatus(partition) {
    override def toString: String = {
      s"${partition}"
    }
  }

  case class TrailingPartition(partition: Int) extends PartitionWithStatus(partition) {
    override def toString: String = {
      s"-${partition}"
    }
  }

  case class NodeWithTopicPartitions(brokerId: Int,
                                     node: Node,
                                     topicPartitions: scala.collection.mutable.Map[String, ArrayBuffer[PartitionWithStatus]]
                                    ) {
    def addTopic(topic: String): Unit = {
      if (topicPartitions.contains(topic)) return else topicPartitions(topic) = ArrayBuffer[PartitionWithStatus]()
    }

    def addTopicPartitionInfo(topic: String, partitionInfo: TopicPartitionInfo): Unit = {
      val replicas: Set[Int] = partitionInfo.replicas.asScala.map(n => n.id).toSet
      val isr: Set[Int] = partitionInfo.isr.asScala.map(n => n.id).toSet
      val leader: Int = partitionInfo.leader.id
      val partitionNumber: Int = partitionInfo.partition
      if (replicas.contains(brokerId)) {
        addTopic(topic)
        val partitionStatus: PartitionWithStatus =
          if (leader == brokerId) LeaderPartition(partitionNumber)
          else if (isr.contains(brokerId)) InSyncPartition(partitionNumber)
          else TrailingPartition(partitionNumber)
        topicPartitions(topic) += partitionStatus
      }
    }
  }

  private object NodeWithTopicPartitions {
    def apply(node: Node): NodeWithTopicPartitions =
      new NodeWithTopicPartitions(node.id,
        node,
        scala.collection.mutable.Map[String, ArrayBuffer[PartitionWithStatus]]())
  }

}

