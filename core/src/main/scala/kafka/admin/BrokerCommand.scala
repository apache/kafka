package kafka.admin

import scala.collection._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import joptsimple._

import kafka.cluster.Broker
import kafka.utils._
import org.apache.kafka.common.requests.MetadataResponse.TopicMetadata
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasUtils

object BrokerCommand {

  def main(args: Array[String]): Unit = {
    val ZKTIMEOUT = 30000
    val SESSIONTIMEOUT = 30000

    val brokerCommandOptions = new BrokerCommandOptions(args)
    brokerCommandOptions.checkArgs()

    val zkUrl = brokerCommandOptions.options.valueOf(brokerCommandOptions.zkConnectOpt)
    val zkUtils = ZkUtils(zkUrl, ZKTIMEOUT, SESSIONTIMEOUT, JaasUtils.isZkSecurityEnabled)
    val allBrokers = getAllBrokers(zkUtils)

    // Note that "filter" here applies only retain those brokers that are specified in the command line.
    // If none specified then include all.
    val filteredBrokers = applyBrokerFilter(
      applyHostFilter(
        applyRackFilter(
          allBrokers,
          brokerCommandOptions),
        brokerCommandOptions),
      brokerCommandOptions)

    val allTopicMetadata = getAllTopicMetadata(zkUtils)
    // Like brokers, only retain topics specified inn the command line. If none specified then include all.
    val filteredTopicMetadata = applyTopicFilter(allTopicMetadata, brokerCommandOptions)

    // This is where the "magic" happens. It aligns the topic/partition data by broker.
    val merged = mergeBrokerTopicMetadata(filteredBrokers, filteredTopicMetadata)
    val output = printInfo(merged, brokerCommandOptions)
    println(output)
  }

  def printInfo(merged: Map[Int, BrokerWithTopicPartitions], brokerCommandOptions: BrokerCommandOptions):String = {
    // Sorting output by broker-id
    val KV_SEPERATOR = ": "
    val FIELD_SEPERATOR = "\t"
    val BROKERID = "BrokerId"
    val HOSTNAME = "Hostname"
    val RACK = "Rack"
    val MISSING_RACK = "N/A"
    val ENDPOINTS = "Endpoints"
    val TOPICS = "Topics"
    val PARTITIONS = "Partitions"
    val LEADER = "Leaders"
    val INSYNC = "InSync"
    val TRAILING = "Trailing"
    val TOPIC_DETAILS = "Topic Details"
    val PARTITION_DETAILS = "Partition Details"
    val LINE_SEPARATOR = String.format("%n")

    val topicDetailsRequired = brokerCommandOptions.options.has(brokerCommandOptions.detailsOpt) ||
      brokerCommandOptions.options.has(brokerCommandOptions.topicDetailsOpt)
    val partitionDetailsRequired = brokerCommandOptions.options.has(brokerCommandOptions.detailsOpt) ||
      brokerCommandOptions.options.has(brokerCommandOptions.partitionDetailsOpt)

    val mergedSortedBrokerIds = SortedMap[Int, BrokerWithTopicPartitions](merged.toArray:_*)

    var output:String = ""
    mergedSortedBrokerIds.values.foreach(brokersWithTopicPartitions => {
      val broker = brokersWithTopicPartitions.broker
      val topicPartitions = brokersWithTopicPartitions.topicPartitions
      output += BROKERID + KV_SEPERATOR + broker.id + FIELD_SEPERATOR
      val host = getAllHostsForBroker(broker).mkString(", ")
      output += HOSTNAME + KV_SEPERATOR + host + FIELD_SEPERATOR
      output += RACK + KV_SEPERATOR + broker.rack.getOrElse(MISSING_RACK) + FIELD_SEPERATOR
      output += ENDPOINTS + KV_SEPERATOR + broker.endPoints.values.mkString(", ").replaceAll("EndPoint", "") + FIELD_SEPERATOR
      output += TOPICS + KV_SEPERATOR + topicPartitions.keys.size.toString + FIELD_SEPERATOR
      val allPartitions = topicPartitions.values.flatten
      output += PARTITIONS + KV_SEPERATOR + allPartitions.size.toString + FIELD_SEPERATOR
      output += LEADER + KV_SEPERATOR + allPartitions.collect{ case p:LeaderPartition => p}.size.toString + FIELD_SEPERATOR
      output += INSYNC + KV_SEPERATOR + allPartitions.collect{ case p:InSyncPartition => p}.size.toString + FIELD_SEPERATOR
      output += TRAILING + KV_SEPERATOR + allPartitions.collect{ case p:TrailingPartition => p}.size.toString + FIELD_SEPERATOR
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

  def getAllBrokers(zkUtils: ZkUtils): Set[Broker]= {
    val allBrokers = zkUtils.getAllBrokersInCluster()
    allBrokers.toSet
  }

  /**
   * For each topic, we get a MetadataResponse.TopicMetadata object
   * which has the following:
   * TopicMetadata.errors = error info, if any none = null
   * TopicMetadata.topic = topic name
   * TopicMetadata.isInternal = boolean value
   * TopicMetadata.partitionMetadata = List of PartitionMetadata,
   *     whose each element is the following partition info:
   *     partitionMetadata.error
   *     partitionMetadata.partition
   *     partitionMetadata.leader
   *     partitionMetadata.replicas = List of brokers
   *     partitionMetadata.isr = List of brokers
   */
  def getAllTopicMetadata(zkUtils: ZkUtils): Set[TopicMetadata] = {
    val allTopics = zkUtils.getAllTopics()
    val allTopicMetadata = AdminUtils.fetchTopicMetadataFromZk(allTopics.toSet, zkUtils)
    allTopicMetadata
  }

  /**
   * This method merges broker and TopicMetadata to return a map keyed by broker-ids and
   * whose value is an object of a simple case class type BrokerWithTopicPartitions.
   * As the name implies, BrokerWithTopicPartitions contains a broker object and a map where
   * the key is the topic name and the value is a sequence of partitions that the broker contains.
   * Note that the partition is not a simple Int
   * @param brokers
   * @param topicMetadata
   * @return Map[Int, BrokerWithTopicPartitions]
   */
  def mergeBrokerTopicMetadata(brokers: Set[Broker], 
                               topicMetadata: Set[TopicMetadata]): Map[Int, BrokerWithTopicPartitions] = {
    val brokersWithTopicPartitions = scala.collection.mutable.Map[Int, BrokerWithTopicPartitions]()
    val brokerIds = brokers.map(_.id)
    for (b <- brokers) {
      brokersWithTopicPartitions(b.id) = new BrokerWithTopicPartitions(b,
        scala.collection.mutable.Map[String, scala.collection.mutable.ArrayBuffer[PartitionWithStatus]]())
    }

    //for (topicData <- topicMetadata.filter(t => t.error() == Errors.NONE)) { // Iterate through each topic
    for (topicData <- topicMetadata) { // Iterate through each topic
      val topicName = topicData.topic()
      val partitions = topicData.partitionMetadata()

      for (partitionData <- partitions) {  // iterate through each partition of a topic
        val partition = partitionData.partition()
        val replicas = partitionData.replicas().map(_.id)
        val leader = partitionData.leader().id()
        val isr = partitionData.isr().map(_.id)

        for (brokerId <- replicas if brokerIds.contains(brokerId)) { // And process the replica list of each partition AND process a brokerId ONLY if it is contained in the input broker set
          // First time seed for a topic for a broker
          if (!brokersWithTopicPartitions(brokerId).topicPartitions.contains(topicName)) {
            brokersWithTopicPartitions(brokerId).topicPartitions(topicName) = scala.collection.mutable.ArrayBuffer[PartitionWithStatus]()
          }
          val partitionWithStatus = if (brokerId == leader)
            LeaderPartition(partition)
          else if (isr.contains(brokerId))
            InSyncPartition(partition)
          else TrailingPartition(partition)
          brokersWithTopicPartitions(brokerId).topicPartitions(topicName).+=:(partitionWithStatus)
        }
      }
    }
    brokersWithTopicPartitions
  }

  def applyBrokerFilter(allBrokers: Set[Broker],
                      brokerCommandOptions: BrokerCommandOptions): Set[Broker] = {
    val filterBrokers = brokerCommandOptions.options.valuesOf(brokerCommandOptions.brokerOpt)
    if (filterBrokers.size() == 0) {
      allBrokers
    } else {
      allBrokers.filter(b => filterBrokers.contains(b.id))
    }
  }

    /**
     *   Given a broker, extract all the unique hostnames that occur across all its endpoints
     */
  def getAllHostsForBroker(broker:Broker): Set[String] = {
    val hosts = broker.endPoints.values.map(e => e.host)
    hosts.toSet
  }

  def applyHostFilter(allBrokers: Set[Broker],
                        brokerCommandOptions: BrokerCommandOptions): Set[Broker] = {
    val filterHosts = brokerCommandOptions.options.valuesOf(brokerCommandOptions.hostOpt)
    if (filterHosts.size() == 0) {
      return allBrokers
    }
    allBrokers.filter(b => getAllHostsForBroker(b).intersect(filterHosts.toSet).nonEmpty)
  }

  def applyRackFilter(allBrokers: Set[Broker],
                      brokerCommandOptions: BrokerCommandOptions): Set[Broker] = {
    val filterRacks = brokerCommandOptions.options.valuesOf(brokerCommandOptions.rackOpt)
    if (filterRacks.size() == 0) {
      return allBrokers
    }
    allBrokers.filter(b => filterRacks.contains(b.rack.getOrElse("")))
  }

  def applyTopicFilter(allTopicMetadata: Set[TopicMetadata],
                       brokerCommandOptions: BrokerCommandOptions): Set[TopicMetadata] = {
    val filterTopics = brokerCommandOptions.options.valuesOf(brokerCommandOptions.topicOpt)
    if (filterTopics.size() == 0) {
      return allTopicMetadata
    }
    allTopicMetadata.filter(t => filterTopics.contains(t.topic()))
  }

  // PartitionWithStatus and the derived case classes help 
  // encapsulate partition replica and its state (viz. leader, insync or under-replicated/trailing).
  // Each replica state has its own "toString"
  class PartitionWithStatus(partition: Int)
  case class LeaderPartition(partition: Int) extends PartitionWithStatus(partition) {
    override def toString:String = {s"+${partition}"}
  }
  case class InSyncPartition(partition: Int) extends PartitionWithStatus(partition) {
    override def toString:String = {s"${partition}"}
  }
  case class TrailingPartition(partition: Int) extends PartitionWithStatus(partition) {
    override def toString:String = {s"-${partition}"}
  }

  case class BrokerWithTopicPartitions(broker: Broker,
                                       topicPartitions: scala.collection.mutable.Map[String, ArrayBuffer[PartitionWithStatus]])

  class BrokerCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for zookeeper(s) " +
      "in the form host:port, host:port/zkchroot or host1,host2,host3:port/zkchroot.")
      .withRequiredArg
      .describedAs("zkurl")
      .ofType(classOf[String])
    val brokerOpt = parser.accepts("broker", "Filter for a broker. Option can be used multiple times for multiple broker-ids")
      .withRequiredArg()
      .describedAs("broker-id")
      .ofType(classOf[Int])
    val hostOpt = parser.accepts("host", "Filter for a hostname. Option can be used multiple times for multiple hostnames")
      .withRequiredArg()
      .describedAs("hostname")
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

    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(zkConnectOpt, brokerOpt, hostOpt, rackOpt, topicOpt, detailsOpt)

    def checkArgs() {
      // Ensure that "zookeeper" option is specified only once.
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      val zkUrls = options.valuesOf(zkConnectOpt)
      if (zkUrls.size != 1) {
        CommandLineUtils.printUsageAndDie(parser, s"Error: zookeeper should be specified only once: ${args.mkString(" ")}")
      }
    }
  }
}
