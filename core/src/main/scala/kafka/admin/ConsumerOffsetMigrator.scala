package kafka.admin

import java.util.Properties

import joptsimple.OptionParser
import kafka.utils.{CommandLineUtils, ZKGroupDirs, ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

object ConsumerOffsetMigrator {

  def main(args: Array[String]) {
    val opts = new ConsumerGroupCommandOptions(args)

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Migrate offsets from Zookeeper to Kafka")

    opts.checkArgs()

    val zookeeperUrl = opts.options.valueOf(opts.zkConnectOpt)
    val bootstrapServers = opts.options.valueOf(opts.bootstrapServerOpt)
    val group = opts.options.valueOf(opts.groupOpt)
    val dryRun = opts.options.has(opts.dryRunOpt)
    val quiet = opts.options.has(opts.quietOpt)

    val zkClient = createZookeeperClient(zookeeperUrl)
    val consumerClient = createConsumerClient(bootstrapServers, group)

    try {
      println(s"Looking up Zookeeper offsets for group $group")
      val zookeeperOffsets = getZookeeperOffsets(zkClient, group)
      if (!quiet) {
        printOffsets(zookeeperOffsets)
        println()
      }

      println(s"Filtering offsets for old or unauthorized topics or partitions")
      val goodOffsets = filterBadOffsets(consumerClient, zookeeperOffsets)
      if (!quiet) {
        printOffsets(goodOffsets)
        println()
      }

      if (!quiet) {
        println(s"Looking up the matching Kafka offsets for group $group")
        val kafkaOffsets = getKafkaOffsets(consumerClient, group, goodOffsets.keySet)
        printOffsets(kafkaOffsets)
        println()
      }

      if (dryRun)
        println(s"Skipping offset commit because dry-run is set")
      else if (goodOffsets.isEmpty)
        println(s"Skipping offset commit because there are no good offsets to commit")
      else {
        println(s"Committing the good Zookeeper offsets...")
        val goodOffsetsJava = goodOffsets.mapValues(new OffsetAndMetadata(_)).asJava
        consumerClient.commitSync(goodOffsetsJava)
        println(s"Committed the good Zookeeper offsets")
        println()

        if (!quiet) {
          println(s"Looking up the Kafka offsets after commit")
          val kafkaOffsets = getKafkaOffsets(consumerClient, group, goodOffsets.keySet)
          printOffsets(kafkaOffsets)
          println()
        }
      }
    } catch {
      case e: Throwable =>
        println("Error while executing consumer offset migrator command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      zkClient.close()
      consumerClient.close()
    }
  }

  private def createConsumerClient(bootstrapServers: String, groupId: String): KafkaConsumer[String, String] = {
    val properties = new Properties()
    val deserializer = (new StringDeserializer).getClass.getName
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

    new KafkaConsumer(properties)
  }

  private def createZookeeperClient(zookeeperUrl: String): ZkUtils = {
    ZkUtils(zookeeperUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled)
  }

  private def getZookeeperOffsets(client: ZkUtils, groupId: String): Map[TopicPartition, Long] = {
    val groupDirs = new ZKGroupDirs(groupId)
    val topics = client.getChildrenParentMayNotExist(groupDirs.consumerGroupOffsetsDir)
    topics.flatMap { topic =>
      val topicDirs = new ZKGroupTopicDirs(groupId, topic)
      val partitions = client.getChildrenParentMayNotExist(topicDirs.consumerOffsetDir)

      partitions.map { partition =>
        (new TopicPartition(topic, partition.toInt), client.readData(s"${topicDirs.consumerOffsetDir}/$partition")._1.toLong)
      }
    }.toMap
  }

  private def getKafkaOffsets(client: KafkaConsumer[String, String], group: String, partitions: Set[TopicPartition]): Map[TopicPartition, Long] = {
    partitions.flatMap { tp =>
      val offsetAndMetadata = client.committed(tp)
      if (offsetAndMetadata != null)
        Some((tp, offsetAndMetadata.offset))
      else
        None
    }.toMap
  }

  // Filters out offsets for topics or partitions that do not exist or are unauthorized
  private def filterBadOffsets(client: KafkaConsumer[String, String], offsets: Map[TopicPartition, Long]): Map[TopicPartition, Long]= {
    val existingTopicPartitions = client.listTopics().asScala.flatMap { case (topic, partitions) =>
      partitions.asScala.map { partitionInfo =>
        new TopicPartition(topic, partitionInfo.partition)
      }
    }.toSet

    offsets.filter { case (tp, offset) =>
      existingTopicPartitions.contains(tp)
    }
  }

  private def printOffsets(offsets: Map[TopicPartition, Long]) = {
    if (offsets.isEmpty) {
      println(s"No offsets found.")
    } else {
      println(s"Found the following offsets:")
      offsets.toSeq
        .sortBy { case (tp, offset) => (tp.topic, tp.partition) }
        .foreach { case (tp, offset) =>
          println(s"   ${tp.topic}:${tp.partition} = $offset")
        }
    }
  }

  class ConsumerGroupCommandOptions(args: Array[String]) {
    val ZkConnectDoc = "REQUIRED: The connection string for the zookeeper connection in the form host:port. Multiple URLS can be given to allow fail-over."
    val BootstrapServerDoc = "REQUIRED: A list of host/port pairs to use for establishing the initial connection to the Kafka cluster."
    val GroupDoc = "REQUIRED: The consumer group to migrate offsets for."
    val DryRunDoc = "If set, offsets will only be read and logged. No Offsets will be committed."
    val QuietDoc = "If set, verbose offset logging will be disabled."
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", ZkConnectDoc)
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val groupOpt = parser.accepts("group", GroupDoc)
      .withRequiredArg
      .describedAs("consumer group")
      .ofType(classOf[String])
    val dryRunOpt = parser.accepts("dry-run", DryRunDoc)
    val quietOpt = parser.accepts("quiet", QuietDoc)
    val options = parser.parse(args : _*)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, bootstrapServerOpt, groupOpt)
    }
  }
}
