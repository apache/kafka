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

import joptsimple.{OptionParser, OptionSpec}

import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{TopicAndPartition, _}
import kafka.consumer.SimpleConsumer
import kafka.utils._

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{Errors, SecurityProtocol}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.collection.{Set, mutable}

object ConsumerGroupCommand extends Logging {

  def main(args: Array[String]) {
    val opts = new ConsumerGroupCommandOptions(args)

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "List all consumer groups, describe a consumer group, or delete consumer group info.")

    // should have exactly one action
    val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --delete")

    opts.checkArgs()

    val consumerGroupService = {
      if (opts.useOldConsumer) {
        System.err.println("Note: This will only show information about consumers that use ZooKeeper (not those using the Java consumer API).\n")
        new ZkConsumerGroupService(opts)
      } else {
        System.err.println("Note: This will only show information about consumers that use the Java consumer API (non-ZooKeeper-based consumers).\n")
        new KafkaConsumerGroupService(opts)
      }
    }

    try {
      if (opts.options.has(opts.listOpt))
        consumerGroupService.listGroups().foreach(println(_))
      else if (opts.options.has(opts.describeOpt)) {
        val (state, assignments) = consumerGroupService.describeGroup()
        val groupId = opts.options.valuesOf(opts.groupOpt).asScala.head
        assignments match {
          case None =>
            // applies to both old and new consumer
            printError(s"The consumer group '$groupId' does not exist.")
          case Some(assignments) =>
            if (opts.useOldConsumer)
              printAssignment(assignments, false)
            else
              state match {
                case Some("Dead") =>
                  printError(s"Consumer group '$groupId' does not exist.")
                case Some("Empty") =>
                  System.err.println(s"Consumer group '$groupId' has no active members.")
                  printAssignment(assignments, true)
                case Some("PreparingRebalance") | Some("AwaitingSync") =>
                  System.err.println(s"Warning: Consumer group '$groupId' is rebalancing.")
                  printAssignment(assignments, true)
                case Some("Stable") =>
                  printAssignment(assignments, true)
                case other =>
                  // the control should never reach here
                  throw new KafkaException(s"Expected a valid consumer group state, but found '${other.getOrElse("NONE")}'.")
              }
        }
      }
      else if (opts.options.has(opts.deleteOpt)) {
        consumerGroupService match {
          case service: ZkConsumerGroupService => service.deleteGroups()
          case _ => throw new IllegalStateException(s"delete is not supported for $consumerGroupService.")
        }
      }
    } catch {
      case e: Throwable =>
        printError(s"Executing consumer group command failed due to ${e.getMessage}", Some(e))
    } finally {
      consumerGroupService.close()
    }
  }

  val MISSING_COLUMN_VALUE = "-"

  def printError(msg: String, e: Option[Throwable] = None): Unit = {
    println(s"Error: $msg")
    e.foreach(debug("Exception in consumer group command", _))
  }

  def printAssignment(groupAssignment: Seq[PartitionAssignmentState], useNewConsumer: Boolean): Unit = {
    print("\n%-30s %-10s %-15s %-15s %-10s %-50s".format("TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID"))
    if (useNewConsumer)
      print("%-30s %s".format("HOST", "CLIENT-ID"))
    println()

    groupAssignment.foreach { consumerAssignment =>
      print("%-30s %-10s %-15s %-15s %-10s %-50s".format(
        consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.getOrElse(MISSING_COLUMN_VALUE),
        consumerAssignment.offset.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.getOrElse(MISSING_COLUMN_VALUE),
        consumerAssignment.lag.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE)))
      if (useNewConsumer)
        print("%-30s %s".format(consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.getOrElse(MISSING_COLUMN_VALUE)))
      println()
    }
  }

  protected case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                                partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                                consumerId: Option[String], host: Option[String],
                                                clientId: Option[String], logEndOffset: Option[Long])

  sealed trait ConsumerGroupService {

    def listGroups(): List[String]

    def describeGroup(): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      collectGroupAssignment(opts.options.valueOf(opts.groupOpt))
    }

    def close(): Unit

    protected def opts: ConsumerGroupCommandOptions

    protected def getLogEndOffset(topicPartition: TopicPartition): LogEndOffsetResult

    protected def collectGroupAssignment(group: String): (Option[String], Option[Seq[PartitionAssignmentState]])

    protected def collectConsumerAssignment(group: String,
                                            coordinator: Option[Node],
                                            topicPartitions: Seq[TopicAndPartition],
                                            getPartitionOffset: TopicAndPartition => Option[Long],
                                            consumerIdOpt: Option[String],
                                            hostOpt: Option[String],
                                            clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {
      if (topicPartitions.isEmpty)
        Array[PartitionAssignmentState](
          PartitionAssignmentState(group, coordinator, None, None, None, getLag(None, None), consumerIdOpt, hostOpt, clientIdOpt, None)
        )
      else {
        var assignmentRows: Array[PartitionAssignmentState] = Array()
        topicPartitions
          .sortBy(_.partition)
          .foreach { topicPartition =>
            assignmentRows = assignmentRows :+ describePartition(group, coordinator, topicPartition.topic, topicPartition.partition, getPartitionOffset(topicPartition),
              consumerIdOpt, hostOpt, clientIdOpt)
          }
        assignmentRows
      }
    }

    protected def getLag(offset: Option[Long], logEndOffset: Option[Long]): Option[Long] =
      offset.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))

    private def describePartition(group: String,
                                  coordinator: Option[Node],
                                  topic: String,
                                  partition: Int,
                                  offsetOpt: Option[Long],
                                  consumerIdOpt: Option[String],
                                  hostOpt: Option[String],
                                  clientIdOpt: Option[String]): PartitionAssignmentState = {
      def getDescribePartitionResult(logEndOffsetOpt: Option[Long]): PartitionAssignmentState =
        PartitionAssignmentState(group, coordinator, Option(topic), Option(partition), offsetOpt,
                                 getLag(offsetOpt, logEndOffsetOpt), consumerIdOpt, hostOpt,
                                 clientIdOpt, logEndOffsetOpt)

      getLogEndOffset(new TopicPartition(topic, partition)) match {
        case LogEndOffsetResult.LogEndOffset(logEndOffset) => getDescribePartitionResult(Some(logEndOffset))
        case LogEndOffsetResult.Unknown => getDescribePartitionResult(None)
        case LogEndOffsetResult.Ignore => null
      }
    }

  }

  class ZkConsumerGroupService(val opts: ConsumerGroupCommandOptions) extends ConsumerGroupService {

    private val zkUtils = {
      val zkUrl = opts.options.valueOf(opts.zkConnectOpt)
      ZkUtils(zkUrl, 30000, 30000, JaasUtils.isZkSecurityEnabled)
    }

    def close() {
      zkUtils.close()
    }

    def listGroups(): List[String] = {
      zkUtils.getConsumerGroups().toList
    }

    def deleteGroups() {
      if (opts.options.has(opts.groupOpt) && opts.options.has(opts.topicOpt))
        deleteForTopic()
      else if (opts.options.has(opts.groupOpt))
        deleteForGroup()
      else if (opts.options.has(opts.topicOpt))
        deleteAllForTopic()
    }

    protected def collectGroupAssignment(group: String): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      val channelSocketTimeoutMs = props.getProperty("channelSocketTimeoutMs", "600").toInt
      val channelRetryBackoffMs = props.getProperty("channelRetryBackoffMsOpt", "300").toInt
      if (!zkUtils.getConsumerGroups().contains(group))
        return (None, None)

      val topics = zkUtils.getTopicsByConsumerGroup(group)
      val topicPartitions = getAllTopicPartitions(topics)
      var groupConsumerIds = zkUtils.getConsumersInGroup(group)

      // mapping of topic partition -> consumer id
      val consumerIdByTopicPartition = topicPartitions.map { topicPartition =>
        val owner = zkUtils.readDataMaybeNull(new ZKGroupTopicDirs(group, topicPartition.topic).consumerOwnerDir + "/" + topicPartition.partition)._1
        topicPartition -> owner.map(o => o.substring(0, o.lastIndexOf('-'))).getOrElse(MISSING_COLUMN_VALUE)
      }.toMap

      // mapping of consumer id -> list of topic partitions
      val consumerTopicPartitions = consumerIdByTopicPartition groupBy{_._2} map {
        case (key, value) => (key, value.unzip._1.toArray) }

      // mapping of consumer id -> list of subscribed topics
      val topicsByConsumerId = zkUtils.getTopicsPerMemberId(group)

      var assignmentRows = topicPartitions.flatMap { topicPartition =>
        val partitionOffsets = getPartitionOffsets(group, List(topicPartition), channelSocketTimeoutMs, channelRetryBackoffMs)
        val consumerId = consumerIdByTopicPartition.get(topicPartition)
        // since consumer id is repeated in client id, leave host and client id empty
        consumerId.foreach(id => groupConsumerIds = groupConsumerIds.filterNot(_ == id))
        collectConsumerAssignment(group, None, List(topicPartition), partitionOffsets.get, consumerId, None, None)
      }

      assignmentRows ++= groupConsumerIds.sortBy(- consumerTopicPartitions.get(_).size).flatMap { consumerId =>
        topicsByConsumerId(consumerId).flatMap { _ =>
          // since consumers with no topic partitions are processed here, we pass empty for topic partitions and offsets
          // since consumer id is repeated in client id, leave host and client id empty
          collectConsumerAssignment(group, None, Array[TopicAndPartition](), Map[TopicAndPartition, Option[Long]](), Some(consumerId), None, None)
        }
      }

      (None, Some(assignmentRows))
    }

    private def getAllTopicPartitions(topics: Seq[String]): Seq[TopicAndPartition] = {
      val topicPartitionMap = zkUtils.getPartitionsForTopics(topics)
      topics.flatMap { topic =>
        val partitions = topicPartitionMap.getOrElse(topic, Seq.empty)
        partitions.map(TopicAndPartition(topic, _))
      }
    }

    protected def getLogEndOffset(topicPartition: TopicPartition): LogEndOffsetResult = {
      zkUtils.getLeaderForPartition(topicPartition.topic, topicPartition.partition) match {
        case Some(-1) => LogEndOffsetResult.Unknown
        case Some(brokerId) =>
          getZkConsumer(brokerId).map { consumer =>
            val topicAndPartition = TopicAndPartition(topicPartition.topic, topicPartition.partition)
            val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
            val logEndOffset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
            consumer.close()
            LogEndOffsetResult.LogEndOffset(logEndOffset)
          }.getOrElse(LogEndOffsetResult.Ignore)
        case None =>
          printError(s"No broker for partition '$topicPartition'")
          LogEndOffsetResult.Ignore
      }
    }

    private def getPartitionOffsets(group: String,
                                    topicPartitions: Seq[TopicAndPartition],
                                    channelSocketTimeoutMs: Int,
                                    channelRetryBackoffMs: Int): Map[TopicAndPartition, Long] = {
      val offsetMap = mutable.Map[TopicAndPartition, Long]()
      val channel = ClientUtils.channelToOffsetManager(group, zkUtils, channelSocketTimeoutMs, channelRetryBackoffMs)
      channel.send(OffsetFetchRequest(group, topicPartitions))
      val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())

      offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
        offsetAndMetadata match {
          case OffsetMetadataAndError.NoOffset =>
            val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
            // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
            // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
            try {
              val offset = zkUtils.readData(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1.toLong
              offsetMap.put(topicAndPartition, offset)
            } catch {
              case z: ZkNoNodeException =>
                printError(s"Could not fetch offset from zookeeper for group '$group' partition '$topicAndPartition' due to missing offset data in zookeeper.", Some(z))
            }
          case offsetAndMetaData if offsetAndMetaData.error == Errors.NONE.code =>
            offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
          case _ =>
            printError(s"Could not fetch offset from kafka for group '$group' partition '$topicAndPartition' due to ${Errors.forCode(offsetAndMetadata.error).exception}.")
        }
      }
      channel.disconnect()
      offsetMap.toMap
    }

    private def deleteForGroup() {
      val groups = opts.options.valuesOf(opts.groupOpt)
      groups.asScala.foreach { group =>
        try {
          if (AdminUtils.deleteConsumerGroupInZK(zkUtils, group))
            println(s"Deleted all consumer group information for group '$group' in zookeeper.")
          else
            printError(s"Delete for group '$group' failed because its consumers are still active.")
        }
        catch {
          case e: ZkNoNodeException =>
            printError(s"Delete for group '$group' failed because group does not exist.", Some(e))
        }
      }
    }

    private def deleteForTopic() {
      val groups = opts.options.valuesOf(opts.groupOpt)
      val topic = opts.options.valueOf(opts.topicOpt)
      Topic.validate(topic)
      groups.asScala.foreach { group =>
        try {
          if (AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topic))
            println(s"Deleted consumer group information for group '$group' topic '$topic' in zookeeper.")
          else
            printError(s"Delete for group '$group' topic '$topic' failed because its consumers are still active.")
        }
        catch {
          case e: ZkNoNodeException =>
            printError(s"Delete for group '$group' topic '$topic' failed because group does not exist.", Some(e))
        }
      }
    }

    private def deleteAllForTopic() {
      val topic = opts.options.valueOf(opts.topicOpt)
      Topic.validate(topic)
      AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic)
      println(s"Deleted consumer group information for all inactive consumer groups for topic '$topic' in zookeeper.")
    }

    private def getZkConsumer(brokerId: Int): Option[SimpleConsumer] = {
      try {
        zkUtils.getBrokerInfo(brokerId)
          .map(_.getBrokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)))
          .map(endPoint => new SimpleConsumer(endPoint.host, endPoint.port, 10000, 100000, "ConsumerGroupCommand"))
          .orElse(throw new BrokerNotAvailableException("Broker id %d does not exist".format(brokerId)))
      } catch {
        case t: Throwable =>
          printError(s"Could not parse broker info due to ${t.getMessage}", Some(t))
          None
      }
    }

  }

  class KafkaConsumerGroupService(val opts: ConsumerGroupCommandOptions) extends ConsumerGroupService {

    private val adminClient = createAdminClient()

    // `consumer` is only needed for `describe`, so we instantiate it lazily
    private var consumer: KafkaConsumer[String, String] = null

    def listGroups(): List[String] = {
      adminClient.listAllConsumerGroupsFlattened().map(_.groupId)
    }

    protected def collectGroupAssignment(group: String): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      val consumerGroupSummary = adminClient.describeConsumerGroup(group)
      (Some(consumerGroupSummary.state),
        consumerGroupSummary.consumers match {
          case None =>
            None
          case Some(consumers) =>
            var assignedTopicPartitions = Array[TopicPartition]()
            val offsets = adminClient.listGroupOffsets(group)
            val rowsWithConsumer =
              if (offsets.isEmpty)
                List[PartitionAssignmentState]()
              else {
                consumers.sortWith(_.assignment.size > _.assignment.size).flatMap { consumerSummary =>
                  val topicPartitions = consumerSummary.assignment.map(tp => TopicAndPartition(tp.topic, tp.partition))
                  assignedTopicPartitions = assignedTopicPartitions ++ consumerSummary.assignment
                  val partitionOffsets: Map[TopicAndPartition, Option[Long]] = consumerSummary.assignment.map { topicPartition =>
                    new TopicAndPartition(topicPartition) -> offsets.get(topicPartition)
                  }.toMap
                  collectConsumerAssignment(group, Some(consumerGroupSummary.coordinator), topicPartitions,
                    partitionOffsets, Some(s"${consumerSummary.consumerId}"), Some(s"${consumerSummary.host}"),
                    Some(s"${consumerSummary.clientId}"))
                }
              }

            val rowsWithoutConsumer = offsets.filterNot {
              case (topicPartition, offset) => assignedTopicPartitions.contains(topicPartition)
              }.flatMap {
                case (topicPartition, offset) =>
                  val topicAndPartition = new TopicAndPartition(topicPartition)
                  collectConsumerAssignment(group, Some(consumerGroupSummary.coordinator), Seq(topicAndPartition),
                      Map(topicAndPartition -> Some(offset)), Some(MISSING_COLUMN_VALUE),
                      Some(MISSING_COLUMN_VALUE), Some(MISSING_COLUMN_VALUE))
                }

            Some(rowsWithConsumer ++ rowsWithoutConsumer)
      }
      )
    }

    protected def getLogEndOffset(topicPartition: TopicPartition): LogEndOffsetResult = {
      val consumer = getConsumer()
      consumer.assign(List(topicPartition).asJava)
      consumer.seekToEnd(List(topicPartition).asJava)
      val logEndOffset = consumer.position(topicPartition)
      LogEndOffsetResult.LogEndOffset(logEndOffset)
    }

    def close() {
      adminClient.close()
      if (consumer != null) consumer.close()
    }

    private def createAdminClient(): AdminClient = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      AdminClient.create(props)
    }

    private def getConsumer() = {
      if (consumer == null)
        consumer = createNewConsumer()
      consumer
    }

    private def createNewConsumer(): KafkaConsumer[String, String] = {
      val properties = new Properties()
      val deserializer = (new StringDeserializer).getClass.getName
      val brokerUrl = opts.options.valueOf(opts.bootstrapServerOpt)
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, opts.options.valueOf(opts.groupOpt))
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)
      if (opts.options.has(opts.commandConfigOpt)) properties.putAll(Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)))

      new KafkaConsumer(properties)
    }

  }

  sealed trait LogEndOffsetResult

  object LogEndOffsetResult {
    case class LogEndOffset(value: Long) extends LogEndOffsetResult
    case object Unknown extends LogEndOffsetResult
    case object Ignore extends LogEndOffsetResult
  }

  class ConsumerGroupCommandOptions(args: Array[String]) {
    val ZkConnectDoc = "REQUIRED (only when using old consumer): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over."
    val BootstrapServerDoc = "REQUIRED (unless old consumer is used): The server to connect to."
    val GroupDoc = "The consumer group we wish to act on."
    val TopicDoc = "The topic whose consumer group information should be deleted."
    val ListDoc = "List all consumer groups."
    val DescribeDoc = "Describe consumer group and list offset lag (number of messages not yet processed) related to given group."
    val nl = System.getProperty("line.separator")
    val DeleteDoc = "Pass in groups to delete topic partition offsets and ownership information " +
      "over the entire consumer group. For instance --group g1 --group g2" + nl +
      "Pass in groups with a single topic to just delete the given topic's partition offsets and ownership " +
      "information for the given consumer groups. For instance --group g1 --group g2 --topic t1" + nl +
      "Pass in just a topic to delete the given topic's partition offsets and ownership information " +
      "for every consumer group. For instance --topic t1" + nl +
      "WARNING: Group deletion only works for old ZK-based consumer groups, and one has to use it carefully to only delete groups that are not active."
    val NewConsumerDoc = "Use new consumer. This is the default."
    val CommandConfigDoc = "Property file containing configs to be passed to Admin Client and Consumer."
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
    val topicOpt = parser.accepts("topic", TopicDoc)
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val listOpt = parser.accepts("list", ListDoc)
    val describeOpt = parser.accepts("describe", DescribeDoc)
    val deleteOpt = parser.accepts("delete", DeleteDoc)
    val newConsumerOpt = parser.accepts("new-consumer", NewConsumerDoc)
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                  .withRequiredArg
                                  .describedAs("command config property file")
                                  .ofType(classOf[String])
    val options = parser.parse(args : _*)

    val useOldConsumer = options.has(zkConnectOpt)

    val allConsumerGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, describeOpt, deleteOpt)

    def checkArgs() {
      // check required args
      if (useOldConsumer) {
        if (options.has(bootstrapServerOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option '$bootstrapServerOpt' is not valid with '$zkConnectOpt'.")
        else if (options.has(newConsumerOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option '$newConsumerOpt' is not valid with '$zkConnectOpt'.")
      } else {
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

        if (options.has(deleteOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option '$deleteOpt' is only valid with '$zkConnectOpt'. Note that " +
            "there's no need to delete group metadata for the new consumer as the group is deleted when the last " +
            "committed offset for that group expires.")
      }

      if (options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)
      if (options.has(deleteOpt) && !options.has(groupOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, "Option %s either takes %s, %s, or both".format(deleteOpt, groupOpt, topicOpt))

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, allConsumerGroupLevelOpts - describeOpt - deleteOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, allConsumerGroupLevelOpts - deleteOpt)
    }
  }
}
