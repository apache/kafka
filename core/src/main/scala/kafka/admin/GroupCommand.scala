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
import kafka.common.TopicAndPartition
import kafka.utils.Implicits._
import kafka.utils._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set}

object GroupCommand extends Logging {

  def main(args: Array[String]) {
    val opts = new GroupCommandOptions(args)

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "List all groups, describe a group, remove group member.")

    // should have exactly one action
    val actions = Seq(opts.listOpt, opts.describeOpt, opts.leaveGroupOpt).count(opts.options.has _)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offset")

    opts.checkArgs()

    val groupService = new KafkaClientGroupService(opts)

    try {
      if (opts.options.has(opts.listOpt))
        groupService.listGroups().foreach(println(_))
      else if (opts.options.has(opts.leaveGroupOpt)) {
        val groupId = opts.options.valuesOf(opts.groupOpt).asScala.head
        val memberId = opts.options.valuesOf(opts.memberIdOpt).asScala.head
        groupService.leaveGroup(groupId, memberId)
      }
      else if (opts.options.has(opts.describeOpt)) {
        val (state, assignments) = groupService.describeGroup()
        val groupId = opts.options.valuesOf(opts.groupOpt).asScala.head
        assignments match {
          case None =>
            printError(s"The consumer group '$groupId' does not exist.")
          case Some(assignments) =>
            state match {
              case Some("Dead") =>
                printError(s"Consumer group '$groupId' does not exist.")
              case Some("Empty") =>
                System.err.println(s"Consumer group '$groupId' has no active members.")
                printAssignment(assignments, true)
              case Some("PreparingRebalance") | Some("AwaitingSync") =>
                System.err.println(s"Warning: group '$groupId' is rebalancing.")
                printAssignment(assignments, true)
              case Some("Stable") =>
                printAssignment(assignments, true)
              case other =>
                // the control should never reach here
                throw new KafkaException(s"Expected a valid consumer group state, but found '${other.getOrElse("NONE")}'.")
            }
        }
      }
    } catch {
      case e: Throwable =>
        printError(s"Executing group command failed due to ${e.getMessage}", Some(e))
    } finally {
      groupService.close()
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

  sealed trait ClientGroupService {

    def listGroups(): List[String]

    def leaveGroup(groupId: String, clientId: String): Unit

    def describeGroup(): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      collectGroupAssignment(opts.options.valueOf(opts.groupOpt))
    }

    def close(): Unit

    protected def getLogEndOffset(topicPartition: TopicPartition): LogOffsetResult

    protected def opts: GroupCommandOptions

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
        case LogOffsetResult.LogOffset(logEndOffset) => getDescribePartitionResult(Some(logEndOffset))
        case LogOffsetResult.Unknown => getDescribePartitionResult(None)
        case LogOffsetResult.Ignore => null
      }
    }

  }

  class KafkaClientGroupService(val opts: GroupCommandOptions) extends ClientGroupService {

    private val adminClient = createAdminClient()

    // `consumer` is only needed for `describe`, so we instantiate it lazily
    private var consumer: KafkaConsumer[String, String] = null

    def listGroups(): List[String] = {
      adminClient.listAllGroupsFlattened().map(_.groupId)
    }

    def leaveGroup(groupId: String, clientId: String) : Unit = {
      adminClient.leaveGroup(groupId, clientId);
    }

    protected def collectGroupAssignment(group: String): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      val consumerGroupSummary = adminClient.describeGroup(group, opts.options.valueOf(opts.timeoutMsOpt))
      (Some(consumerGroupSummary.state),
        consumerGroupSummary.consumers match {
          case None =>
            None
          case Some(consumers) =>
            var assignedTopicPartitions = Array[TopicPartition]()
            val offsets = adminClient.listGroupOffsets(group)
            val rowsWithConsumer =
            {
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

    protected def getLogEndOffset(topicPartition: TopicPartition): LogOffsetResult = {
      val consumer = getConsumer()
      val offsets = consumer.endOffsets(List(topicPartition).asJava)
      val logStartOffset = offsets.get(topicPartition)
      LogOffsetResult.LogOffset(logStartOffset)
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
      if (opts.options.has(opts.commandConfigOpt))
        properties ++= Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))

      new KafkaConsumer(properties)
    }
  }

  sealed trait LogOffsetResult

  object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  class GroupCommandOptions(args: Array[String]) {
    val BootstrapServerDoc = "REQUIRED (for groups based on the new consumer): The server to connect to."
    val GroupDoc = "The group we wish to act on."
    val ListDoc = "List all groups."
    val DescribeDoc = "Describe group and list offset lag (number of messages not yet processed) related to given group."
    val nl = System.getProperty("line.separator")
    val TimeoutMsDoc = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
      "to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, " +
      "or is going through some changes)."
    val CommandConfigDoc = "Property file containing configs to be passed to Admin Client and Consumer."
    var LeaveGroupDoc = "Let a group member leave the specify group by given groupId and memberId"
    var MemberIdDoc = "Specify the memberId to execute the group command relates that member"

    val parser = new OptionParser(false)
    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val groupOpt = parser.accepts("group", GroupDoc)
      .withRequiredArg
      .describedAs("group")
      .ofType(classOf[String])
    val memberIdOpt = parser.accepts("memberId", MemberIdDoc)
      .withRequiredArg
      .describedAs("memberId")
      .ofType(classOf[String])
    val leaveGroupOpt = parser.accepts("leaveGroup", LeaveGroupDoc)

    val listOpt = parser.accepts("list", ListDoc)
    val describeOpt = parser.accepts("describe", DescribeDoc)
    val timeoutMsOpt = parser.accepts("timeout", TimeoutMsDoc)
      .withRequiredArg
      .describedAs("timeout (ms)")
      .ofType(classOf[Long])
      .defaultsTo(5000)
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])
    val options = parser.parse(args : _*)

    val describeOptPresent = options.has(describeOpt)

    val allGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, describeOpt)

    def checkArgs() {
      // check required args
      if (options.has(timeoutMsOpt) && !describeOptPresent)
        debug(s"Option '$timeoutMsOpt' is applicable only when both '$bootstrapServerOpt' and '$describeOpt' are used.")

      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (describeOptPresent)
        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, allGroupLevelOpts - describeOpt)
    }
  }
}
