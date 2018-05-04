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

import java.text.{ParseException, SimpleDateFormat}
import java.util.{Date, Properties}
import javax.xml.datatype.DatatypeFactory
import joptsimple.{OptionParser, OptionSpec}

import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils._
import kafka.utils.Implicits._

import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.errors.BrokerNotAvailableException
import org.apache.kafka.common.{KafkaException, Node, TopicPartition}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._
import scala.collection.{Seq, Set, mutable}

object ConsumerGroupCommand extends Logging {

  def main(args: Array[String]) {
    val opts = new ConsumerGroupCommandOptions(args)

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "List all consumer groups, describe a consumer group, delete consumer group info, or reset consumer group offsets.")

    // should have exactly one action
    val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt).count(opts.options.has)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets")

    opts.checkArgs()

    val consumerGroupService = {
      if (opts.useOldConsumer) {
        Console.err.println("Note: This will only show information about consumers that use ZooKeeper (not those using the Java consumer API).")
        new ZkConsumerGroupService(opts)
      } else {
        Console.err.println("Note: This will not show information about old Zookeeper-based consumers.")
        new KafkaConsumerGroupService(opts)
      }
    }

    try {
      if (opts.options.has(opts.listOpt))
        consumerGroupService.listGroups().foreach(println(_))
      else if (opts.options.has(opts.describeOpt))
        consumerGroupService.describeGroup()
      else if (opts.options.has(opts.deleteOpt))
        consumerGroupService.deleteGroups()
      else if (opts.options.has(opts.resetOffsetsOpt)) {
        val offsetsToReset = consumerGroupService.resetOffsets()
        if (opts.options.has(opts.exportOpt)) {
          val exported = consumerGroupService.exportOffsetsToReset(offsetsToReset)
          println(exported)
        } else
          printOffsetsToReset(offsetsToReset)
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

  def convertTimestamp(timeString: String): java.lang.Long = {
    val datetime: String = timeString match {
      case ts if ts.split("T")(1).contains("+") || ts.split("T")(1).contains("-") || ts.split("T")(1).contains("Z") => ts.toString
      case ts => s"${ts}Z"
    }
    val date = try {
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").parse(datetime)
    } catch {
      case _: ParseException => new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").parse(datetime)
    }
    date.getTime
  }

  def printOffsetsToReset(groupAssignmentsToReset: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    println("\n%-30s %-10s %-15s".format("TOPIC", "PARTITION", "NEW-OFFSET"))

    groupAssignmentsToReset.foreach {
      case (consumerAssignment, offsetAndMetadata) =>
        println("%-30s %-10s %-15s".format(
          consumerAssignment.topic,
          consumerAssignment.partition,
          offsetAndMetadata.offset))
    }
  }

  protected case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                                partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                                consumerId: Option[String], host: Option[String],
                                                clientId: Option[String], logEndOffset: Option[Long])

  protected case class MemberAssignmentState(group: String, consumerId: String, host: String, clientId: String,
                                             numPartitions: Int, assignment: List[TopicPartition])

  protected case class GroupState(group: String, coordinator: Node, assignmentStrategy: String, state: String, numMembers: Int)

  sealed trait ConsumerGroupService {

    def listGroups(): List[String]

    private def shouldPrintMemberState(group: String, state: Option[String], numRows: Option[Int]): Boolean = {
      // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
      // if it's undefined or 0, there is no relevant group information to display.
      numRows match {
        case None =>
          // applies to both old and new consumer
          printError(s"The consumer group '$group' does not exist.")
          false
        case Some(num) =>
          opts.useOldConsumer || {
            state match {
              case Some("Dead") =>
                printError(s"Consumer group '$group' does not exist.")
              case Some("Empty") =>
                Console.err.println(s"Consumer group '$group' has no active members.")
              case Some("PreparingRebalance") | Some("CompletingRebalance") =>
                Console.err.println(s"Warning: Consumer group '$group' is rebalancing.")
              case Some("Stable") =>
              case other =>
                // the control should never reach here
                throw new KafkaException(s"Expected a valid consumer group state, but found '${other.getOrElse("NONE")}'.")
            }
            !state.contains("Dead") && num > 0
          }
      }
    }

    private def size(colOpt: Option[Seq[Object]]): Option[Int] = colOpt.map(_.size)

    private def printOffsets(group: String, state: Option[String], assignments: Option[Seq[PartitionAssignmentState]]): Unit = {
      if (shouldPrintMemberState(group, state, size(assignments))) {
        // find proper columns width
        var (maxTopicLen, maxConsumerIdLen, maxHostLen) = (15, 15, 15)
        assignments match {
          case None => // do nothing
          case Some(consumerAssignments) =>
            consumerAssignments.foreach { consumerAssignment =>
              maxTopicLen = Math.max(maxTopicLen, consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE).length)
              maxConsumerIdLen = Math.max(maxConsumerIdLen, consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE).length)
              if (!opts.useOldConsumer)
                maxHostLen = Math.max(maxHostLen, consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE).length)
            }
        }

        print(s"\n%${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s "
          .format("TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID"))

        if (!opts.useOldConsumer)
          print(s"%${-maxHostLen}s %s".format("HOST", "CLIENT-ID"))
        println()

        assignments match {
          case None => // do nothing
          case Some(consumerAssignments) =>
            consumerAssignments.foreach { consumerAssignment =>
              print(s"%${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s ".format(
                consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.getOrElse(MISSING_COLUMN_VALUE),
                consumerAssignment.offset.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.getOrElse(MISSING_COLUMN_VALUE),
                consumerAssignment.lag.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE)))
              if (!opts.useOldConsumer)
                print(s"%${-maxHostLen}s %s".format(consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.clientId.getOrElse(MISSING_COLUMN_VALUE)))
              println()
            }
        }
      }
    }

    private def printMembers(group: String, state: Option[String], assignments: Option[Seq[MemberAssignmentState]], verbose: Boolean): Unit = {
      if (shouldPrintMemberState(group, state, size(assignments))) {
        // find proper columns width
        var (maxConsumerIdLen, maxHostLen, maxClientIdLen) = (15, 15, 15)
        assignments match {
          case None => // do nothing
          case Some(memberAssignments) =>
            memberAssignments.foreach { memberAssignment =>
              maxConsumerIdLen = Math.max(maxConsumerIdLen, memberAssignment.consumerId.length)
              maxHostLen = Math.max(maxHostLen, memberAssignment.host.length)
              maxClientIdLen = Math.max(maxClientIdLen, memberAssignment.clientId.length)
            }
        }

        print(s"\n%${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "
          .format("CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
        if (verbose)
          print("%s".format("ASSIGNMENT"))
        println()

        assignments match {
          case None => // do nothing
          case Some(memberAssignments) =>
            memberAssignments.foreach { memberAssignment =>
              print(s"%${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s ".format(
                memberAssignment.consumerId, memberAssignment.host, memberAssignment.clientId, memberAssignment.numPartitions))
              if (verbose) {
                val partitions = memberAssignment.assignment match {
                  case List() => MISSING_COLUMN_VALUE
                  case assignment =>
                    assignment.groupBy(_.topic).map {
                      case (topic, partitionList) => topic + partitionList.map(_.partition).sorted.mkString("(", ",", ")")
                    }.toList.sorted.mkString(", ")
                }
                print("%s".format(partitions))
              }
              println()
            }
        }
      }
    }

    private def printState(group: String, state: GroupState): Unit = {
      // this method is reachable only for the new consumer option (where the given state is always defined)
      if (shouldPrintMemberState(group, Some(state.state), Some(1))) {
        val coordinator = s"${state.coordinator.host}:${state.coordinator.port} (${state.coordinator.idString})"
        val coordinatorColLen = Math.max(25, coordinator.length)
        print(s"\n%${-coordinatorColLen}s %-25s %-20s %s".format("COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS"))
        print(s"\n%${-coordinatorColLen}s %-25s %-20s %s".format(coordinator, state.assignmentStrategy, state.state, state.numMembers))
        println()
      }
    }

    def describeGroup(): Unit = {
      val group = opts.options.valuesOf(opts.groupOpt).asScala.head
      val membersOptPresent = opts.options.has(opts.membersOpt)
      val stateOptPresent = opts.options.has(opts.stateOpt)
      val offsetsOptPresent = opts.options.has(opts.offsetsOpt)
      val subActions = Seq(membersOptPresent, offsetsOptPresent, stateOptPresent).count(_ == true)

      if (subActions == 0 || offsetsOptPresent) {
        val offsets = collectGroupOffsets()
        printOffsets(group, offsets._1, offsets._2)
      } else if (membersOptPresent) {
        val members = collectGroupMembers(opts.options.has(opts.verboseOpt))
        printMembers(group, members._1, members._2, opts.options.has(opts.verboseOpt))
      } else
        printState(group, collectGroupState())
    }

    def close(): Unit

    protected def opts: ConsumerGroupCommandOptions

    protected def getLogEndOffset(topicPartition: TopicPartition): LogOffsetResult =
      getLogEndOffsets(Seq(topicPartition)).getOrElse(topicPartition, LogOffsetResult.Ignore)

    protected def getLogEndOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult]

    def collectGroupOffsets(): (Option[String], Option[Seq[PartitionAssignmentState]])

    def collectGroupMembers(verbose: Boolean): (Option[String], Option[Seq[MemberAssignmentState]]) = throw new UnsupportedOperationException

    def collectGroupState(): GroupState = throw new UnsupportedOperationException

    protected def collectConsumerAssignment(group: String,
                                            coordinator: Option[Node],
                                            topicPartitions: Seq[TopicPartition],
                                            getPartitionOffset: TopicPartition => Option[Long],
                                            consumerIdOpt: Option[String],
                                            hostOpt: Option[String],
                                            clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {
      if (topicPartitions.isEmpty) {
        Array[PartitionAssignmentState](
          PartitionAssignmentState(group, coordinator, None, None, None, getLag(None, None), consumerIdOpt, hostOpt, clientIdOpt, None)
        )
      }
      else
        describePartitions(group, coordinator, topicPartitions.sortBy(_.partition), getPartitionOffset, consumerIdOpt, hostOpt, clientIdOpt)
    }

    private def getLag(offset: Option[Long], logEndOffset: Option[Long]): Option[Long] =
      offset.filter(_ != -1).flatMap(offset => logEndOffset.map(_ - offset))

    private def describePartitions(group: String,
                                   coordinator: Option[Node],
                                   topicPartitions: Seq[TopicPartition],
                                   getPartitionOffset: TopicPartition => Option[Long],
                                   consumerIdOpt: Option[String],
                                   hostOpt: Option[String],
                                   clientIdOpt: Option[String]): Array[PartitionAssignmentState] = {

      def getDescribePartitionResult(topicPartition: TopicPartition, logEndOffsetOpt: Option[Long]): PartitionAssignmentState = {
        val offset = getPartitionOffset(topicPartition)
        PartitionAssignmentState(group, coordinator, Option(topicPartition.topic), Option(topicPartition.partition), offset,
          getLag(offset, logEndOffsetOpt), consumerIdOpt, hostOpt, clientIdOpt, logEndOffsetOpt)
      }

      getLogEndOffsets(topicPartitions).map {
        logEndOffsetResult =>
          logEndOffsetResult._2 match {
            case LogOffsetResult.LogOffset(logEndOffset) => getDescribePartitionResult(logEndOffsetResult._1, Some(logEndOffset))
            case LogOffsetResult.Unknown => getDescribePartitionResult(logEndOffsetResult._1, None)
            case LogOffsetResult.Ignore => null
          }
      }.toArray
    }

    def resetOffsets(): Map[TopicPartition, OffsetAndMetadata] = throw new UnsupportedOperationException

    def exportOffsetsToReset(assignmentsToReset: Map[TopicPartition, OffsetAndMetadata]): String = throw new UnsupportedOperationException

    def deleteGroups(): Map[String, Errors]
  }

  @deprecated("This class has been deprecated and will be removed in a future release.", "0.11.0.0")
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

    def deleteGroups(): Map[String, Errors] = {
      if (opts.options.has(opts.groupOpt) && opts.options.has(opts.topicOpt))
        deleteGroupsInfoForTopic()
      else if (opts.options.has(opts.groupOpt))
        deleteGroupsInfo()
      else if (opts.options.has(opts.topicOpt))
        deleteAllGroupsInfoForTopic()

      Map()
    }

    def collectGroupOffsets(): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      val group = opts.options.valueOf(opts.groupOpt)
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
          collectConsumerAssignment(group, None, Array[TopicPartition](), Map[TopicPartition, Option[Long]](), Some(consumerId), None, None)
        }
      }

      (None, Some(assignmentRows))
    }

    private def getAllTopicPartitions(topics: Seq[String]): Seq[TopicPartition] = {
      val topicPartitionMap = zkUtils.getPartitionsForTopics(topics)
      topics.flatMap { topic =>
        val partitions = topicPartitionMap.getOrElse(topic, Seq.empty)
        partitions.map(new TopicPartition(topic, _))
      }
    }

    protected def getLogEndOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      topicPartitions.map { topicPartition => (topicPartition,
        zkUtils.getLeaderForPartition(topicPartition.topic, topicPartition.partition) match {
          case Some(-1) => LogOffsetResult.Unknown
          case Some(brokerId) =>
            getZkConsumer(brokerId).map { consumer =>
              val topicAndPartition = new TopicAndPartition(topicPartition)
              val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
              val logEndOffset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
              consumer.close()
              LogOffsetResult.LogOffset(logEndOffset)
            }.getOrElse(LogOffsetResult.Ignore)
          case None =>
            printError(s"No broker for partition '$topicPartition'")
            LogOffsetResult.Ignore
        }
      )}.toMap
    }

    private def getPartitionOffsets(group: String,
                                    topicPartitions: Seq[TopicPartition],
                                    channelSocketTimeoutMs: Int,
                                    channelRetryBackoffMs: Int): Map[TopicPartition, Long] = {
      val offsetMap = mutable.Map[TopicAndPartition, Long]()
      val channel = ClientUtils.channelToOffsetManager(group, zkUtils, channelSocketTimeoutMs, channelRetryBackoffMs)
      channel.send(OffsetFetchRequest(group, topicPartitions.map(new TopicAndPartition(_))))
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
          case offsetAndMetaData if offsetAndMetaData.error == Errors.NONE =>
            offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
          case _ =>
            printError(s"Could not fetch offset from kafka for group '$group' partition '$topicAndPartition' due to ${offsetAndMetadata.error.message}.")
        }
      }
      channel.disconnect()
      offsetMap.map { case (topicAndPartition, offset) =>
        (new TopicPartition(topicAndPartition.topic, topicAndPartition.partition), offset)
      }.toMap
    }

    private def deleteGroupsInfo(): Map[String, Errors] = {
      val groups = opts.options.valuesOf(opts.groupOpt)
      groups.asScala.map { group =>
        try {
          if (AdminUtils.deleteConsumerGroupInZK(zkUtils, group)) {
            println(s"Deleted all consumer group information for group '$group' in zookeeper.")
            group -> Errors.NONE
          }
          else {
            printError(s"Delete for group '$group' failed because its consumers are still active.")
            group -> Errors.NON_EMPTY_GROUP
          }
        }
        catch {
          case e: ZkNoNodeException =>
            printError(s"Delete for group '$group' failed because group does not exist.", Some(e))
            group -> Errors.forException(e)
        }
      }.toMap
    }

    private def deleteGroupsInfoForTopic(): Map[String, Errors] = {
      val groups = opts.options.valuesOf(opts.groupOpt)
      val topic = opts.options.valueOf(opts.topicOpt)
      Topic.validate(topic)
      groups.asScala.map { group =>
        try {
          if (AdminUtils.deleteConsumerGroupInfoForTopicInZK(zkUtils, group, topic)) {
            println(s"Deleted consumer group information for group '$group' topic '$topic' in zookeeper.")
            group -> Errors.NONE
          }
          else {
            printError(s"Delete for group '$group' topic '$topic' failed because its consumers are still active.")
            group -> Errors.NON_EMPTY_GROUP
          }
        }
        catch {
          case e: ZkNoNodeException =>
            printError(s"Delete for group '$group' topic '$topic' failed because group does not exist.", Some(e))
            group -> Errors.forException(e)
        }
      }.toMap
    }

    private def deleteAllGroupsInfoForTopic(): Map[String, Errors] = {
      val topic = opts.options.valueOf(opts.topicOpt)
      Topic.validate(topic)
      val deletedGroups = AdminUtils.deleteAllConsumerGroupInfoForTopicInZK(zkUtils, topic)
      println(s"Deleted consumer group information for all inactive consumer groups for topic '$topic' in zookeeper.")
      deletedGroups.map(_ -> Errors.NONE).toMap

    }

    private def getZkConsumer(brokerId: Int): Option[SimpleConsumer] = {
      try {
        zkUtils.getBrokerInfo(brokerId)
          .map(_.brokerEndPoint(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)))
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
    private var consumer: KafkaConsumer[String, String] = _

    override def listGroups(): List[String] = {
      adminClient.listAllConsumerGroupsFlattened().map(_.groupId)
    }

    override def collectGroupOffsets(): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      val group = opts.options.valueOf(opts.groupOpt)
      val consumerGroupSummary = adminClient.describeConsumerGroup(group, opts.options.valueOf(opts.timeoutMsOpt))
      val assignments = consumerGroupSummary.consumers.map { consumers =>
        var assignedTopicPartitions = Array[TopicPartition]()
        val offsets = adminClient.listGroupOffsets(group)
        val rowsWithConsumer =
          if (offsets.isEmpty)
            List[PartitionAssignmentState]()
          else {
            consumers.filter(_.assignment.nonEmpty).sortWith(_.assignment.size > _.assignment.size).flatMap { consumerSummary =>
              val topicPartitions = consumerSummary.assignment
              assignedTopicPartitions = assignedTopicPartitions ++ consumerSummary.assignment
              val partitionOffsets: Map[TopicPartition, Option[Long]] = consumerSummary.assignment.map { topicPartition =>
                new TopicPartition(topicPartition.topic, topicPartition.partition) -> offsets.get(topicPartition)
              }.toMap
              collectConsumerAssignment(group, Some(consumerGroupSummary.coordinator), topicPartitions,
                partitionOffsets, Some(s"${consumerSummary.consumerId}"), Some(s"${consumerSummary.host}"),
                Some(s"${consumerSummary.clientId}"))
            }
          }

        val rowsWithoutConsumer = offsets.filterKeys(!assignedTopicPartitions.contains(_)).flatMap {
          case (topicPartition, offset) =>
            collectConsumerAssignment(group, Some(consumerGroupSummary.coordinator), Seq(topicPartition),
              Map(topicPartition -> Some(offset)), Some(MISSING_COLUMN_VALUE),
              Some(MISSING_COLUMN_VALUE), Some(MISSING_COLUMN_VALUE))
        }

        rowsWithConsumer ++ rowsWithoutConsumer
      }

      (Some(consumerGroupSummary.state), assignments)
    }

    override def collectGroupMembers(verbose: Boolean): (Option[String], Option[Seq[MemberAssignmentState]]) = {
      val group = opts.options.valueOf(opts.groupOpt)
      val consumerGroupSummary = adminClient.describeConsumerGroup(group, opts.options.valueOf(opts.timeoutMsOpt))
      (Some(consumerGroupSummary.state),
        consumerGroupSummary.consumers.map(_.map {
          consumer => MemberAssignmentState(group, consumer.consumerId, consumer.host, consumer.clientId, consumer.assignment.length,
            if (verbose) consumer.assignment else List())
        })
      )
    }

    override def collectGroupState(): GroupState = {
      val group = opts.options.valueOf(opts.groupOpt)
      val consumerGroupSummary = adminClient.describeConsumerGroup(group, opts.options.valueOf(opts.timeoutMsOpt))
      GroupState(group, consumerGroupSummary.coordinator, consumerGroupSummary.assignmentStrategy,
        consumerGroupSummary.state, consumerGroupSummary.consumers.get.size)
    }

    protected def getLogEndOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val offsets = getConsumer.endOffsets(topicPartitions.asJava)
      topicPartitions.map { topicPartition =>
        val logEndOffset = offsets.get(topicPartition)
        topicPartition -> LogOffsetResult.LogOffset(logEndOffset)
      }.toMap
    }

    protected def getLogStartOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val offsets = getConsumer.beginningOffsets(topicPartitions.asJava)
      topicPartitions.map { topicPartition =>
        val logStartOffset = offsets.get(topicPartition)
        topicPartition -> LogOffsetResult.LogOffset(logStartOffset)
      }.toMap
    }

    protected def getLogTimestampOffsets(topicPartitions: Seq[TopicPartition], timestamp: java.lang.Long): Map[TopicPartition, LogOffsetResult] = {
      val consumer = getConsumer
      consumer.assign(topicPartitions.asJava)

      val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
        consumer.offsetsForTimes(topicPartitions.map(_ -> timestamp).toMap.asJava).asScala.partition(_._2 != null)

      val successfulLogTimestampOffsets = successfulOffsetsForTimes.map {
        case (topicPartition, offsetAndTimestamp) => topicPartition -> LogOffsetResult.LogOffset(offsetAndTimestamp.offset)
      }.toMap

      successfulLogTimestampOffsets ++ getLogEndOffsets(unsuccessfulOffsetsForTimes.keySet.toSeq)
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

    private def getConsumer = {
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

    override def resetOffsets(): Map[TopicPartition, OffsetAndMetadata] = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val consumerGroupSummary = adminClient.describeConsumerGroup(groupId, opts.options.valueOf(opts.timeoutMsOpt))
      consumerGroupSummary.state match {
        case "Empty" | "Dead" =>
          val partitionsToReset = getPartitionsToReset(groupId)
          val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)

          // Dry-run is the default behavior if --execute is not specified
          val dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt)
          if (!dryRun)
            getConsumer.commitSync(preparedOffsets.asJava)
          preparedOffsets
        case currentState =>
          printError(s"Assignments can only be reset if the group '$groupId' is inactive, but the current state is $currentState.")
          Map.empty
      }
    }

    private def parseTopicPartitionsToReset(topicArgs: Seq[String]): Seq[TopicPartition] = topicArgs.flatMap {
      case topicArg if topicArg.contains(":") =>
        val topicPartitions = topicArg.split(":")
        val topic = topicPartitions(0)
        topicPartitions(1).split(",").map(partition => new TopicPartition(topic, partition.toInt))
      case topic => getConsumer.partitionsFor(topic).asScala
        .map(partitionInfo => new TopicPartition(topic, partitionInfo.partition))
    }

    private def getPartitionsToReset(groupId: String): Seq[TopicPartition] = {
      if (opts.options.has(opts.allTopicsOpt)) {
        val allTopicPartitions = adminClient.listGroupOffsets(groupId).keys.toSeq
        allTopicPartitions
      } else if (opts.options.has(opts.topicOpt)) {
        val topics = opts.options.valuesOf(opts.topicOpt).asScala
        parseTopicPartitionsToReset(topics)
      } else {
        if (opts.options.has(opts.resetFromFileOpt))
          Nil
        else
          CommandLineUtils.printUsageAndDie(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.")
      }
    }

    private def parseResetPlan(resetPlanCsv: String): Map[TopicPartition, OffsetAndMetadata] = {
      resetPlanCsv.split("\n")
        .map { line =>
          val Array(topic, partition, offset) = line.split(",").map(_.trim)
          val topicPartition = new TopicPartition(topic, partition.toInt)
          val offsetAndMetadata = new OffsetAndMetadata(offset.toLong)
          (topicPartition, offsetAndMetadata)
        }.toMap
    }

    private def prepareOffsetsToReset(groupId: String, partitionsToReset: Seq[TopicPartition]): Map[TopicPartition, OffsetAndMetadata] = {
      if (opts.options.has(opts.resetToOffsetOpt)) {
        val offset = opts.options.valueOf(opts.resetToOffsetOpt)
        checkOffsetsRange(partitionsToReset.map((_, offset)).toMap).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToEarliestOpt)) {
        val logStartOffsets = getLogStartOffsets(partitionsToReset)
        partitionsToReset.map { topicPartition =>
          logStartOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting starting offset of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetToLatestOpt)) {
        val logEndOffsets = getLogEndOffsets(partitionsToReset)
        partitionsToReset.map { topicPartition =>
          logEndOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetShiftByOpt)) {
        val currentCommittedOffsets = adminClient.listGroupOffsets(groupId)
        val requestedOffsets = partitionsToReset.map { topicPartition =>
          val shiftBy = opts.options.valueOf(opts.resetShiftByOpt)
          val currentOffset = currentCommittedOffsets.getOrElse(topicPartition,
            throw new IllegalArgumentException(s"Cannot shift offset for partition $topicPartition since there is no current committed offset"))
          (topicPartition, currentOffset + shiftBy)
        }.toMap
        checkOffsetsRange(requestedOffsets).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToDatetimeOpt)) {
        val timestamp = convertTimestamp(opts.options.valueOf(opts.resetToDatetimeOpt))
        val logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp)
        partitionsToReset.map { topicPartition =>
          val logTimestampOffset = logTimestampOffsets.get(topicPartition)
          logTimestampOffset match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetByDurationOpt)) {
        val duration = opts.options.valueOf(opts.resetByDurationOpt)
        val durationParsed = DatatypeFactory.newInstance().newDuration(duration)
        val now = new Date()
        durationParsed.negate().addTo(now)
        val timestamp = now.getTime
        val logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp)
        partitionsToReset.map { topicPartition =>
          val logTimestampOffset = logTimestampOffsets.get(topicPartition)
          logTimestampOffset match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetFromFileOpt)) {
        val resetPlanPath = opts.options.valueOf(opts.resetFromFileOpt)
        val resetPlanCsv = Utils.readFileAsString(resetPlanPath)
        val resetPlan = parseResetPlan(resetPlanCsv)
        val requestedOffsets = resetPlan.keySet.map { topicPartition =>
          (topicPartition, resetPlan(topicPartition).offset())
        }.toMap
        checkOffsetsRange(requestedOffsets).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToCurrentOpt)) {
        val currentCommittedOffsets = adminClient.listGroupOffsets(groupId)
        val (partitionsToResetWithCommittedOffset, partitionsToResetWithoutCommittedOffset) =
          partitionsToReset.partition(currentCommittedOffsets.keySet.contains(_))

        val preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.map { topicPartition =>
          (topicPartition, new OffsetAndMetadata(currentCommittedOffsets.get(topicPartition) match {
            case Some(offset) => offset
            case _ => throw new IllegalStateException(s"Expected a valid current offset for topic partition: $topicPartition")
          }))
        }.toMap

        val preparedOffsetsForPartitionsWithoutCommittedOffset = getLogEndOffsets(partitionsToResetWithoutCommittedOffset).map {
          case (topicPartition, LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
          case (topicPartition, _) => CommandLineUtils.printUsageAndDie(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
        }

        preparedOffsetsForPartitionsWithCommittedOffset ++ preparedOffsetsForPartitionsWithoutCommittedOffset
      } else {
        CommandLineUtils.printUsageAndDie(opts.parser, "Option '%s' requires one of the following scenarios: %s".format(opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts) )
      }
    }

    private def checkOffsetsRange(requestedOffsets: Map[TopicPartition, Long]) = {
      val logStartOffsets = getLogStartOffsets(requestedOffsets.keySet.toSeq)
      val logEndOffsets = getLogEndOffsets(requestedOffsets.keySet.toSeq)
      requestedOffsets.map { case (topicPartition, offset) => (topicPartition,
        logEndOffsets.get(topicPartition) match {
          case Some(LogOffsetResult.LogOffset(endOffset)) if offset > endOffset =>
            warn(s"New offset ($offset) is higher than latest offset for topic partition $topicPartition. Value will be set to $endOffset")
            endOffset

          case Some(_) => logStartOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(startOffset)) if offset < startOffset =>
              warn(s"New offset ($offset) is lower than earliest offset for topic partition $topicPartition. Value will be set to $startOffset")
              startOffset

            case _ => offset
          }

          case None => // the control should not reach here
            throw new IllegalStateException(s"Unexpected non-existing offset value for topic partition $topicPartition")
        })
      }
    }

    override def exportOffsetsToReset(assignmentsToReset: Map[TopicPartition, OffsetAndMetadata]): String = {
      val rows = assignmentsToReset.map { case (k,v) => s"${k.topic},${k.partition},${v.offset}" }(collection.breakOut): List[String]
      rows.foldRight("")(_ + "\n" + _)
    }

    override def deleteGroups(): Map[String, Errors] = {
      val groupsToDelete = opts.options.valuesOf(opts.groupOpt).asScala.toList
      val result = adminClient.deleteConsumerGroups(groupsToDelete)
      val successfullyDeleted = result.filter {
        case (_, error) => error == Errors.NONE
      }.keySet

      if (successfullyDeleted.size == result.size)
        println(s"Deletion of requested consumer groups (${successfullyDeleted.mkString("'", "', '", "'")}) was successful.")
      else {
        printError("Deletion of some consumer groups failed:")
        result.foreach {
          case (group, error) if error != Errors.NONE => println(s"* Group '$group' could not be deleted due to: ${error.toString}")
          case _ => // no need to print successful deletions individually
        }
        if (successfullyDeleted.nonEmpty)
          println(s"\nThese consumer groups were deleted successfully: ${successfullyDeleted.mkString("'", "', '", "'")}")
      }

      result
    }
  }

  sealed trait LogOffsetResult

  object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  class ConsumerGroupCommandOptions(args: Array[String]) {
    val ZkConnectDoc = "REQUIRED (for consumer groups based on the old consumer): The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over."
    val BootstrapServerDoc = "REQUIRED (for consumer groups based on the new consumer): The server to connect to."
    val GroupDoc = "The consumer group we wish to act on."
    val TopicDoc = "The topic whose consumer group information should be deleted or topic whose should be included in the reset offset process. " +
      "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
      "Reset-offsets also supports multiple topic inputs."
    val AllTopicsDoc = "Consider all topics assigned to a group in the `reset-offsets` process."
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
    val NewConsumerDoc = "Use the new consumer implementation. This is the default, so this option is deprecated and " +
      "will be removed in a future release."
    val TimeoutMsDoc = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
      "to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, " +
      "or is going through some changes)."
    val CommandConfigDoc = "Property file containing configs to be passed to Admin Client and Consumer."
    val ResetOffsetsDoc = "Reset offsets of consumer group. Supports one consumer group at the time, and instances should be inactive" + nl +
      "Has 2 execution options: --dry-run (the default) to plan which offsets to reset, and --execute to update the offsets. " +
      "Additionally, the --export option is used to export the results to a CSV format." + nl +
      "You must choose one of the following reset specifications: --to-datetime, --by-period, --to-earliest, " +
      "--to-latest, --shift-by, --from-file, --to-current." + nl +
      "To define the scope use --all-topics or --topic. One scope must be specified unless you use '--from-file'."
    val DryRunDoc = "Only show results without executing changes on Consumer Groups. Supported operations: reset-offsets."
    val ExecuteDoc = "Execute operation. Supported operations: reset-offsets."
    val ExportDoc = "Export operation execution to a CSV file. Supported operations: reset-offsets."
    val ResetToOffsetDoc = "Reset offsets to a specific offset."
    val ResetFromFileDoc = "Reset offsets to values defined in CSV file."
    val ResetToDatetimeDoc = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'"
    val ResetByDurationDoc = "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'"
    val ResetToEarliestDoc = "Reset offsets to earliest offset."
    val ResetToLatestDoc = "Reset offsets to latest offset."
    val ResetToCurrentDoc = "Reset offsets to current offset."
    val ResetShiftByDoc = "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative."
    val MembersDoc = "Describe members of the group. This option may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --members"
    val VerboseDoc = "Provide additional information, if any, when describing the group. This option may be used " +
      "with '--offsets'/'--members'/'--state' and '--bootstrap-server' options only." + nl + "Example: --bootstrap-server localhost:9092 --describe --group group1 --members --verbose"
    val OffsetsDoc = "Describe the group and list all topic partitions in the group along with their offset lag. " +
      "This is the default sub-action of and may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --offsets"
    val StateDoc = "Describe the group state. This option may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --state"

    val parser = new OptionParser(false)
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
    val allTopicsOpt = parser.accepts("all-topics", AllTopicsDoc)
    val listOpt = parser.accepts("list", ListDoc)
    val describeOpt = parser.accepts("describe", DescribeDoc)
    val deleteOpt = parser.accepts("delete", DeleteDoc)
    val newConsumerOpt = parser.accepts("new-consumer", NewConsumerDoc)
    val timeoutMsOpt = parser.accepts("timeout", TimeoutMsDoc)
                             .withRequiredArg
                             .describedAs("timeout (ms)")
                             .ofType(classOf[Long])
                             .defaultsTo(5000)
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                  .withRequiredArg
                                  .describedAs("command config property file")
                                  .ofType(classOf[String])
    val resetOffsetsOpt = parser.accepts("reset-offsets", ResetOffsetsDoc)
    val dryRunOpt = parser.accepts("dry-run", DryRunDoc)
    val executeOpt = parser.accepts("execute", ExecuteDoc)
    val exportOpt = parser.accepts("export", ExportDoc)
    val resetToOffsetOpt = parser.accepts("to-offset", ResetToOffsetDoc)
                           .withRequiredArg()
                           .describedAs("offset")
                           .ofType(classOf[Long])
    val resetFromFileOpt = parser.accepts("from-file", ResetFromFileDoc)
                                 .withRequiredArg()
                                 .describedAs("path to CSV file")
                                 .ofType(classOf[String])
    val resetToDatetimeOpt = parser.accepts("to-datetime", ResetToDatetimeDoc)
                                   .withRequiredArg()
                                   .describedAs("datetime")
                                   .ofType(classOf[String])
    val resetByDurationOpt = parser.accepts("by-duration", ResetByDurationDoc)
                                   .withRequiredArg()
                                   .describedAs("duration")
                                   .ofType(classOf[String])
    val resetToEarliestOpt = parser.accepts("to-earliest", ResetToEarliestDoc)
    val resetToLatestOpt = parser.accepts("to-latest", ResetToLatestDoc)
    val resetToCurrentOpt = parser.accepts("to-current", ResetToCurrentDoc)
    val resetShiftByOpt = parser.accepts("shift-by", ResetShiftByDoc)
                             .withRequiredArg()
                             .describedAs("number-of-offsets")
                             .ofType(classOf[Long])
    val membersOpt = parser.accepts("members", MembersDoc)
                           .availableIf(describeOpt)
                           .availableUnless(zkConnectOpt)
    val verboseOpt = parser.accepts("verbose", VerboseDoc)
                           .availableIf(describeOpt)
                           .availableUnless(zkConnectOpt)
    val offsetsOpt = parser.accepts("offsets", OffsetsDoc)
                           .availableIf(describeOpt)
                           .availableUnless(zkConnectOpt)
    val stateOpt = parser.accepts("state", StateDoc)
                         .availableIf(describeOpt)
                         .availableUnless(zkConnectOpt)
    parser.mutuallyExclusive(membersOpt, offsetsOpt, stateOpt)

    val options = parser.parse(args : _*)

    val useOldConsumer = options.has(zkConnectOpt)
    val describeOptPresent = options.has(describeOpt)

    val allConsumerGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, describeOpt, deleteOpt, resetOffsetsOpt)
    val allResetOffsetScenarioOpts: Set[OptionSpec[_]] = Set(resetToOffsetOpt, resetShiftByOpt,
      resetToDatetimeOpt, resetByDurationOpt, resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt)

    def checkArgs() {
      // check required args
      if (options.has(timeoutMsOpt) && (!describeOptPresent || useOldConsumer))
        debug(s"Option $timeoutMsOpt is applicable only when both $bootstrapServerOpt and $describeOpt are used.")

      if (useOldConsumer) {
        if (options.has(bootstrapServerOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option $bootstrapServerOpt is not valid with $zkConnectOpt.")
        else if (options.has(newConsumerOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option $newConsumerOpt is not valid with $zkConnectOpt.")
      } else {
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

        if (options.has(newConsumerOpt)) {
          Console.err.println(s"The --new-consumer option is deprecated and will be removed in a future major release. " +
            s"The new consumer is used by default if the --bootstrap-server option is provided.")
        }

        if (options.has(deleteOpt) && options.has(topicOpt))
          CommandLineUtils.printUsageAndDie(parser, s"When deleting a consumer group the option $topicOpt is only " +
            s"valid with $zkConnectOpt. The new consumer does not support topic-specific offset deletion from a consumer group.")
      }

      if (describeOptPresent)
        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)

      if (options.has(deleteOpt) && !options.has(groupOpt) && !options.has(topicOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $deleteOpt either takes $groupOpt, $topicOpt, or both")

      if (options.has(resetOffsetsOpt)) {
        if (options.has(dryRunOpt) && options.has(executeOpt))
          CommandLineUtils.printUsageAndDie(parser, s"Option $resetOffsetsOpt only accepts one of $executeOpt and $dryRunOpt")

        if (!options.has(dryRunOpt) && !options.has(executeOpt)) {
          Console.err.println("WARN: No action will be performed as the --execute option is missing." +
            "In a future major release, the default behavior of this command will be to prompt the user before " +
            "executing the reset rather than doing a dry run. You should add the --dry-run option explicitly " +
            "if you are scripting this command and want to keep the current default behavior without prompting.")
        }

        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToOffsetOpt, allResetOffsetScenarioOpts - resetToOffsetOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToDatetimeOpt, allResetOffsetScenarioOpts - resetToDatetimeOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetByDurationOpt, allResetOffsetScenarioOpts - resetByDurationOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToEarliestOpt, allResetOffsetScenarioOpts - resetToEarliestOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToLatestOpt, allResetOffsetScenarioOpts - resetToLatestOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToCurrentOpt, allResetOffsetScenarioOpts - resetToCurrentOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetShiftByOpt, allResetOffsetScenarioOpts - resetShiftByOpt)
        CommandLineUtils.checkInvalidArgs(parser, options, resetFromFileOpt, allResetOffsetScenarioOpts - resetFromFileOpt)
      }

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, allConsumerGroupLevelOpts - describeOpt - deleteOpt - resetOffsetsOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, allConsumerGroupLevelOpts - deleteOpt - resetOffsetsOpt)
    }
  }
}
