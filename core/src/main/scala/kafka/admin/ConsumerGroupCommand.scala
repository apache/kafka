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
import java.time.{Duration, Instant}
import java.util
import java.util.Properties

import joptsimple.OptionSpec
import kafka.utils._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.{CommonClientConfigs, admin}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{KafkaException, Node, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, Set}
import scala.util.{Failure, Success, Try}

object ConsumerGroupCommand extends Logging {

  def main(args: Array[String]) {
    val opts = new ConsumerGroupCommandOptions(args)

    CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to list all consumer groups, describe a consumer group, delete consumer group info, or reset consumer group offsets.")

    // should have exactly one action
    val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt).count(opts.options.has)
    if (actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets")

    opts.checkArgs()

    val consumerGroupService = new ConsumerGroupService(opts)

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
    e.foreach(_.printStackTrace())
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

  private[admin] case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                                partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                                consumerId: Option[String], host: Option[String],
                                                clientId: Option[String], logEndOffset: Option[Long])

  private[admin] case class MemberAssignmentState(group: String, consumerId: String, host: String, clientId: String,
                                             numPartitions: Int, assignment: List[TopicPartition])

  private[admin] case class GroupState(group: String, coordinator: Node, assignmentStrategy: String, state: String, numMembers: Int)

  class ConsumerGroupService(val opts: ConsumerGroupCommandOptions) {

    private val adminClient = createAdminClient()

    // `consumer` is only needed for `describe`, so we instantiate it lazily
    private var consumer: KafkaConsumer[String, String] = _

    def listGroups(): List[String] = {
      val result = adminClient.listConsumerGroups(
        withTimeoutMs(new ListConsumerGroupsOptions))

        val listings = result.all.get.asScala
        listings.map(_.groupId).toList
    }

    private def shouldPrintMemberState(group: String, state: Option[String], numRows: Option[Int]): Boolean = {
      // numRows contains the number of data rows, if any, compiled from the API call in the caller method.
      // if it's undefined or 0, there is no relevant group information to display.
      numRows match {
        case None =>
          printError(s"The consumer group '$group' does not exist.")
          false
        case Some(num) => state match {
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
              maxHostLen = Math.max(maxHostLen, consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE).length)
            }
        }

        println(s"\n%${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s %${-maxHostLen}s %s"
          .format("TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"))

        assignments match {
          case None => // do nothing
          case Some(consumerAssignments) =>
            consumerAssignments.foreach { consumerAssignment =>
              println(s"%-${maxTopicLen}s %-10s %-15s %-15s %-15s %-${maxConsumerIdLen}s %-${maxHostLen}s %s".format(
                consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.getOrElse(MISSING_COLUMN_VALUE),
                consumerAssignment.offset.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.getOrElse(MISSING_COLUMN_VALUE),
                consumerAssignment.lag.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE),
                consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.getOrElse(MISSING_COLUMN_VALUE)))
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

    private def collectConsumerAssignment(group: String,
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
        case (topicPartition, LogOffsetResult.LogOffset(offset)) => getDescribePartitionResult(topicPartition, Some(offset))
        case (topicPartition, _) => getDescribePartitionResult(topicPartition, None)
      }.toArray
    }

    def resetOffsets(): Map[TopicPartition, OffsetAndMetadata] = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val consumerGroups = adminClient.describeConsumerGroups(
        util.Arrays.asList(groupId),
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups()

      val group = consumerGroups.get(groupId).get
      group.state.toString match {
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

    /**
      * Returns the state of the specified consumer group and partition assignment states
      */
    def collectGroupOffsets(): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val consumerGroup = adminClient.describeConsumerGroups(
        List(groupId).asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions())
      ).describedGroups.get(groupId).get

      val state = consumerGroup.state
      val committedOffsets = getCommittedOffsets(groupId).asScala.toMap
      var assignedTopicPartitions = ListBuffer[TopicPartition]()
      val rowsWithConsumer = consumerGroup.members.asScala.filter(!_.assignment.topicPartitions.isEmpty).toSeq
        .sortWith(_.assignment.topicPartitions.size > _.assignment.topicPartitions.size).flatMap { consumerSummary =>
        val topicPartitions = consumerSummary.assignment.topicPartitions.asScala
        assignedTopicPartitions = assignedTopicPartitions ++ topicPartitions
        val partitionOffsets = consumerSummary.assignment.topicPartitions.asScala
          .map { topicPartition =>
            topicPartition -> committedOffsets.get(topicPartition).map(_.offset)
          }.toMap

        collectConsumerAssignment(groupId, Option(consumerGroup.coordinator), topicPartitions.toList,
          partitionOffsets, Some(s"${consumerSummary.consumerId}"), Some(s"${consumerSummary.host}"),
          Some(s"${consumerSummary.clientId}"))
      }

      val rowsWithoutConsumer = committedOffsets.filterKeys(!assignedTopicPartitions.contains(_)).flatMap {
        case (topicPartition, offset) =>
          collectConsumerAssignment(
            groupId,
            Option(consumerGroup.coordinator),
            Seq(topicPartition),
            Map(topicPartition -> Some(offset.offset)),
                                  Some(MISSING_COLUMN_VALUE),
                                  Some(MISSING_COLUMN_VALUE),
                                  Some(MISSING_COLUMN_VALUE))
      }

      (Some(state.toString), Some(rowsWithConsumer ++ rowsWithoutConsumer))
    }

    private[admin] def collectGroupMembers(verbose: Boolean): (Option[String], Option[Seq[MemberAssignmentState]]) = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val consumerGroups = adminClient.describeConsumerGroups(
        List(groupId).asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups()

      val group = consumerGroups.get(groupId).get
      val state = group.state

      (Some(state.toString),
        Option(group.members().asScala.map {
          consumer => MemberAssignmentState(groupId, consumer.consumerId, consumer.host, consumer.clientId, consumer.assignment.topicPartitions.size(),
            if (verbose) consumer.assignment.topicPartitions.asScala.toList else List())
        }.toList))
    }

    private[admin] def collectGroupState(): GroupState = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val consumerGroups = adminClient.describeConsumerGroups(
        util.Arrays.asList(groupId),
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups()

      val group = consumerGroups.get(groupId).get
      GroupState(groupId, group.coordinator, group.partitionAssignor(),
        group.state.toString, group.members().size)
    }

    private def getLogEndOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val offsets = getConsumer.endOffsets(topicPartitions.asJava)
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(logEndOffset) => topicPartition -> LogOffsetResult.LogOffset(logEndOffset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    private def getLogStartOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val offsets = getConsumer.beginningOffsets(topicPartitions.asJava)
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(logStartOffset) => topicPartition -> LogOffsetResult.LogOffset(logStartOffset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    private def getLogTimestampOffsets(topicPartitions: Seq[TopicPartition], timestamp: java.lang.Long): Map[TopicPartition, LogOffsetResult] = {
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

    private def createAdminClient(): admin.AdminClient = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      admin.AdminClient.create(props)
    }

    private def getConsumer = {
      if (consumer == null)
        consumer = createConsumer()
      consumer
    }

    private def createConsumer(): KafkaConsumer[String, String] = {
      val properties = new Properties()
      val deserializer = (new StringDeserializer).getClass.getName
      val brokerUrl = opts.options.valueOf(opts.bootstrapServerOpt)
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl)
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, opts.options.valueOf(opts.groupOpt))
      properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

      if (opts.options.has(opts.commandConfigOpt)) {
        Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)).asScala.foreach {
          case (k,v) => properties.put(k, v)
        }
      }

      new KafkaConsumer(properties)
    }

    private def withTimeoutMs [T <: AbstractOptions[T]] (options : T) =  {
      val t = opts.options.valueOf(opts.timeoutMsOpt).intValue()
      options.timeoutMs(t)
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
        val allTopicPartitions = getCommittedOffsets(groupId).keySet().asScala.toSeq
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

    private def getCommittedOffsets(groupId: String) = {
      adminClient.listConsumerGroupOffsets(
        groupId,
        withTimeoutMs(new ListConsumerGroupOffsetsOptions)
      ).partitionsToOffsetAndMetadata.get
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
        val currentCommittedOffsets = getCommittedOffsets(groupId)
        val requestedOffsets = partitionsToReset.map { topicPartition =>
          val shiftBy = opts.options.valueOf(opts.resetShiftByOpt)
          val currentOffset = currentCommittedOffsets.asScala.getOrElse(topicPartition,
            throw new IllegalArgumentException(s"Cannot shift offset for partition $topicPartition since there is no current committed offset")).offset
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
        val durationParsed = Duration.parse(duration)
        val now = Instant.now()
        val timestamp = now.minus(durationParsed).toEpochMilli
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
          (topicPartition, resetPlan(topicPartition).offset)
        }.toMap
        checkOffsetsRange(requestedOffsets).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToCurrentOpt)) {
        val currentCommittedOffsets = getCommittedOffsets(groupId)
        val (partitionsToResetWithCommittedOffset, partitionsToResetWithoutCommittedOffset) =
          partitionsToReset.partition(currentCommittedOffsets.keySet.contains(_))

        val preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.map { topicPartition =>
          (topicPartition, new OffsetAndMetadata(currentCommittedOffsets.get(topicPartition) match {
            case offset if offset != null => offset.offset
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

    def exportOffsetsToReset(assignmentsToReset: Map[TopicPartition, OffsetAndMetadata]): String = {
      val rows = assignmentsToReset.map { case (k,v) => s"${k.topic},${k.partition},${v.offset}" }(collection.breakOut): List[String]
      rows.foldRight("")(_ + "\n" + _)
    }

    def deleteGroups(): Map[String, Throwable] = {
      val groupsToDelete = opts.options.valuesOf(opts.groupOpt).asScala.toList
      val deletedGroups = adminClient.deleteConsumerGroups(
        groupsToDelete.asJava,
        withTimeoutMs(new DeleteConsumerGroupsOptions)
      ).deletedGroups().asScala

      val result = deletedGroups.mapValues { f =>
        Try(f.get) match {
          case _: Success[_] => null
          case Failure(e) => e
        }
      }

      val (success, failed) = result.partition {
        case (_, error) => error == null
      }

      if (failed.isEmpty) {
        println(s"Deletion of requested consumer groups (${success.keySet.mkString("'", "', '", "'")}) was successful.")
      }
      else {
        printError("Deletion of some consumer groups failed:")
        failed.foreach {
          case (group, error) => println(s"* Group '$group' could not be deleted due to: ${error.toString}")
        }
        if (success.nonEmpty)
          println(s"\nThese consumer groups were deleted successfully: ${success.keySet.mkString("'", "', '", "'")}")
      }

      result.toMap
    }
  }

  sealed trait LogOffsetResult

  object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  class ConsumerGroupCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val BootstrapServerDoc = "REQUIRED: The server(s) to connect to."
    val GroupDoc = "The consumer group we wish to act on."
    val TopicDoc = "The topic whose consumer group information should be deleted or topic whose should be included in the reset offset process. " +
      "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
      "Reset-offsets also supports multiple topic inputs."
    val AllTopicsDoc = "Consider all topics assigned to a group in the `reset-offsets` process."
    val ListDoc = "List all consumer groups."
    val DescribeDoc = "Describe consumer group and list offset lag (number of messages not yet processed) related to given group."
    val nl = System.getProperty("line.separator")
    val DeleteDoc = "Pass in groups to delete topic partition offsets and ownership information " +
      "over the entire consumer group. For instance --group g1 --group g2"
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
    val verboseOpt = parser.accepts("verbose", VerboseDoc)
                           .availableIf(describeOpt)
    val offsetsOpt = parser.accepts("offsets", OffsetsDoc)
                           .availableIf(describeOpt)
    val stateOpt = parser.accepts("state", StateDoc)
                         .availableIf(describeOpt)

    parser.mutuallyExclusive(membersOpt, offsetsOpt, stateOpt)

    options = parser.parse(args : _*)

    val describeOptPresent = options.has(describeOpt)

    val allConsumerGroupLevelOpts: Set[OptionSpec[_]] = Set(listOpt, describeOpt, deleteOpt, resetOffsetsOpt)
    val allResetOffsetScenarioOpts: Set[OptionSpec[_]] = Set(resetToOffsetOpt, resetShiftByOpt,
      resetToDatetimeOpt, resetByDurationOpt, resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt)

    def checkArgs() {
      // check required args
      if (options.has(timeoutMsOpt) && !describeOptPresent)
        debug(s"Option $timeoutMsOpt is applicable only when $describeOpt is used.")

      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (options.has(deleteOpt) && options.has(topicOpt))
          CommandLineUtils.printUsageAndDie(parser, s"The consumer does not support topic-specific offset " +
            "deletion from a consumer group.")

      if (describeOptPresent)
        CommandLineUtils.checkRequiredArgs(parser, options, groupOpt)

      if (options.has(deleteOpt) && !options.has(groupOpt))
        CommandLineUtils.printUsageAndDie(parser, s"Option $deleteOpt takes $groupOpt")

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
