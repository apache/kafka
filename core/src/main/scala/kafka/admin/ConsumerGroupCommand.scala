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

import com.fasterxml.jackson.databind.{ObjectReader, ObjectWriter}

import java.time.{Duration, Instant}
import java.util.{Collections, Properties}
import com.fasterxml.jackson.dataformat.csv.CsvMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.{ConsumerGroupState, GroupType, KafkaException, Node, TopicPartition}
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Seq, immutable, mutable}
import scala.util.{Failure, Success, Try}
import joptsimple.{OptionException, OptionSpec, OptionSpecBuilder}
import org.apache.kafka.common.protocol.Errors

import scala.collection.immutable.TreeMap
import scala.reflect.ClassTag
import org.apache.kafka.common.requests.ListOffsetsResponse

object ConsumerGroupCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new ConsumerGroupCommandOptions(args)
    try {
      opts.checkArgs()
      CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to list all consumer groups, describe a consumer group, delete consumer group info, or reset consumer group offsets.")

      // should have exactly one action
      val actions = Seq(opts.listOpt, opts.describeOpt, opts.deleteOpt, opts.resetOffsetsOpt, opts.deleteOffsetsOpt).count(opts.options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndExit(opts.parser, "Command must include exactly one action: --list, --describe, --delete, --reset-offsets, --delete-offsets")

      run(opts)
    } catch {
      case e: OptionException =>
        CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage)
    }
  }

  def run(opts: ConsumerGroupCommandOptions): Unit = {
    val consumerGroupService = new ConsumerGroupService(opts)
    try {
      if (opts.options.has(opts.listOpt))
        consumerGroupService.listGroups()
      else if (opts.options.has(opts.describeOpt))
        consumerGroupService.describeGroups()
      else if (opts.options.has(opts.deleteOpt))
        consumerGroupService.deleteGroups()
      else if (opts.options.has(opts.resetOffsetsOpt)) {
        val offsetsToReset = consumerGroupService.resetOffsets()
        if (opts.options.has(opts.exportOpt)) {
          val exported = consumerGroupService.exportOffsetsToCsv(offsetsToReset)
          println(exported)
        } else
          printOffsetsToReset(offsetsToReset)
      }
      else if (opts.options.has(opts.deleteOffsetsOpt)) {
        consumerGroupService.deleteOffsets()
      }
    } catch {
      case e: IllegalArgumentException =>
        CommandLineUtils.printUsageAndExit(opts.parser, e.getMessage)
      case e: Throwable =>
        printError(s"Executing consumer group command failed due to ${e.getMessage}", Some(e))
    } finally {
      consumerGroupService.close()
    }
  }

  def consumerGroupStatesFromString(input: String): Set[ConsumerGroupState] = {
    val parsedStates = input.split(',').map(s => ConsumerGroupState.parse(s.trim)).toSet
    if (parsedStates.contains(ConsumerGroupState.UNKNOWN)) {
      val validStates = ConsumerGroupState.values().filter(_ != ConsumerGroupState.UNKNOWN)
      throw new IllegalArgumentException(s"Invalid state list '$input'. Valid states are: ${validStates.mkString(", ")}")
    }
    parsedStates
  }

  def consumerGroupTypesFromString(input: String): Set[GroupType] = {
    val parsedTypes = input.toLowerCase.split(',').map(s => GroupType.parse(s.trim)).toSet
    if (parsedTypes.contains(GroupType.UNKNOWN)) {
      val validTypes = GroupType.values().filter(_ != GroupType.UNKNOWN)
      throw new IllegalArgumentException(s"Invalid types list '$input'. Valid types are: ${validTypes.mkString(", ")}")
    }
    parsedTypes
  }

  val MISSING_COLUMN_VALUE = "-"

  private def printError(msg: String, e: Option[Throwable] = None): Unit = {
    println(s"\nError: $msg")
    e.foreach(_.printStackTrace())
  }

  private def printOffsetsToReset(groupAssignmentsToReset: Map[String, Map[TopicPartition, OffsetAndMetadata]]): Unit = {
    val format = "%-30s %-30s %-10s %-15s"
    if (groupAssignmentsToReset.nonEmpty)
      println("\n" + format.format("GROUP", "TOPIC", "PARTITION", "NEW-OFFSET"))
    for {
      (groupId, assignment) <- groupAssignmentsToReset
      (consumerAssignment, offsetAndMetadata) <- assignment
    } {
      println(format.format(
        groupId,
        consumerAssignment.topic,
        consumerAssignment.partition,
        offsetAndMetadata.offset))
    }
  }

  case class PartitionAssignmentState(group: String, coordinator: Option[Node], topic: Option[String],
                                                partition: Option[Int], offset: Option[Long], lag: Option[Long],
                                                consumerId: Option[String], host: Option[String],
                                                clientId: Option[String], logEndOffset: Option[Long])

  private[admin] case class MemberAssignmentState(group: String, consumerId: String, host: String, clientId: String, groupInstanceId: String,
                                             numPartitions: Int, assignment: List[TopicPartition])

  private[admin] case class GroupState(group: String, coordinator: Node, assignmentStrategy: String, state: String, numMembers: Int)

  private[admin] sealed trait CsvRecord
  private[admin] case class CsvRecordWithGroup(group: String, topic: String, partition: Int, offset: Long) extends CsvRecord
  private[admin] case class CsvRecordNoGroup(topic: String, partition: Int, offset: Long) extends CsvRecord
  private[admin] object CsvRecordWithGroup {
    val fields: Array[String] = Array("group", "topic", "partition", "offset")
  }
  private[admin] object CsvRecordNoGroup {
    val fields: Array[String] = Array("topic", "partition", "offset")
  }
  // Example: CsvUtils().readerFor[CsvRecordWithoutGroup]
  private[admin] case class CsvUtils() {
    val mapper = new CsvMapper
    mapper.registerModule(DefaultScalaModule)
    def readerFor[T <: CsvRecord : ClassTag]: ObjectReader = {
      val schema = getSchema[T]
      val clazz = implicitly[ClassTag[T]].runtimeClass
      mapper.readerFor(clazz).`with`(schema)
    }
    def writerFor[T <: CsvRecord : ClassTag]: ObjectWriter = {
      val schema = getSchema[T]
      val clazz = implicitly[ClassTag[T]].runtimeClass
      mapper.writerFor(clazz).`with`(schema)
    }
    private def getSchema[T <: CsvRecord : ClassTag] = {
      val clazz = implicitly[ClassTag[T]].runtimeClass

      val fields =
        if (classOf[CsvRecordWithGroup] == clazz) CsvRecordWithGroup.fields
        else if (classOf[CsvRecordNoGroup] == clazz) CsvRecordNoGroup.fields
        else throw new IllegalStateException(s"Unhandled class $clazz")

      val schema = mapper.schemaFor(clazz).sortedBy(fields: _*)
      schema
    }
  }

  class ConsumerGroupService(val opts: ConsumerGroupCommandOptions,
                             private[admin] val configOverrides: Map[String, String] = Map.empty) {

    private val adminClient = createAdminClient(configOverrides)

    // We have to make sure it is evaluated once and available
    private lazy val resetPlanFromFile: Option[Map[String, Map[TopicPartition, OffsetAndMetadata]]] = {
      if (opts.options.has(opts.resetFromFileOpt)) {
        val resetPlanPath = opts.options.valueOf(opts.resetFromFileOpt)
        val resetPlanCsv = Utils.readFileAsString(resetPlanPath)
        val resetPlan = parseResetPlan(resetPlanCsv)
        Some(resetPlan)
      } else None
    }

    def listGroups(): Unit = {
      val includeType = opts.options.has(opts.typeOpt)
      val includeState = opts.options.has(opts.stateOpt)

      if (includeType || includeState) {
        val types = typeValues()
        val states = stateValues()
        val listings = listConsumerGroupsWithFilters(types, states)

        printGroupInfo(listings, includeType, includeState)

      } else {
        listConsumerGroups().foreach(println(_))
      }
    }

    private def stateValues(): Set[ConsumerGroupState] = {
      val stateValue = opts.options.valueOf(opts.stateOpt)
      if (stateValue == null || stateValue.isEmpty)
        Set[ConsumerGroupState]()
      else
        consumerGroupStatesFromString(stateValue)
    }

    private def typeValues(): Set[GroupType] = {
      val typeValue = opts.options.valueOf(opts.typeOpt)
      if (typeValue == null || typeValue.isEmpty)
        Set[GroupType]()
      else
        consumerGroupTypesFromString(typeValue)
    }

    private def printGroupInfo(groups: List[ConsumerGroupListing], includeType: Boolean, includeState: Boolean): Unit = {
      def groupId(groupListing: ConsumerGroupListing): String = groupListing.groupId
      def groupType(groupListing: ConsumerGroupListing): String = groupListing.`type`().orElse(GroupType.UNKNOWN).toString
      def groupState(groupListing: ConsumerGroupListing): String = groupListing.state.orElse(ConsumerGroupState.UNKNOWN).toString

      val maxGroupLen = groups.foldLeft(15)((maxLen, groupListing) => Math.max(maxLen, groupId(groupListing).length)) + 10
      var format = s"%-${maxGroupLen}s"
      var header = List("GROUP")
      var extractors: List[ConsumerGroupListing => String] = List(groupId)

      if (includeType) {
        header = header :+ "TYPE"
        extractors = extractors :+ groupType _
        format += " %-20s"
      }

      if (includeState) {
        header = header :+ "STATE"
        extractors = extractors :+ groupState _
        format += " %-20s"
      }

      println(format.format(header: _*))

      groups.foreach { groupListing =>
        val info = extractors.map(extractor => extractor(groupListing))
        println(format.format(info: _*))
      }
    }

    def listConsumerGroups(): List[String] = {
      val result = adminClient.listConsumerGroups(withTimeoutMs(new ListConsumerGroupsOptions))
      val listings = result.all.get.asScala
      listings.map(_.groupId).toList
    }

    def listConsumerGroupsWithFilters(types: Set[GroupType], states: Set[ConsumerGroupState]): List[ConsumerGroupListing] = {
      val listConsumerGroupsOptions = withTimeoutMs(new ListConsumerGroupsOptions())
      listConsumerGroupsOptions
        .inStates(states.asJava)
        .withTypes(types.asJava)
      val result = adminClient.listConsumerGroups(listConsumerGroupsOptions)
      result.all.get.asScala.toList
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
            Console.err.println(s"\nConsumer group '$group' has no active members.")
          case Some("PreparingRebalance") | Some("CompletingRebalance") | Some("Assigning") | Some("Reconciling") =>
            Console.err.println(s"\nWarning: Consumer group '$group' is rebalancing.")
          case Some("Stable") =>
          case other =>
            // the control should never reach here
            throw new KafkaException(s"Expected a valid consumer group state, but found '${other.getOrElse("NONE")}'.")
        }
        !state.contains("Dead") && num > 0
      }
    }

    private def size(colOpt: Option[Seq[Object]]): Option[Int] = colOpt.map(_.size)

    private def printOffsets(offsets: Map[String, (Option[String], Option[Seq[PartitionAssignmentState]])]): Unit = {
      for ((groupId, (state, assignments)) <- offsets) {
        if (shouldPrintMemberState(groupId, state, size(assignments))) {
          // find proper columns width
          var (maxGroupLen, maxTopicLen, maxConsumerIdLen, maxHostLen) = (15, 15, 15, 15)
          assignments match {
            case None => // do nothing
            case Some(consumerAssignments) =>
              consumerAssignments.foreach { consumerAssignment =>
                maxGroupLen = Math.max(maxGroupLen, consumerAssignment.group.length)
                maxTopicLen = Math.max(maxTopicLen, consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE).length)
                maxConsumerIdLen = Math.max(maxConsumerIdLen, consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE).length)
                maxHostLen = Math.max(maxHostLen, consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE).length)
              }
          }

          val format = s"%${-maxGroupLen}s %${-maxTopicLen}s %-10s %-15s %-15s %-15s %${-maxConsumerIdLen}s %${-maxHostLen}s %s"
          println("\n" + format
            .format("GROUP", "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG", "CONSUMER-ID", "HOST", "CLIENT-ID"))

          assignments match {
            case None => // do nothing
            case Some(consumerAssignments) =>
              consumerAssignments.foreach { consumerAssignment =>
                println(format.format(
                  consumerAssignment.group,
                  consumerAssignment.topic.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.partition.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.offset.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.logEndOffset.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.lag.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.consumerId.getOrElse(MISSING_COLUMN_VALUE),
                  consumerAssignment.host.getOrElse(MISSING_COLUMN_VALUE), consumerAssignment.clientId.getOrElse(MISSING_COLUMN_VALUE))
                )
              }
          }
        }
      }
    }

    private def printMembers(members: Map[String, (Option[String], Option[Seq[MemberAssignmentState]])], verbose: Boolean): Unit = {
      for ((groupId, (state, assignments)) <- members) {
        if (shouldPrintMemberState(groupId, state, size(assignments))) {
          // find proper columns width
          var (maxGroupLen, maxConsumerIdLen, maxGroupInstanceIdLen, maxHostLen, maxClientIdLen, includeGroupInstanceId) = (15, 15, 17, 15, 15, false)
          assignments match {
            case None => // do nothing
            case Some(memberAssignments) =>
              memberAssignments.foreach { memberAssignment =>
                maxGroupLen = Math.max(maxGroupLen, memberAssignment.group.length)
                maxConsumerIdLen = Math.max(maxConsumerIdLen, memberAssignment.consumerId.length)
                maxGroupInstanceIdLen =  Math.max(maxGroupInstanceIdLen, memberAssignment.groupInstanceId.length)
                maxHostLen = Math.max(maxHostLen, memberAssignment.host.length)
                maxClientIdLen = Math.max(maxClientIdLen, memberAssignment.clientId.length)
                includeGroupInstanceId = includeGroupInstanceId || memberAssignment.groupInstanceId.nonEmpty
              }
          }

          val wideFormat = s"%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxGroupInstanceIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "
          val shortFormat = s"%${-maxGroupLen}s %${-maxConsumerIdLen}s %${-maxHostLen}s %${-maxClientIdLen}s %-15s "

          if (includeGroupInstanceId) {
            print("\n" + wideFormat
                .format("GROUP", "CONSUMER-ID", "GROUP-INSTANCE-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
          } else {
            print("\n" + shortFormat
                .format("GROUP", "CONSUMER-ID", "HOST", "CLIENT-ID", "#PARTITIONS"))
          }
          if (verbose)
            print(s"%s".format("ASSIGNMENT"))
          println()

          assignments match {
            case None => // do nothing
            case Some(memberAssignments) =>
              memberAssignments.foreach { memberAssignment =>
                if (includeGroupInstanceId) {
                  print(wideFormat.format(
                    memberAssignment.group, memberAssignment.consumerId, memberAssignment.groupInstanceId, memberAssignment.host,
                    memberAssignment.clientId, memberAssignment.numPartitions))
                } else {
                  print(shortFormat.format(
                    memberAssignment.group, memberAssignment.consumerId, memberAssignment.host, memberAssignment.clientId, memberAssignment.numPartitions))
                }
                if (verbose) {
                  val partitions = memberAssignment.assignment match {
                    case List() => MISSING_COLUMN_VALUE
                    case assignment =>
                      assignment.groupBy(_.topic).map {
                        case (topic, partitionList) => topic + partitionList.map(_.partition).sorted.mkString("(", ",", ")")
                      }.toList.sorted.mkString(", ")
                  }
                  print(s"%s".format(partitions))
                }
                println()
              }
          }
        }
      }
    }

    private def printStates(states: Map[String, GroupState]): Unit = {
      for ((groupId, state) <- states) {
        if (shouldPrintMemberState(groupId, Some(state.state), Some(1))) {
          val coordinator = s"${state.coordinator.host}:${state.coordinator.port} (${state.coordinator.idString})"
          val coordinatorColLen = Math.max(25, coordinator.length)
          val format = s"\n%${-coordinatorColLen}s %-25s %-20s %-15s %s"
          print(format.format("GROUP", "COORDINATOR (ID)", "ASSIGNMENT-STRATEGY", "STATE", "#MEMBERS"))
          print(format.format(state.group, coordinator, state.assignmentStrategy, state.state, state.numMembers))
          println()
        }
      }
    }

    def describeGroups(): Unit = {
      val groupIds =
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
        else opts.options.valuesOf(opts.groupOpt).asScala
      val membersOptPresent = opts.options.has(opts.membersOpt)
      val stateOptPresent = opts.options.has(opts.stateOpt)
      val offsetsOptPresent = opts.options.has(opts.offsetsOpt)
      val subActions = Seq(membersOptPresent, offsetsOptPresent, stateOptPresent).count(_ == true)

      if (subActions == 0 || offsetsOptPresent) {
        val offsets = collectGroupsOffsets(groupIds)
        printOffsets(offsets)
      } else if (membersOptPresent) {
        val members = collectGroupsMembers(groupIds, opts.options.has(opts.verboseOpt))
        printMembers(members, opts.options.has(opts.verboseOpt))
      } else {
        val states = collectGroupsState(groupIds)
        printStates(states)
      }
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
        logEndOffsetResult =>
          logEndOffsetResult._2 match {
            case LogOffsetResult.LogOffset(logEndOffset) => getDescribePartitionResult(logEndOffsetResult._1, Some(logEndOffset))
            case LogOffsetResult.Unknown => getDescribePartitionResult(logEndOffsetResult._1, None)
            case LogOffsetResult.Ignore => null
          }
      }.toArray
    }

    def resetOffsets(): Map[String, Map[TopicPartition, OffsetAndMetadata]] = {
      val groupIds =
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
        else opts.options.valuesOf(opts.groupOpt).asScala

      val consumerGroups = adminClient.describeConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups()

      val result =
        consumerGroups.asScala.foldLeft(immutable.Map[String, Map[TopicPartition, OffsetAndMetadata]]()) {
          case (acc, (groupId, groupDescription)) =>
            groupDescription.get.state().toString match {
              case "Empty" | "Dead" =>
                val partitionsToReset = getPartitionsToReset(groupId)
                val preparedOffsets = prepareOffsetsToReset(groupId, partitionsToReset)

                // Dry-run is the default behavior if --execute is not specified
                val dryRun = opts.options.has(opts.dryRunOpt) || !opts.options.has(opts.executeOpt)
                if (!dryRun) {
                  adminClient.alterConsumerGroupOffsets(
                    groupId,
                    preparedOffsets.asJava,
                    withTimeoutMs(new AlterConsumerGroupOffsetsOptions)
                  ).all.get
                }
                acc.updated(groupId, preparedOffsets)
              case currentState =>
                printError(s"Assignments can only be reset if the group '$groupId' is inactive, but the current state is $currentState.")
                acc.updated(groupId, Map.empty)
            }
        }
      result
    }

    def deleteOffsets(groupId: String, topics: List[String]): (Errors, Map[TopicPartition, Throwable]) = {
      val partitionLevelResult = mutable.Map[TopicPartition, Throwable]()

      val (topicWithPartitions, topicWithoutPartitions) = topics.partition(_.contains(":"))
      val knownPartitions = topicWithPartitions.flatMap(parseTopicsWithPartitions)

      // Get the partitions of topics that the user did not explicitly specify the partitions
      val describeTopicsResult = adminClient.describeTopics(
        topicWithoutPartitions.asJava,
        withTimeoutMs(new DescribeTopicsOptions))

      val unknownPartitions = describeTopicsResult.topicNameValues().asScala.flatMap { case (topic, future) =>
        Try(future.get()) match {
          case Success(description) => description.partitions().asScala.map { partition =>
            new TopicPartition(topic, partition.partition())
          }
          case Failure(e) =>
            partitionLevelResult += new TopicPartition(topic, -1) -> e
            List.empty
        }
      }

      val partitions = knownPartitions ++ unknownPartitions

      val deleteResult = adminClient.deleteConsumerGroupOffsets(
        groupId,
        partitions.toSet.asJava,
        withTimeoutMs(new DeleteConsumerGroupOffsetsOptions)
      )

      var topLevelException = Errors.NONE
      Try(deleteResult.all.get) match {
        case Success(_) =>
        case Failure(e) => topLevelException = Errors.forException(e.getCause)
      }

      partitions.foreach { partition =>
        Try(deleteResult.partitionResult(partition).get()) match {
          case Success(_) => partitionLevelResult += partition -> null
          case Failure(e) => partitionLevelResult += partition -> e
        }
      }

      (topLevelException, partitionLevelResult)
    }

    def deleteOffsets(): Unit = {
      val groupId = opts.options.valueOf(opts.groupOpt)
      val topics = opts.options.valuesOf(opts.topicOpt).asScala.toList

      val (topLevelResult, partitionLevelResult) = deleteOffsets(groupId, topics)

      topLevelResult match {
        case Errors.NONE =>
          println(s"Request succeed for deleting offsets with topic ${topics.mkString(", ")} group $groupId")
        case Errors.INVALID_GROUP_ID =>
          printError(s"'$groupId' is not valid.")
        case Errors.GROUP_ID_NOT_FOUND =>
          printError(s"'$groupId' does not exist.")
        case Errors.GROUP_AUTHORIZATION_FAILED =>
          printError(s"Access to '$groupId' is not authorized.")
        case Errors.NON_EMPTY_GROUP =>
          printError(s"Deleting offsets of a consumer group '$groupId' is forbidden if the group is not empty.")
        case Errors.GROUP_SUBSCRIBED_TO_TOPIC |
             Errors.TOPIC_AUTHORIZATION_FAILED |
             Errors.UNKNOWN_TOPIC_OR_PARTITION =>
          printError(s"Encounter some partition level error, see the follow-up details:")
        case _ =>
          printError(s"Encounter some unknown error: $topLevelResult")
      }

      val format = "%-30s %-15s %-15s"
      println("\n" + format.format("TOPIC", "PARTITION", "STATUS"))
      partitionLevelResult.toList.sortBy(t => t._1.topic + t._1.partition.toString).foreach { case (tp, error) =>
        println(format.format(
          tp.topic,
          if (tp.partition >= 0) tp.partition else "Not Provided",
          if (error != null) s"Error: ${error.getMessage}" else "Successful"
        ))
      }
    }

    private[admin] def describeConsumerGroups(groupIds: Seq[String]): mutable.Map[String, ConsumerGroupDescription] = {
      adminClient.describeConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DescribeConsumerGroupsOptions)
      ).describedGroups().asScala.map {
        case (groupId, groupDescriptionFuture) => (groupId, groupDescriptionFuture.get())
      }
    }

    /**
      * Returns the state of the specified consumer group and partition assignment states
      */
    def collectGroupOffsets(groupId: String): (Option[String], Option[Seq[PartitionAssignmentState]]) = {
      collectGroupsOffsets(List(groupId)).getOrElse(groupId, (None, None))
    }

    /**
      * Returns states of the specified consumer groups and partition assignment states
      */
    private def collectGroupsOffsets(groupIds: Seq[String]): TreeMap[String, (Option[String], Option[Seq[PartitionAssignmentState]])] = {
      val consumerGroups = describeConsumerGroups(groupIds)

      val groupOffsets = TreeMap[String, (Option[String], Option[Seq[PartitionAssignmentState]])]() ++ (for ((groupId, consumerGroup) <- consumerGroups) yield {
        val state = consumerGroup.state
        val committedOffsets = getCommittedOffsets(groupId)
        // The admin client returns `null` as a value to indicate that there is not committed offset for a partition.
        def getPartitionOffset(tp: TopicPartition): Option[Long] = committedOffsets.get(tp).filter(_ != null).map(_.offset)
        var assignedTopicPartitions = ListBuffer[TopicPartition]()
        val rowsWithConsumer = consumerGroup.members.asScala.filterNot(_.assignment.topicPartitions.isEmpty).toSeq
          .sortBy(_.assignment.topicPartitions.size)(Ordering[Int].reverse).flatMap { consumerSummary =>
          val topicPartitions = consumerSummary.assignment.topicPartitions.asScala
          assignedTopicPartitions = assignedTopicPartitions ++ topicPartitions
          collectConsumerAssignment(groupId, Option(consumerGroup.coordinator), topicPartitions.toList,
            getPartitionOffset, Some(s"${consumerSummary.consumerId}"), Some(s"${consumerSummary.host}"),
            Some(s"${consumerSummary.clientId}"))
        }
        val unassignedPartitions = committedOffsets.filterNot { case (tp, _) => assignedTopicPartitions.contains(tp) }
        val rowsWithoutConsumer = if (unassignedPartitions.nonEmpty) {
          collectConsumerAssignment(
            groupId,
            Option(consumerGroup.coordinator),
            unassignedPartitions.keySet.toSeq,
            getPartitionOffset,
            Some(MISSING_COLUMN_VALUE),
            Some(MISSING_COLUMN_VALUE),
            Some(MISSING_COLUMN_VALUE)).toSeq
        } else
          Seq.empty

        groupId -> (Some(state.toString), Some(rowsWithConsumer ++ rowsWithoutConsumer))
      }).toMap

      groupOffsets
    }

    private[admin] def collectGroupMembers(groupId: String, verbose: Boolean): (Option[String], Option[Seq[MemberAssignmentState]]) = {
      collectGroupsMembers(Seq(groupId), verbose)(groupId)
    }

    private[admin] def collectGroupsMembers(groupIds: Seq[String], verbose: Boolean): TreeMap[String, (Option[String], Option[Seq[MemberAssignmentState]])] = {
      val consumerGroups = describeConsumerGroups(groupIds)
      TreeMap[String, (Option[String], Option[Seq[MemberAssignmentState]])]() ++ (for ((groupId, consumerGroup) <- consumerGroups) yield {
        val state = consumerGroup.state.toString
        val memberAssignmentStates = consumerGroup.members().asScala.map(consumer =>
          MemberAssignmentState(
            groupId,
            consumer.consumerId,
            consumer.host,
            consumer.clientId,
            consumer.groupInstanceId.orElse(""),
            consumer.assignment.topicPartitions.size(),
            if (verbose) consumer.assignment.topicPartitions.asScala.toList else List()
          )).toList
        groupId -> (Some(state), Option(memberAssignmentStates))
      }).toMap
    }

    private[admin] def collectGroupState(groupId: String): GroupState = {
      collectGroupsState(Seq(groupId))(groupId)
    }

    private[admin] def collectGroupsState(groupIds: Seq[String]): TreeMap[String, GroupState] = {
      val consumerGroups = describeConsumerGroups(groupIds)
      TreeMap[String, GroupState]() ++ (for ((groupId, groupDescription) <- consumerGroups) yield {
        groupId -> GroupState(
          groupId,
          groupDescription.coordinator,
          groupDescription.partitionAssignor(),
          groupDescription.state.toString,
          groupDescription.members().size
        )
      }).toMap
    }

    private def getLogEndOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val endOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.latest
      }.toMap
      val offsets = adminClient.listOffsets(
        endOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    private def getLogStartOffsets(topicPartitions: Seq[TopicPartition]): Map[TopicPartition, LogOffsetResult] = {
      val startOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.earliest
      }.toMap
      val offsets = adminClient.listOffsets(
        startOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      topicPartitions.map { topicPartition =>
        Option(offsets.get(topicPartition)) match {
          case Some(listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
          case _ => topicPartition -> LogOffsetResult.Unknown
        }
      }.toMap
    }

    private def getLogTimestampOffsets(topicPartitions: Seq[TopicPartition], timestamp: java.lang.Long): Map[TopicPartition, LogOffsetResult] = {
      val timestampOffsets = topicPartitions.map { topicPartition =>
        topicPartition -> OffsetSpec.forTimestamp(timestamp)
      }.toMap
      val offsets = adminClient.listOffsets(
        timestampOffsets.asJava,
        withTimeoutMs(new ListOffsetsOptions)
      ).all.get
      val (successfulOffsetsForTimes, unsuccessfulOffsetsForTimes) =
        offsets.asScala.partition(_._2.offset != ListOffsetsResponse.UNKNOWN_OFFSET)

      val successfulLogTimestampOffsets = successfulOffsetsForTimes.map {
        case (topicPartition, listOffsetsResultInfo) => topicPartition -> LogOffsetResult.LogOffset(listOffsetsResultInfo.offset)
      }.toMap

      unsuccessfulOffsetsForTimes.foreach { entry =>
        println(s"\nWarn: Partition " + entry._1.partition() + " from topic " + entry._1.topic() +
          " is empty. Falling back to latest known offset.")
      }

      successfulLogTimestampOffsets ++ getLogEndOffsets(unsuccessfulOffsetsForTimes.keySet.toSeq)
    }

    def close(): Unit = {
      adminClient.close()
    }

    // Visibility for testing
    protected def createAdminClient(configOverrides: Map[String, String]): Admin = {
      val props = if (opts.options.has(opts.commandConfigOpt)) Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)) else new Properties()
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      configOverrides.forKeyValue { (k, v) => props.put(k, v)}
      Admin.create(props)
    }

    private def withTimeoutMs [T <: AbstractOptions[T]] (options : T) =  {
      val t = opts.options.valueOf(opts.timeoutMsOpt).intValue()
      options.timeoutMs(t)
    }

    private def parseTopicsWithPartitions(topicArg: String): Seq[TopicPartition] = {
      def partitionNum(partition: String): Int = {
        try {
          partition.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"Invalid partition '$partition' specified in topic arg '$topicArg''")
        }
      }
      topicArg.split(":") match {
        case Array(topic, partitions) =>
          partitions.split(",").map(partition => new TopicPartition(topic, partitionNum(partition)))
        case _ =>
          throw new IllegalArgumentException(s"Invalid topic arg '$topicArg', expected topic name and partitions")
      }
    }

    private def parseTopicPartitionsToReset(topicArgs: Seq[String]): Seq[TopicPartition] = {
      val (topicsWithPartitions, topics) = topicArgs.partition(_.contains(":"))
      val specifiedPartitions = topicsWithPartitions.flatMap(parseTopicsWithPartitions)

      val unspecifiedPartitions = if (topics.nonEmpty) {
        val descriptionMap = adminClient.describeTopics(
          topics.asJava,
          withTimeoutMs(new DescribeTopicsOptions)
        ).allTopicNames().get.asScala
        descriptionMap.flatMap { case (topic, description) =>
          description.partitions().asScala.map { tpInfo =>
            new TopicPartition(topic, tpInfo.partition)
          }
        }
      } else
        Seq.empty
      specifiedPartitions ++ unspecifiedPartitions
    }

    private def getPartitionsToReset(groupId: String): Seq[TopicPartition] = {
      if (opts.options.has(opts.allTopicsOpt)) {
        getCommittedOffsets(groupId).keys.toSeq
      } else if (opts.options.has(opts.topicOpt)) {
        val topics = opts.options.valuesOf(opts.topicOpt).asScala
        parseTopicPartitionsToReset(topics)
      } else {
        if (opts.options.has(opts.resetFromFileOpt))
          Nil
        else
          ToolsUtils.printUsageAndExit(opts.parser, "One of the reset scopes should be defined: --all-topics, --topic.")
      }
    }

    private def getCommittedOffsets(groupId: String): Map[TopicPartition, OffsetAndMetadata] = {
      adminClient.listConsumerGroupOffsets(
        Collections.singletonMap(groupId, new ListConsumerGroupOffsetsSpec),
        withTimeoutMs(new ListConsumerGroupOffsetsOptions())
      ).partitionsToOffsetAndMetadata(groupId).get().asScala
    }

    type GroupMetadata = immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]
    private def parseResetPlan(resetPlanCsv: String): GroupMetadata = {
      def updateGroupMetadata(group: String, topic: String, partition: Int, offset: Long, acc: GroupMetadata) = {
        val topicPartition = new TopicPartition(topic, partition)
        val offsetAndMetadata = new OffsetAndMetadata(offset)
        val dataMap = acc.getOrElse(group, immutable.Map()).updated(topicPartition, offsetAndMetadata)
        acc.updated(group, dataMap)
      }
      val csvReader = CsvUtils().readerFor[CsvRecordNoGroup]
      val lines = resetPlanCsv.split("\n")
      val isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1
      val isOldCsvFormat = lines.headOption.flatMap(line =>
        Try(csvReader.readValue[CsvRecordNoGroup](line)).toOption).nonEmpty
      // Single group CSV format: "topic,partition,offset"
      val dataMap = if (isSingleGroupQuery && isOldCsvFormat) {
        val group = opts.options.valueOf(opts.groupOpt)
        lines.foldLeft(immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]()) { (acc, line) =>
          val CsvRecordNoGroup(topic, partition, offset) = csvReader.readValue[CsvRecordNoGroup](line)
          updateGroupMetadata(group, topic, partition, offset, acc)
        }
        // Multiple group CSV format: "group,topic,partition,offset"
      } else {
        val csvReader = CsvUtils().readerFor[CsvRecordWithGroup]
        lines.foldLeft(immutable.Map[String, immutable.Map[TopicPartition, OffsetAndMetadata]]()) { (acc, line) =>
          val CsvRecordWithGroup(group, topic, partition, offset) = csvReader.readValue[CsvRecordWithGroup](line)
          updateGroupMetadata(group, topic, partition, offset, acc)
        }
      }
      dataMap
    }

    private def prepareOffsetsToReset(groupId: String,
                                      partitionsToReset: Seq[TopicPartition]): Map[TopicPartition, OffsetAndMetadata] = {
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
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting starting offset of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetToLatestOpt)) {
        val logEndOffsets = getLogEndOffsets(partitionsToReset)
        partitionsToReset.map { topicPartition =>
          logEndOffsets.get(topicPartition) match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetShiftByOpt)) {
        val currentCommittedOffsets = getCommittedOffsets(groupId)
        val requestedOffsets = partitionsToReset.map { topicPartition =>
          val shiftBy = opts.options.valueOf(opts.resetShiftByOpt)
          val currentOffset = currentCommittedOffsets.getOrElse(topicPartition,
            throw new IllegalArgumentException(s"Cannot shift offset for partition $topicPartition since there is no current committed offset")).offset
          (topicPartition, currentOffset + shiftBy)
        }.toMap
        checkOffsetsRange(requestedOffsets).map {
          case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
        }
      } else if (opts.options.has(opts.resetToDatetimeOpt)) {
        val timestamp = Utils.getDateTime(opts.options.valueOf(opts.resetToDatetimeOpt))
        val logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp)
        partitionsToReset.map { topicPartition =>
          val logTimestampOffset = logTimestampOffsets.get(topicPartition)
          logTimestampOffset match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
          }
        }.toMap
      } else if (opts.options.has(opts.resetByDurationOpt)) {
        val duration = opts.options.valueOf(opts.resetByDurationOpt)
        val durationParsed = Duration.parse(duration)
        val now = Instant.now()
        durationParsed.negated().addTo(now)
        val timestamp = now.minus(durationParsed).toEpochMilli
        val logTimestampOffsets = getLogTimestampOffsets(partitionsToReset, timestamp)
        partitionsToReset.map { topicPartition =>
          val logTimestampOffset = logTimestampOffsets.get(topicPartition)
          logTimestampOffset match {
            case Some(LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
            case _ => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting offset by timestamp of topic partition: $topicPartition")
          }
        }.toMap
      } else if (resetPlanFromFile.isDefined) {
        resetPlanFromFile.map(resetPlan => resetPlan.get(groupId).map { resetPlanForGroup =>
          val requestedOffsets = resetPlanForGroup.keySet.map { topicPartition =>
            topicPartition -> resetPlanForGroup(topicPartition).offset
          }.toMap
          checkOffsetsRange(requestedOffsets).map {
            case (topicPartition, newOffset) => (topicPartition, new OffsetAndMetadata(newOffset))
          }
        } match {
          case Some(resetPlanForGroup) => resetPlanForGroup
          case None =>
            printError(s"No reset plan for group $groupId found")
            Map[TopicPartition, OffsetAndMetadata]()
        }).getOrElse(Map.empty)
      } else if (opts.options.has(opts.resetToCurrentOpt)) {
        val currentCommittedOffsets = getCommittedOffsets(groupId)
        val (partitionsToResetWithCommittedOffset, partitionsToResetWithoutCommittedOffset) =
          partitionsToReset.partition(currentCommittedOffsets.keySet.contains(_))

        val preparedOffsetsForPartitionsWithCommittedOffset = partitionsToResetWithCommittedOffset.map { topicPartition =>
          (topicPartition, new OffsetAndMetadata(currentCommittedOffsets.get(topicPartition) match {
            case Some(offset) => offset.offset
            case None => throw new IllegalStateException(s"Expected a valid current offset for topic partition: $topicPartition")
          }))
        }.toMap

        val preparedOffsetsForPartitionsWithoutCommittedOffset = getLogEndOffsets(partitionsToResetWithoutCommittedOffset).map {
          case (topicPartition, LogOffsetResult.LogOffset(offset)) => (topicPartition, new OffsetAndMetadata(offset))
          case (topicPartition, _) => ToolsUtils.printUsageAndExit(opts.parser, s"Error getting ending offset of topic partition: $topicPartition")
        }

        preparedOffsetsForPartitionsWithCommittedOffset ++ preparedOffsetsForPartitionsWithoutCommittedOffset
      } else {
        ToolsUtils.printUsageAndExit(opts.parser, "Option '%s' requires one of the following scenarios: %s".format(opts.resetOffsetsOpt, opts.allResetOffsetScenarioOpts))
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

    def exportOffsetsToCsv(assignments: Map[String, Map[TopicPartition, OffsetAndMetadata]]): String = {
      val isSingleGroupQuery = opts.options.valuesOf(opts.groupOpt).size() == 1
      val csvWriter =
        if (isSingleGroupQuery) CsvUtils().writerFor[CsvRecordNoGroup]
        else CsvUtils().writerFor[CsvRecordWithGroup]
      val rows = assignments.flatMap { case (groupId, partitionInfo) =>
        partitionInfo.map { case (k: TopicPartition, v: OffsetAndMetadata) =>
          val csvRecord =
            if (isSingleGroupQuery) CsvRecordNoGroup(k.topic, k.partition, v.offset)
            else CsvRecordWithGroup(groupId, k.topic, k.partition, v.offset)
          csvWriter.writeValueAsString(csvRecord)
        }
      }
      rows.mkString("")
    }

    def deleteGroups(): Map[String, Throwable] = {
      val groupIds =
        if (opts.options.has(opts.allGroupsOpt)) listConsumerGroups()
        else opts.options.valuesOf(opts.groupOpt).asScala

      val groupsToDelete = adminClient.deleteConsumerGroups(
        groupIds.asJava,
        withTimeoutMs(new DeleteConsumerGroupsOptions)
      ).deletedGroups().asScala

      val result = groupsToDelete.map { case (g, f) =>
        Try(f.get) match {
          case Success(_) => g -> null
          case Failure(e) => g -> e
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

  private object LogOffsetResult {
    case class LogOffset(value: Long) extends LogOffsetResult
    case object Unknown extends LogOffsetResult
    case object Ignore extends LogOffsetResult
  }

  class ConsumerGroupCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val BootstrapServerDoc = "REQUIRED: The server(s) to connect to."
    private val GroupDoc = "The consumer group we wish to act on."
    private val TopicDoc = "The topic whose consumer group information should be deleted or topic whose should be included in the reset offset process. " +
      "In `reset-offsets` case, partitions can be specified using this format: `topic1:0,1,2`, where 0,1,2 are the partition to be included in the process. " +
      "Reset-offsets also supports multiple topic inputs."
    private val AllTopicsDoc = "Consider all topics assigned to a group in the `reset-offsets` process."
    private val ListDoc = "List all consumer groups."
    private val DescribeDoc = "Describe consumer group and list offset lag (number of messages not yet processed) related to given group."
    private val AllGroupsDoc = "Apply to all consumer groups."
    val nl: String = System.getProperty("line.separator")
    private val DeleteDoc = "Pass in groups to delete topic partition offsets and ownership information " +
      "over the entire consumer group. For instance --group g1 --group g2"
    private val TimeoutMsDoc = "The timeout that can be set for some use cases. For example, it can be used when describing the group " +
      "to specify the maximum amount of time in milliseconds to wait before the group stabilizes (when the group is just created, " +
      "or is going through some changes)."
    val CommandConfigDoc: String = "Property file containing configs to be passed to Admin Client and Consumer."
    private val ResetOffsetsDoc = "Reset offsets of consumer group. Supports one consumer group at the time, and instances should be inactive" + nl +
      "Has 2 execution options: --dry-run (the default) to plan which offsets to reset, and --execute to update the offsets. " +
      "Additionally, the --export option is used to export the results to a CSV format." + nl +
      "You must choose one of the following reset specifications: --to-datetime, --by-duration, --to-earliest, " +
      "--to-latest, --shift-by, --from-file, --to-current, --to-offset." + nl +
      "To define the scope use --all-topics or --topic. One scope must be specified unless you use '--from-file'."
    private val DryRunDoc = "Only show results without executing changes on Consumer Groups. Supported operations: reset-offsets."
    private val ExecuteDoc = "Execute operation. Supported operations: reset-offsets."
    private val ExportDoc = "Export operation execution to a CSV file. Supported operations: reset-offsets."
    private val ResetToOffsetDoc = "Reset offsets to a specific offset."
    private val ResetFromFileDoc = "Reset offsets to values defined in CSV file."
    private val ResetToDatetimeDoc = "Reset offsets to offset from datetime. Format: 'YYYY-MM-DDTHH:mm:SS.sss'"
    private val ResetByDurationDoc = "Reset offsets to offset by duration from current timestamp. Format: 'PnDTnHnMnS'"
    private val ResetToEarliestDoc = "Reset offsets to earliest offset."
    private val ResetToLatestDoc = "Reset offsets to latest offset."
    private val ResetToCurrentDoc = "Reset offsets to current offset."
    private val ResetShiftByDoc = "Reset offsets shifting current offset by 'n', where 'n' can be positive or negative."
    private val MembersDoc = "Describe members of the group. This option may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --members"
    private val VerboseDoc = "Provide additional information, if any, when describing the group. This option may be used " +
      "with '--offsets'/'--members'/'--state' and '--bootstrap-server' options only." + nl + "Example: --bootstrap-server localhost:9092 --describe --group group1 --members --verbose"
    private val OffsetsDoc = "Describe the group and list all topic partitions in the group along with their offset lag. " +
      "This is the default sub-action of and may be used with '--describe' and '--bootstrap-server' options only." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --offsets"
    private val StateDoc = "When specified with '--describe', includes the state of the group." + nl +
      "Example: --bootstrap-server localhost:9092 --describe --group group1 --state" + nl +
      "When specified with '--list', it displays the state of all groups. It can also be used to list groups with specific states." + nl +
      "Example: --bootstrap-server localhost:9092 --list --state stable,empty" + nl +
      "This option may be used with '--describe', '--list' and '--bootstrap-server' options only."
    private val TypeDoc = "When specified with '--list', it displays the types of all the groups. It can also be used to list groups with specific types." + nl +
      "Example: --bootstrap-server localhost:9092 --list --type classic,consumer" + nl +
      "This option may be used with the '--list' option only."
    private val DeleteOffsetsDoc = "Delete offsets of consumer group. Supports one consumer group at the time, and multiple topics."

    val bootstrapServerOpt: OptionSpec[String] = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server to connect to")
                                   .ofType(classOf[String])
    val groupOpt: OptionSpec[String] = parser.accepts("group", GroupDoc)
                         .withRequiredArg
                         .describedAs("consumer group")
                         .ofType(classOf[String])
    val topicOpt: OptionSpec[String] = parser.accepts("topic", TopicDoc)
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val allTopicsOpt: OptionSpecBuilder = parser.accepts("all-topics", AllTopicsDoc)
    val listOpt: OptionSpecBuilder = parser.accepts("list", ListDoc)
    val describeOpt: OptionSpecBuilder = parser.accepts("describe", DescribeDoc)
    val allGroupsOpt: OptionSpecBuilder = parser.accepts("all-groups", AllGroupsDoc)
    val deleteOpt: OptionSpecBuilder = parser.accepts("delete", DeleteDoc)
    val timeoutMsOpt: OptionSpec[Long] = parser.accepts("timeout", TimeoutMsDoc)
                             .withRequiredArg
                             .describedAs("timeout (ms)")
                             .ofType(classOf[Long])
                             .defaultsTo(5000)
    val commandConfigOpt: OptionSpec[String] = parser.accepts("command-config", CommandConfigDoc)
                                  .withRequiredArg
                                  .describedAs("command config property file")
                                  .ofType(classOf[String])
    val resetOffsetsOpt: OptionSpecBuilder = parser.accepts("reset-offsets", ResetOffsetsDoc)
    val deleteOffsetsOpt: OptionSpecBuilder = parser.accepts("delete-offsets", DeleteOffsetsDoc)
    val dryRunOpt: OptionSpecBuilder = parser.accepts("dry-run", DryRunDoc)
    val executeOpt: OptionSpecBuilder = parser.accepts("execute", ExecuteDoc)
    val exportOpt: OptionSpecBuilder = parser.accepts("export", ExportDoc)
    val resetToOffsetOpt: OptionSpec[Long] = parser.accepts("to-offset", ResetToOffsetDoc)
                           .withRequiredArg()
                           .describedAs("offset")
                           .ofType(classOf[Long])
    val resetFromFileOpt: OptionSpec[String] = parser.accepts("from-file", ResetFromFileDoc)
                                 .withRequiredArg()
                                 .describedAs("path to CSV file")
                                 .ofType(classOf[String])
    val resetToDatetimeOpt: OptionSpec[String] = parser.accepts("to-datetime", ResetToDatetimeDoc)
                                   .withRequiredArg()
                                   .describedAs("datetime")
                                   .ofType(classOf[String])
    val resetByDurationOpt: OptionSpec[String] = parser.accepts("by-duration", ResetByDurationDoc)
                                   .withRequiredArg()
                                   .describedAs("duration")
                                   .ofType(classOf[String])
    val resetToEarliestOpt: OptionSpecBuilder = parser.accepts("to-earliest", ResetToEarliestDoc)
    val resetToLatestOpt: OptionSpecBuilder = parser.accepts("to-latest", ResetToLatestDoc)
    val resetToCurrentOpt: OptionSpecBuilder = parser.accepts("to-current", ResetToCurrentDoc)
    val resetShiftByOpt: OptionSpec[Long] = parser.accepts("shift-by", ResetShiftByDoc)
                             .withRequiredArg()
                             .describedAs("number-of-offsets")
                             .ofType(classOf[Long])
    val membersOpt: OptionSpecBuilder = parser.accepts("members", MembersDoc)
                           .availableIf(describeOpt)
    val verboseOpt: OptionSpecBuilder = parser.accepts("verbose", VerboseDoc)
                           .availableIf(describeOpt)
    val offsetsOpt: OptionSpecBuilder = parser.accepts("offsets", OffsetsDoc)
                           .availableIf(describeOpt)
    val stateOpt: OptionSpec[String] = parser.accepts("state", StateDoc)
                         .availableIf(describeOpt, listOpt)
                         .withOptionalArg()
                         .ofType(classOf[String])
    val typeOpt: OptionSpec[String] = parser.accepts("type", TypeDoc)
                        .availableIf(listOpt)
                        .withOptionalArg()
                        .ofType(classOf[String])

    options = parser.parse(args : _*)

    private val allGroupSelectionScopeOpts = immutable.Set[OptionSpec[_]](groupOpt, allGroupsOpt)
    private val allConsumerGroupLevelOpts = immutable.Set[OptionSpec[_]](listOpt, describeOpt, deleteOpt, resetOffsetsOpt)
    val allResetOffsetScenarioOpts: Set[OptionSpec[_]] = immutable.Set[OptionSpec[_]](resetToOffsetOpt, resetShiftByOpt,
      resetToDatetimeOpt, resetByDurationOpt, resetToEarliestOpt, resetToLatestOpt, resetToCurrentOpt, resetFromFileOpt)
    private val allDeleteOffsetsOpts = immutable.Set[OptionSpec[_]](groupOpt, topicOpt)

    def checkArgs(): Unit = {

      CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)

      if (options.has(describeOpt)) {
        if (!options.has(groupOpt) && !options.has(allGroupsOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $describeOpt takes one of these options: ${allGroupSelectionScopeOpts.mkString(", ")}")
        val mutuallyExclusiveOpts: Set[OptionSpec[_]] = Set(membersOpt, offsetsOpt, stateOpt)
        if (mutuallyExclusiveOpts.toList.map(o => if (options.has(o)) 1 else 0).sum > 1) {
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $describeOpt takes at most one of these options: ${mutuallyExclusiveOpts.mkString(", ")}")
        }
        if (options.has(stateOpt) && options.valueOf(stateOpt) != null)
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $describeOpt does not take a value for $stateOpt")
      } else {
        if (options.has(timeoutMsOpt))
          debug(s"Option $timeoutMsOpt is applicable only when $describeOpt is used.")
      }

      if (options.has(deleteOpt)) {
        if (!options.has(groupOpt) && !options.has(allGroupsOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $deleteOpt takes one of these options: ${allGroupSelectionScopeOpts.mkString(", ")}")
        if (options.has(topicOpt))
          CommandLineUtils.printUsageAndExit(parser, s"The consumer does not support topic-specific offset " +
            "deletion from a consumer group.")
      }

      if (options.has(deleteOffsetsOpt)) {
        if (!options.has(groupOpt) || !options.has(topicOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $deleteOffsetsOpt takes the following options: ${allDeleteOffsetsOpts.mkString(", ")}")
      }

      if (options.has(resetOffsetsOpt)) {
        if (options.has(dryRunOpt) && options.has(executeOpt))
          CommandLineUtils.printUsageAndExit(parser, s"Option $resetOffsetsOpt only accepts one of $executeOpt and $dryRunOpt")

        if (!options.has(dryRunOpt) && !options.has(executeOpt)) {
          Console.err.println("WARN: No action will be performed as the --execute option is missing." +
            "In a future major release, the default behavior of this command will be to prompt the user before " +
            "executing the reset rather than doing a dry run. You should add the --dry-run option explicitly " +
            "if you are scripting this command and want to keep the current default behavior without prompting.")
        }

        if (!options.has(groupOpt) && !options.has(allGroupsOpt))
          CommandLineUtils.printUsageAndExit(parser,
            s"Option $resetOffsetsOpt takes one of these options: ${allGroupSelectionScopeOpts.mkString(", ")}")
        CommandLineUtils.checkInvalidArgs(parser, options, resetToOffsetOpt,   (allResetOffsetScenarioOpts - resetToOffsetOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToDatetimeOpt, (allResetOffsetScenarioOpts - resetToDatetimeOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetByDurationOpt, (allResetOffsetScenarioOpts - resetByDurationOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToEarliestOpt, (allResetOffsetScenarioOpts - resetToEarliestOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToLatestOpt,   (allResetOffsetScenarioOpts - resetToLatestOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetToCurrentOpt,  (allResetOffsetScenarioOpts - resetToCurrentOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetShiftByOpt,    (allResetOffsetScenarioOpts - resetShiftByOpt).asJava)
        CommandLineUtils.checkInvalidArgs(parser, options, resetFromFileOpt,   (allResetOffsetScenarioOpts - resetFromFileOpt).asJava)
      }

      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, (allGroupSelectionScopeOpts - groupOpt).asJava)
      CommandLineUtils.checkInvalidArgs(parser, options, groupOpt, (allConsumerGroupLevelOpts - describeOpt - deleteOpt - resetOffsetsOpt).asJava)
      CommandLineUtils.checkInvalidArgs(parser, options, topicOpt, (allConsumerGroupLevelOpts - deleteOpt - resetOffsetsOpt).asJava )
    }
  }
}
