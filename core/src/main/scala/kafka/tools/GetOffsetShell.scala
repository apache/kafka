/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package kafka.tools

import joptsimple._
import kafka.utils.{CommandLineUtils, Exit, IncludeList, ToolsUtils}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, ListTopicsOptions, OffsetSpec}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.requests.{ListOffsetsRequest, ListOffsetsResponse}
import org.apache.kafka.common.utils.Utils

import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.regex.Pattern
import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.math.Ordering.Implicits.infixOrderingOps

object GetOffsetShell {
  private val TopicPartitionPattern = Pattern.compile("([^:,]*)(?::(?:([0-9]*)|(?:([0-9]*)-([0-9]*))))?")

  def main(args: Array[String]): Unit = {
    try {
      fetchOffsets(args)
    } catch {
      case e: Exception =>
        println(s"Error occurred: ${e.getMessage}")
        Exit.exit(1, Some(e.getMessage))
    }
  }

  private[tools] def fetchOffsets(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified. The server(s) to connect to in the form HOST1:PORT1,HOST2:PORT2.")
                           .withRequiredArg
                           .describedAs("HOST1:PORT1,...,HOST3:PORT3")
                           .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED. The server(s) to connect to in the form HOST1:PORT1,HOST2:PORT2.")
                           .requiredUnless("broker-list")
                           .withRequiredArg
                           .describedAs("HOST1:PORT1,...,HOST3:PORT3")
                           .ofType(classOf[String])
    val topicPartitionsOpt = parser.accepts("topic-partitions", s"Comma separated list of topic-partition patterns to get the offsets for, with the format of '$TopicPartitionPattern'." +
                                            " The first group is an optional regex for the topic name, if omitted, it matches any topic name." +
                                            " The section after ':' describes a 'partition' pattern, which can be: a number, a range in the format of 'NUMBER-NUMBER' (lower inclusive, upper exclusive), an inclusive lower bound in the format of 'NUMBER-', an exclusive upper bound in the format of '-NUMBER' or may be omitted to accept all partitions.")
                           .withRequiredArg
                           .describedAs("topic1:1,topic2:0-3,topic3,topic4:5-,topic5:-3")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", s"The topic to get the offsets for. It also accepts a regular expression. If not present, all authorized topics are queried. Cannot be used if --topic-partitions is present.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", s"Comma separated list of partition ids to get the offsets for. If not present, all partitions of the authorized topics are queried. Cannot be used if --topic-partitions is present.")
                           .withRequiredArg
                           .describedAs("partition ids")
                           .ofType(classOf[String])
    val timeOpt = parser.accepts("time", "timestamp of the offsets before that. [Note: No offset is returned, if the timestamp greater than recently committed record timestamp is given.]")
                           .withRequiredArg
                           .describedAs("<timestamp> / -1 or latest / -2 or earliest / -3 or max-timestamp")
                           .ofType(classOf[String])
                           .defaultsTo("latest")
    val commandConfigOpt = parser.accepts("command-config", s"Property file containing configs to be passed to Admin Client.")
                           .withRequiredArg
                           .describedAs("config file")
                           .ofType(classOf[String])
    val excludeInternalTopicsOpt = parser.accepts("exclude-internal-topics", s"By default, internal topics are included. If specified, internal topics are excluded.")

    if (args.isEmpty)
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting topic-partition offsets.")

    val options = parser.parse(args : _*)

    val effectiveBrokerListOpt = if (options.has(bootstrapServerOpt))
      bootstrapServerOpt
    else
      brokerListOpt

    CommandLineUtils.checkRequiredArgs(parser, options, effectiveBrokerListOpt)

    val clientId = "GetOffsetShell"
    val brokerList = options.valueOf(effectiveBrokerListOpt)

    ToolsUtils.validatePortOrDie(parser, brokerList)
    val excludeInternalTopics = options.has(excludeInternalTopicsOpt)

    if (options.has(topicPartitionsOpt) && (options.has(topicOpt) || options.has(partitionsOpt))) {
      throw new IllegalArgumentException("--topic-partitions cannot be used with --topic or --partitions")
    }

    val offsetSpec = parseOffsetSpec(options.valueOf(timeOpt))

    val topicPartitionFilter = if (options.has(topicPartitionsOpt)) {
      createTopicPartitionFilterWithPatternList(options.valueOf(topicPartitionsOpt))
    } else {
      createTopicPartitionFilterWithTopicAndPartitionPattern(
        if (options.has(topicOpt)) Some(options.valueOf(topicOpt)) else None,
        options.valueOf(partitionsOpt)
      )
    }

    val config = if (options.has(commandConfigOpt))
      Utils.loadProps(options.valueOf(commandConfigOpt))
    else
      new Properties
    config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.setProperty(AdminClientConfig.CLIENT_ID_CONFIG, clientId)
    val adminClient = Admin.create(config)

    try {
      val partitionInfos = listPartitionInfos(adminClient, topicPartitionFilter, excludeInternalTopics)

      if (partitionInfos.isEmpty) {
        throw new IllegalArgumentException("Could not match any topic-partitions with the specified filters")
      }

      val timestampsToSearch = partitionInfos.map(tp => tp -> offsetSpec).toMap.asJava

      val listOffsetsResult = adminClient.listOffsets(timestampsToSearch)
      val partitionOffsets = partitionInfos.flatMap { tp =>
        try {
          val partitionInfo = listOffsetsResult.partitionResult(tp).get
          if (partitionInfo.offset != ListOffsetsResponse.UNKNOWN_OFFSET) {
            Some((tp, partitionInfo.offset))
          } else {
            None
          }
        } catch {
          case e: ExecutionException =>
            e.getCause match {
              case cause: KafkaException =>
                System.err.println(s"Skip getting offsets for topic-partition ${tp.topic}:${tp.partition} due to error: ${cause.getMessage}")
              case _ =>
                throw e
            }
            None
        }
      }

      partitionOffsets.sortWith((tp1, tp2) => compareTopicPartitions(tp1._1, tp2._1)).foreach {
        case (tp, offset) => println(s"${tp.topic}:${tp.partition}:${Option(offset).getOrElse("")}")
      }
    } finally {
      adminClient.close()
    }
  }

  private def parseOffsetSpec(listOffsetsTimestamp: String): OffsetSpec = {
    listOffsetsTimestamp match {
      case "earliest" => OffsetSpec.earliest()
      case "latest" => OffsetSpec.latest()
      case "max-timestamp" => OffsetSpec.maxTimestamp()
      case _ =>
        try {
          listOffsetsTimestamp.toLong match {
            case ListOffsetsRequest.EARLIEST_TIMESTAMP => OffsetSpec.earliest()
            case ListOffsetsRequest.LATEST_TIMESTAMP => OffsetSpec.latest()
            case ListOffsetsRequest.MAX_TIMESTAMP => OffsetSpec.maxTimestamp()
            case value => OffsetSpec.forTimestamp(value)
          }
        } catch {
          case e: NumberFormatException =>
            throw new IllegalArgumentException(s"Malformed time argument $listOffsetsTimestamp, please use -1 or latest / -2 or earliest / -3 or max-timestamp, or a specified long format timestamp", e)
        }
    }
  }

  def compareTopicPartitions(a: TopicPartition, b: TopicPartition): Boolean = {
    (a.topic(), a.partition()) < (b.topic(), b.partition())
  }

  /**
   * Creates a topic-partition filter based on a list of patterns.
   * Expected format:
   * List: TopicPartitionPattern(, TopicPartitionPattern)*
   * TopicPartitionPattern: TopicPattern(:PartitionPattern)? | :PartitionPattern
   * TopicPattern: REGEX
   * PartitionPattern: NUMBER | NUMBER-(NUMBER)? | -NUMBER
   */
  def createTopicPartitionFilterWithPatternList(
    topicPartitions: String
  ): TopicPartitionFilter = {
    val ruleSpecs = topicPartitions.split(",")
    val rules = ruleSpecs.map(ruleSpec => parseRuleSpec(ruleSpec))
    CompositeTopicPartitionFilter(rules)
  }

  def parseRuleSpec(ruleSpec: String): TopicPartitionFilter = {
    val matcher = TopicPartitionPattern.matcher(ruleSpec)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Invalid rule specification: $ruleSpec")

    def group(group: Int): Option[String] = {
      Option(matcher.group(group)).filter(s => s != null && s.nonEmpty)
    }

    val topicFilter = IncludeList(group(1).getOrElse(".*"))
    val partitionFilter = group(2).map(_.toInt) match {
      case Some(partition) =>
        UniquePartitionFilter(partition)
      case None =>
        val lowerRange = group(3).map(_.toInt).getOrElse(0)
        val upperRange = group(4).map(_.toInt).getOrElse(Int.MaxValue)
        PartitionRangeFilter(lowerRange, upperRange)
    }
    TopicFilterAndPartitionFilter(
      topicFilter,
      partitionFilter
    )
  }

  /**
   * Creates a topic-partition filter based on a topic pattern and a set of partition ids.
   */
  def createTopicPartitionFilterWithTopicAndPartitionPattern(
    topicOpt: Option[String],
    partitionIds: String
  ): TopicFilterAndPartitionFilter = {
    TopicFilterAndPartitionFilter(
      IncludeList(topicOpt.getOrElse(".*")),
      PartitionsSetFilter(createPartitionSet(partitionIds))
    )
  }

  def createPartitionSet(partitionsString: String): Set[Int] = {
    if (partitionsString == null || partitionsString.isEmpty)
      Set.empty
    else
      partitionsString.split(",").map { partitionString =>
        try partitionString.toInt
        catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"--partitions expects a comma separated list of numeric " +
              s"partition ids, but received: $partitionsString")
        }
      }.toSet
  }

  /**
   * Return the partition infos. Filter them with topicPartitionFilter.
   */
  private def listPartitionInfos(
    client: Admin,
    topicPartitionFilter: TopicPartitionFilter,
    excludeInternalTopics: Boolean
  ): Seq[TopicPartition] = {
    val listTopicsOptions = new ListTopicsOptions().listInternal(!excludeInternalTopics)
    val topics = client.listTopics(listTopicsOptions).names.get
    val filteredTopics = topics.asScala.filter(topicPartitionFilter.isTopicAllowed)

    client.describeTopics(filteredTopics.asJava).allTopicNames.get.asScala.flatMap { case (topic, description) =>
      description
        .partitions
        .asScala
        .map(tp => new TopicPartition(topic, tp.partition))
        .filter(topicPartitionFilter.isTopicPartitionAllowed)
    }.toBuffer
  }
}

trait PartitionFilter {

  /**
   * Used to filter partitions based on a certain criteria, for example, a set of partition ids.
   */
  def isPartitionAllowed(partition: Int): Boolean
}

case class PartitionsSetFilter(partitionIds: Set[Int]) extends PartitionFilter {
  override def isPartitionAllowed(partition: Int): Boolean = partitionIds.isEmpty || partitionIds.contains(partition)
}

case class UniquePartitionFilter(partition: Int) extends PartitionFilter {
  override def isPartitionAllowed(partition: Int): Boolean = partition == this.partition
}

case class PartitionRangeFilter(lowerRange: Int, upperRange: Int) extends PartitionFilter {
  override def isPartitionAllowed(partition: Int): Boolean = partition >= lowerRange && partition < upperRange
}

trait TopicPartitionFilter {

  /**
   * Used to filter topics based on a certain criteria, for example, a set of topic names or a regular expression.
   */
  def isTopicAllowed(topic: String): Boolean

  /**
   * Used to filter topic-partitions based on a certain criteria, for example, a topic pattern and a set of partition ids.
   */
  def isTopicPartitionAllowed(partition: TopicPartition): Boolean
}

/**
 * Creates a topic-partition filter based on a topic filter and a partition filter
 */
case class TopicFilterAndPartitionFilter(
  topicFilter: IncludeList,
  partitionFilter: PartitionFilter
) extends TopicPartitionFilter {

  override def isTopicPartitionAllowed(partition: TopicPartition): Boolean = {
    isTopicAllowed(partition.topic) && partitionFilter.isPartitionAllowed(partition.partition)
  }

  override def isTopicAllowed(topic: String): Boolean = {
    topicFilter.isTopicAllowed(topic, false)
  }
}

case class CompositeTopicPartitionFilter(filters: Array[TopicPartitionFilter]) extends TopicPartitionFilter {

  override def isTopicAllowed(topic: String): Boolean = {
    filters.exists(_.isTopicAllowed(topic))
  }

  override def isTopicPartitionAllowed(tp: TopicPartition): Boolean = {
    filters.exists(_.isTopicPartitionAllowed(tp))
  }
}
