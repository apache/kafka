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

import java.util.Properties
import joptsimple._
import kafka.utils.{CommandLineUtils, Exit, IncludeList, ToolsUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils

import java.util.regex.Pattern
import scala.jdk.CollectionConverters._
import scala.collection.Seq
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

  private def fetchOffsets(args: Array[String]): Unit = {
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
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1L)
    val commandConfigOpt = parser.accepts("command-config", s"Property file containing configs to be passed to Consumer Client.")
                           .withRequiredArg
                           .describedAs("config file")
                           .ofType(classOf[String])
    val excludeInternalTopicsOpt = parser.accepts("exclude-internal-topics", s"By default, internal topics are included. If specified, internal topics are excluded.")

    if (args.length == 0)
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

    val listOffsetsTimestamp = options.valueOf(timeOpt).longValue

    val topicPartitionFilter = if (options.has(topicPartitionsOpt)) {
      createTopicPartitionFilterWithPatternList(options.valueOf(topicPartitionsOpt), excludeInternalTopics)
    } else {
      val partitionIdsRequested = createPartitionSet(options.valueOf(partitionsOpt))

      createTopicPartitionFilterWithTopicAndPartitionPattern(
        if (options.has(topicOpt)) Some(options.valueOf(topicOpt)) else None,
        excludeInternalTopics,
        partitionIdsRequested
      )
    }

    val config = if (options.has(commandConfigOpt))
      Utils.loadProps(options.valueOf(commandConfigOpt))
    else
      new Properties
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    val consumer = new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)

    try {
      val partitionInfos = listPartitionInfos(consumer, topicPartitionFilter)

      if (partitionInfos.isEmpty) {
        throw new IllegalArgumentException("Could not match any topic-partitions with the specified filters")
      }

      val topicPartitions = partitionInfos.flatMap { p =>
        if (p.leader == null) {
          System.err.println(s"Error: topic-partition ${p.topic}:${p.partition} does not have a leader. Skip getting offsets")
          None
        } else
          Some(new TopicPartition(p.topic, p.partition))
      }

      /* Note that the value of the map can be null */
      val partitionOffsets: collection.Map[TopicPartition, java.lang.Long] = listOffsetsTimestamp match {
        case ListOffsetsRequest.EARLIEST_TIMESTAMP => consumer.beginningOffsets(topicPartitions.asJava).asScala
        case ListOffsetsRequest.LATEST_TIMESTAMP => consumer.endOffsets(topicPartitions.asJava).asScala
        case _ =>
          val timestampsToSearch = topicPartitions.map(tp => tp -> (listOffsetsTimestamp: java.lang.Long)).toMap.asJava
          consumer.offsetsForTimes(timestampsToSearch).asScala.map { case (k, x) =>
            if (x == null) (k, null) else (k, x.offset: java.lang.Long)
          }
      }

      partitionOffsets.toSeq.sortWith((tp1, tp2) => compareTopicPartitions(tp1._1, tp2._1)).foreach {
        case (tp, offset) => println(s"${tp.topic}:${tp.partition}:${Option(offset).getOrElse("")}")
      }
    } finally {
      consumer.close()
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
  def createTopicPartitionFilterWithPatternList(topicPartitions: String, excludeInternalTopics: Boolean): PartitionInfo => Boolean = {
    val ruleSpecs = topicPartitions.split(",")
    val rules = ruleSpecs.map(ruleSpec => parseRuleSpec(ruleSpec, excludeInternalTopics))
    tp => rules.exists { rule => rule.apply(tp) }
  }

  def parseRuleSpec(ruleSpec: String, excludeInternalTopics: Boolean): PartitionInfo => Boolean = {
    val matcher = TopicPartitionPattern.matcher(ruleSpec)
    if (!matcher.matches())
      throw new IllegalArgumentException(s"Invalid rule specification: $ruleSpec")

    def group(group: Int): Option[String] = {
      Option(matcher.group(group)).filter(s => s != null && s.nonEmpty)
    }

    val topicFilter = IncludeList(group(1).getOrElse(".*"))
    val partitionFilter = group(2).map(_.toInt) match {
      case Some(partition) =>
        (p: Int) => p == partition
      case None =>
        val lowerRange = group(3).map(_.toInt).getOrElse(0)
        val upperRange = group(4).map(_.toInt).getOrElse(Int.MaxValue)
        (p: Int) => p >= lowerRange && p < upperRange
    }

    tp => topicFilter.isTopicAllowed(tp.topic, excludeInternalTopics) && partitionFilter(tp.partition)
  }

  /**
   * Creates a topic-partition filter based on a topic pattern and a set of partition ids.
   */
  def createTopicPartitionFilterWithTopicAndPartitionPattern(topicOpt: Option[String], excludeInternalTopics: Boolean, partitionIds: Set[Int]): PartitionInfo => Boolean = {
    val topicsFilter = IncludeList(topicOpt.getOrElse(".*"))
    t => topicsFilter.isTopicAllowed(t.topic, excludeInternalTopics) && (partitionIds.isEmpty || partitionIds.contains(t.partition))
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
  private def listPartitionInfos(consumer: KafkaConsumer[_, _], topicPartitionFilter: PartitionInfo => Boolean): Seq[PartitionInfo] = {
    consumer.listTopics.asScala.values.flatMap { partitions =>
      partitions.asScala.filter(topicPartitionFilter)
    }.toBuffer
  }
}
