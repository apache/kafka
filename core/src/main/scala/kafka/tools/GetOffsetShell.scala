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
import kafka.utils.{CommandLineUtils, Exit, ToolsUtils, IncludeList}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.requests.ListOffsetsRequest
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._
import scala.collection.Seq

object GetOffsetShell {

  def main(args: Array[String]): Unit = {
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
    val topicPartitionsOpt = parser.accepts("topic-partitions", "Comma separated list of topic-partition specifications to get the offsets for, with the format of topic:partition. The 'topic' part can be a regex or may be omitted to only specify the partitions, and query all authorized topics." +
                                            " The 'partition' part can be: a number, a range in the format of 'NUMBER-NUMBER' (lower inclusive, upper exclusive), an inclusive lower bound in the format of 'NUMBER-', an exclusive upper bound in the format of '-NUMBER' or may be omitted to accept all partitions of the specified topic.")
                           .withRequiredArg
                           .describedAs("topic:partition,...,topic:partition")
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
      System.err.println(s"--topic-partitions cannot be used with --topic or --partitions")
      Exit.exit(1)
    }

    val partitionIdsRequested: Set[Int] = {
      val partitionsString = options.valueOf(partitionsOpt)
      if (partitionsString == null || partitionsString.isEmpty)
        Set.empty
      else
        partitionsString.split(",").map { partitionString =>
          try partitionString.toInt
          catch {
            case _: NumberFormatException =>
              System.err.println(s"--partitions expects a comma separated list of numeric partition ids, but received: $partitionsString")
              Exit.exit(1)
          }
        }.toSet
    }
    val listOffsetsTimestamp = options.valueOf(timeOpt).longValue

    val topicPartitionFilter = if (options.has(topicPartitionsOpt)) {
      createTopicPartitionFilterWithPatternList(options.valueOf(topicPartitionsOpt), excludeInternalTopics)
    } else {
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

    val partitionInfos = listPartitionInfos(consumer, topicPartitionFilter)

    if (partitionInfos.isEmpty) {
      System.err.println(s"Could not match any topic-partitions with the specified filters")
      Exit.exit(1)
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

    partitionOffsets.toSeq.sortWith((tp1, tp2) => {
      val topicComp = tp1._1.topic.compareTo(tp2._1.topic)
      if (topicComp == 0)
        tp1._1.partition < tp2._1.partition
      else
        topicComp < 0
    }).foreach { case (tp, offset) =>
      println(s"${tp.topic}:${tp.partition}:${Option(offset).getOrElse("")}")
    }
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
    val rules = ruleSpecs.map { ruleSpec =>
      val parts = ruleSpec.split(":")
      if (parts.length == 1) {
        val includeList = IncludeList(parts(0))
        tp: PartitionInfo => includeList.isTopicAllowed(tp.topic, excludeInternalTopics)
      } else if (parts.length == 2) {
        try {
          val partitionFilter = createPartitionFilter(parts(1))
          val includeList = if (parts(0).trim().isEmpty) IncludeList(".*") else IncludeList(parts(0))

          tp: PartitionInfo => includeList.isTopicAllowed(tp.topic, excludeInternalTopics) && partitionFilter.apply(tp.partition)
        } catch {
          case e: IllegalArgumentException =>
            throw new IllegalArgumentException(s"Invalid rule '$ruleSpec'; Issue: ${e.getMessage}")
        }

      } else {
        throw new IllegalArgumentException(s"Invalid topic-partition rule: $ruleSpec")
      }
    }

    tp => rules.exists(rule => rule.apply(tp))
  }

  /**
   * Creates a partition filter based on a single id or a range.
   * Expected format:
   * PartitionPattern: NUMBER | NUMBER-(NUMBER)? | -NUMBER
   */
  def createPartitionFilter(spec: String): Int => Boolean = {
    try {
      if (spec.indexOf('-') != -1) {
        val rangeParts = spec.split("-", -1)
        if (rangeParts.length != 2 || rangeParts(0).isEmpty && rangeParts(1).isEmpty) {
          throw new IllegalArgumentException(s"Invalid range specification: $spec")
        }

        if (rangeParts(0).isEmpty) {
          val max = rangeParts(1).toInt
          partition: Int => partition < max
        } else if (rangeParts(1).isEmpty) {
          val min = rangeParts(0).toInt
          partition: Int => partition >= min
        } else {
          val min = rangeParts(0).toInt
          val max = rangeParts(1).toInt

          if (min > max) {
            throw new IllegalArgumentException(s"Range lower bound cannot be greater than upper bound: $spec")
          }

          partition: Int => partition >= min && partition < max
        }
      } else {
        val number = spec.toInt
        partition: Int => partition == number
      }
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"Expected a number in partition specification: $spec")
    }
  }

  /**
   * Creates a topic-partition filter based on a topic pattern and a set of partition ids.
   */
  def createTopicPartitionFilterWithTopicAndPartitionPattern(topicOpt: Option[String], excludeInternalTopics: Boolean, partitionIds: Set[Int]): PartitionInfo => Boolean = {
    val topicsFilter = IncludeList(topicOpt.getOrElse(".*"))
    t => topicsFilter.isTopicAllowed(t.topic, excludeInternalTopics) && (partitionIds.isEmpty || partitionIds.contains(t.partition))
  }

  /**
   * Return the partition infos. Filter them with topicPartitionFilter.
   */
  private def listPartitionInfos(consumer: KafkaConsumer[_, _], topicPartitionFilter: PartitionInfo => Boolean): Seq[PartitionInfo] = {
    consumer.listTopics.asScala.values.flatMap { partitions =>
      partitions.asScala.filter { tp => topicPartitionFilter.apply(tp) }
    }.toBuffer
  }
}
