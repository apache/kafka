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
import kafka.utils.{CommandLineUtils, Exit, ToolsUtils, Whitelist}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._
import scala.collection.Seq

object GetOffsetShell {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                           .withRequiredArg
                           .describedAs("hostname:port,...,hostname:port")
                           .ofType(classOf[String])
    val topicPartitionOpt = parser.accepts("topic-partitions", "Comma separated list of topic-partition specifications to get the offsets for, with the format of topic:partition. The 'topic' part can be a regex or may be omitted to only specify the partitions, and query all authorized topics." +
                                            " The 'partition' part can be: a number, a range in the format of 'NUMBER-NUMBER' (lower inclusive, upper exclusive), an inclusive lower bound in the format of 'NUMBER-', an exclusive upper bound in the format of '-NUMBER' or may be omitted to accept all partitions of the specified topic.")
                           .withRequiredArg
                           .describedAs("topic:partition,...,topic:partition")
                           .ofType(classOf[String])
                           .defaultsTo("")
    val topicOpt = parser.accepts("topic", s"The topic to get the offsets for. It also accepts a regular expression. If not present, all authorized topics are queried. Ignored if $topicPartitionOpt is present.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionOpt = parser.accepts("partitions", s"Comma separated list of partition ids to get the offsets for. If not present, all partitions of the authorized topics are queried. Ignored if $topicPartitionOpt is present.")
                           .withRequiredArg
                           .describedAs("partition ids")
                           .ofType(classOf[String])
                           .defaultsTo("")
    val timeOpt = parser.accepts("time", "timestamp of the offsets before that. [Note: No offset is returned, if the timestamp greater than recently commited record timestamp is given.]")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1L)
    val commandConfigOpt = parser.accepts("command-config", s"Consumer config properties file.")
                           .withRequiredArg
                           .describedAs("config file")
                           .ofType(classOf[String])
    val excludeInternalTopicsOpt = parser.accepts("exclude-internal-topics", s"By default, internal topics are included. If specified, internal topics are excluded.")

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting topic-partition offsets.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt)

    val clientId = "GetOffsetShell"
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val excludeInternalTopics = options.has(excludeInternalTopicsOpt)

    val partitionIdsRequested: Set[Int] = {
      val partitionsString = options.valueOf(partitionOpt)
      if (partitionsString.isEmpty)
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

    val topicPartitionFilter = if (options.has(topicPartitionOpt)) {
      Some(createTopicPartitionFilterWithPatternList(options.valueOf(topicPartitionOpt), excludeInternalTopics))
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

    val topicPartitions = partitionInfos.sortWith((tp1, tp2) => {
      val topicComp = tp1.topic.compareTo(tp2.topic)
      if(topicComp == 0)
        tp1.partition < tp2.partition
      else
        topicComp < 0
    }).flatMap { p =>
      if (p.leader == null) {
        System.err.println(s"Error: topic-partition ${p.topic}:${p.partition} does not have a leader. Skip getting offsets")
        None
      } else
        Some(new TopicPartition(p.topic, p.partition))
    }

    /* Note that the value of the map can be null */
    val partitionOffsets: collection.Map[TopicPartition, java.lang.Long] = listOffsetsTimestamp match {
      case ListOffsetRequest.EARLIEST_TIMESTAMP => consumer.beginningOffsets(topicPartitions.asJava).asScala
      case ListOffsetRequest.LATEST_TIMESTAMP => consumer.endOffsets(topicPartitions.asJava).asScala
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
  private def createTopicPartitionFilterWithPatternList(topicPartitions: String, excludeInternalTopics: Boolean): PartitionInfo => Boolean = {
    val ruleSpecs = topicPartitions.split(",")
    val rules = ruleSpecs.map(ruleSpec => {
      val parts = ruleSpec.split(":")
      if (parts.length == 1) {
        val whitelist = Whitelist(parts(0))
        tp: PartitionInfo => whitelist.isTopicAllowed(tp.topic, excludeInternalTopics)
      } else if (parts.length == 2) {
        val partitionFilter = createPartitionFilter(parts(1))

        if (parts(0).trim().isEmpty) {
          tp: PartitionInfo => partitionFilter.apply(tp.partition)
        } else {
          val whitelist = Whitelist(parts(0))
          tp: PartitionInfo => whitelist.isTopicAllowed(tp.topic, excludeInternalTopics) && partitionFilter.apply(tp.partition)
        }
      } else {
        throw new IllegalArgumentException(s"Invalid topic-partition rule: $ruleSpec")
      }
    })

    tp => rules.exists(rule => rule.apply(tp))
  }

  /**
   * Creates a partition filter based on a single id or a range.
   * Expected format:
   * PartitionPattern: NUMBER | NUMBER-(NUMBER)? | -NUMBER
   */
  private def createPartitionFilter(spec: String): Int => Boolean = {
    if (spec.indexOf('-') != -1) {
      val rangeParts = spec.split("-", -1)
      if(rangeParts.length != 2 || rangeParts(0).isEmpty && rangeParts(1).isEmpty) {
        throw new IllegalArgumentException(s"Invalid range specification: $spec")
      }

      if(rangeParts(0).isEmpty) {
        val max = rangeParts(1).toInt
        partition: Int => partition < max
      } else if(rangeParts(1).isEmpty) {
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
  }

  /**
   * Creates a topic-partition filter based on a topic pattern and a set of partition ids.
   */
  private def createTopicPartitionFilterWithTopicAndPartitionPattern(topicOpt: Option[String], excludeInternalTopics: Boolean, partitionIds: Set[Int]): Option[PartitionInfo => Boolean] = {
    topicOpt match {
      case Some(topic) =>
        val topicsFilter = Whitelist(topic)
        if(partitionIds.isEmpty)
          Some(t => topicsFilter.isTopicAllowed(t.topic, excludeInternalTopics))
        else
          Some(t => topicsFilter.isTopicAllowed(t.topic, excludeInternalTopics) && partitionIds.contains(t.partition))
      case None =>
        if(excludeInternalTopics) {
          if(partitionIds.isEmpty)
            Some(t => !Topic.isInternal(t.topic))
          else
            Some(t => !Topic.isInternal(t.topic) && partitionIds.contains(t.partition))
        } else {
          if(partitionIds.isEmpty)
            None
          else
            Some(t => partitionIds.contains(t.partition))
        }
    }
  }

  /**
   * Return the partition infos. Filter them with topicPartitionFilter if specified.
   */
  private def listPartitionInfos(consumer: KafkaConsumer[_, _], topicPartitionFilter: Option[PartitionInfo => Boolean]): Seq[PartitionInfo] = {
    val topicListUnfiltered = consumer.listTopics.asScala.values.flatMap { tp => tp.asScala }
    val topicList = topicPartitionFilter match {
      case Some(filter) => topicListUnfiltered.filter { tp => filter.apply(tp) }
      case _ => topicListUnfiltered
    }
    topicList.toBuffer
  }
}
