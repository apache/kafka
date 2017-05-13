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
import kafka.common.Topic.InternalTopics
import kafka.utils.{CommandLineUtils, CoreUtils, Logging, ToolsUtils}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._


object GetOffsetShell extends Logging {

  private final val TOOL_NAME = this.getClass.getSimpleName.replace("$", "")

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val bootstrapServersOpt = parser.accepts("bootstrap-servers", "REQUIRED: The list of servers to connect to.")
      .withRequiredArg
      .describedAs("hostname:port,...,hostname:port")
      .ofType(classOf[String])
      /* We must allow default value here to preserve backward compatibility - old users may still have brokerListOpt. */
      .defaultsTo("")
    val topicsOpt = parser.accepts("topics", s"The list of topics to get offsets from. If not specified, $TOOL_NAME will get offsets for all topics.")
      .withRequiredArg
      .describedAs("topic1,...,topicN")
      .ofType(classOf[String])
      .defaultsTo("")
    val includeInternalTopicsOpt = parser.accepts("include-internal-topics", s"By default, when the list if topics is not given, $TOOL_NAME excludes internal topics like consumer offsets. This options forces $TOOL_NAME to include them.")
    val partitionsOpt = parser.accepts("partitions", s"The list of integers - partition identifiers. If not specified, $TOOL_NAME will get offsets for all partitions.")
      .withRequiredArg
      .describedAs("p1,...pM")
      .ofType(classOf[String])
      .defaultsTo("")
    val timeOpt = parser.accepts("time", s"$TOOL_NAME will get the earliest offset whose timestamp is greater than or equal to this timestamp. Special values: -1 (the latest offset in the partition), -2 (the earliest offset in the partition)")
      .withRequiredArg
      .describedAs("timestamp/-1(latest)/-2(earliest)")
      .ofType(classOf[java.lang.Long])
      .defaultsTo(-1L)
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
      .withRequiredArg
      .describedAs("property1=value1,...")
      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", s"DEPRECATED. Left for backward compatibility but may be removed in the future. Same as $bootstrapServersOpt. When both specified, $bootstrapServersOpt is used.")
      .withRequiredArg
      .describedAs("hostname:port,...,hostname:port")
      .ofType(classOf[String])
      .defaultsTo("")
    val topicOpt = parser.accepts("topic", s"DEPRECATED. Left for backward compatibility but may be removed in the future. Same as $topicsOpt. When both specified, $topicsOpt is used.")
      .withRequiredArg
      .describedAs("topic1,...,topicN")
      .ofType(classOf[String])
      .defaultsTo("")
    val nOffsetsOpt = parser.accepts("offsets", "DEPRECATED AND IGNORED: Always one offset is returned for each partition.")
      .withRequiredArg
      .describedAs("count")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", s"DEPRECATED AND IGNORED: Use $consumerPropertyOpt and pass '${ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG}' property instead.")
      .withRequiredArg
      .describedAs("ms")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1000)

    if(args.length == 0) {
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting consumer offsets.")
    }

    val options = parser.parse(args : _*)
    if (!options.has(bootstrapServersOpt) && !options.has(brokerListOpt)) {
      CommandLineUtils.printUsageAndDie(parser, "Please specify at least one Kafka server to connect to")
    }

    if (options.has(brokerListOpt)) {
      System.err.println(s"Warning: option $brokerListOpt is deprecated and may be removed in the future. Use $bootstrapServersOpt instead")
    }
    if (options.has(topicOpt)) {
      System.err.println(s"Warning: option $topicOpt is deprecated and may be removed in the future. Use $topicsOpt instead")
    }
    if (options.has(nOffsetsOpt)) {
      System.err.println(s"Warning: option $nOffsetsOpt is deprecated and ignored. It may be removed in the future. This implementation returns one offset per partition.")
    }
    if (options.has(maxWaitMsOpt)) {
      System.err.println(s"Warning: option $maxWaitMsOpt is deprecated and ignored. It may be removed in the future. As a replacement, use $consumerPropertyOpt option and pass '${ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG}' property.")
    }

    val bootstrapServers = if (options.has(bootstrapServersOpt)) options.valueOf(bootstrapServersOpt) else options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, bootstrapServers)
    val topicList = if (options.has(topicsOpt)) options.valueOf(topicsOpt) else options.valueOf(topicOpt)
    val includeInternalTopics = options.has(includeInternalTopicsOpt)
    val partitionList = options.valueOf(partitionsOpt)
    val timestamp = options.valueOf(timeOpt).longValue
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt))

    val topics = CoreUtils.parseCsvList(topicList).toSet
    val partitions = CoreUtils.parseCsvList(partitionList).map(_.toInt).toSet

    if (isDebugEnabled) {
      debug(s"Bootstrap servers: $bootstrapServers")
      debug(s"Topics: $topics")
      debug(s"Include internal topics: $includeInternalTopics")
      debug(s"Partitions: $partitions")
      debug(s"Timestamp: $timestamp")
      debug(s"Extra consumer properties: $extraConsumerProps")
    }

    val offsets = getOffsets(bootstrapServers,
      topics,
      partitions,
      timestamp,
      includeInternalTopics,
      extraConsumerProps)
    val report = offsets
      .toList.sortBy { case (tp, _) => (tp.topic, tp.partition) }
      .map {
        case (tp, reply) => reply match {
          case Right(offset) => "%s:%d: %d".format(tp.topic, tp.partition, offset)
          case Left(error) => "%s:%d: %s".format(tp.topic, tp.partition, error)
        }
      }
      .mkString("\n")
    println(report)
  }

  /**
    * Gets offsets for given topic partitions, for the given timestamp.
    * <p>
    * In total, this implementation makes at most two requests for the broker,
    * no matter how many topics and partitions are given in the argument:
    * get available partitions and get last offsets for partitions.
    *
    * @param bootstrapServers      Bootstrap Kafka servers - a comma separated list as KafkaConsumer accepts.
    * @param topics                Topics to query. If empty, all available topics are queried.
    * @param partitions            Partitions to query. If empty, all available partitions are queried.
    * @param timestamp             Get the earliest offset whose timestamp is greater than or equal to this timestamp value.
    *                              Special values are: -1 (the latest offsets), -2 (the earliest offsets).
    * @param includeInternalTopics When the topic partitions argument is empty, exclude internal topics (like consumer offsets) from consideration.
    * @param extraConsumerProps    Extra properties for Kafka consumer that will be created to get offsets.
    * @return A map of entries per each topic partition, each entry contains either the offset or an error message.
    * @throws IllegalArgumentException Bootstrap servers is null, topics is null, partitions is null.
    */
  def getOffsets(bootstrapServers: String,
                 topics: Set[String],
                 partitions: Set[Int],
                 timestamp: Long,
                 includeInternalTopics: Boolean,
                 extraConsumerProps: Properties = new Properties): Map[TopicPartition, Either[String, Long]] = {
    require(bootstrapServers != null, "Bootstrap servers cannot be null")
    require(topics != null, "Topics cannot be null")
    require(partitions != null, "Partitions cannot be null")

    val props = new Properties
    props.putAll(extraConsumerProps)
    val consumerConfig = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    props.putAll(consumerConfig)
    val consumer: Consumer[Array[Byte], Array[Byte]] = new KafkaConsumer(props)

    val available = consumer.listTopics()
      .map { case (topic, pInfos) => topic -> pInfos.map(_.partition()).toSet }
      .toMap
    val (toError, toRequest) = extractExistingPartitions(topics, partitions, available, includeInternalTopics)
    val offsets = timestamp match {
      case -1 => getEndOffsets(consumer, toRequest)
      case -2 => getBeginOffsets(consumer, toRequest)
      case _ => getOffsetsForTimestamp(consumer, toRequest, timestamp)
    }
    toError ++ offsets
  }

  private def extractExistingPartitions(topics: Set[String],
                                        partitions: Set[Int],
                                        available: Map[String, Set[Int]],
                                        includeInternalTopics: Boolean):
                     (Map[TopicPartition, Either[String, Long]], Set[TopicPartition]) = {
    if (topics.isEmpty && partitions.isEmpty) {
      val availablePartitions = toPartitions(available)
      (
        Map.empty,
        checkIncludeInternalTopics(availablePartitions, includeInternalTopics)
      )
    } else if (topics.isEmpty) {
      val toRequest = toPartitions(available).filter(tp => partitions.contains(tp.partition()))
      val toError = getNonExistingPartitions(partitions, available)
      (
        missingPartitions(checkIncludeInternalTopics(toError, includeInternalTopics)),
        checkIncludeInternalTopics(toRequest, includeInternalTopics)
      )
    } else if (partitions.isEmpty) {
      val toRequest = available.filter { case (topic, ps) => topics.contains(topic) }
      val toError = topics.filter(topic => !available.contains(topic))
      (
        missingTopics(toError),
        toPartitions(toRequest)
      )
    } else {
      val (nonExistingTopics, existingTopics) = topics.partition(topic => !available.contains(topic))
      val requestedPartitions = for {
        topic <- existingTopics
        partition <- partitions
      } yield new TopicPartition(topic, partition)
      val availablePartitions = toPartitions(available)
      val (toError, toRequest) = requestedPartitions.partition(tp => !availablePartitions.contains(tp))
      (
        missingTopics(nonExistingTopics) ++ missingPartitions(toError),
        toRequest
      )
    }
  }

  private def toPartitions(partitionsByTopic: Map[String, Set[Int]]): Set[TopicPartition] = {
    (for {
      (topic, partitions) <- partitionsByTopic
      partition <- partitions
    } yield new TopicPartition(topic, partition)).toSet
  }

  private def checkIncludeInternalTopics(partitions: Set[TopicPartition], includeInternalTopics: Boolean): Set[TopicPartition] = {
    if (includeInternalTopics)
      partitions
    else
      partitions.filterNot(tp => InternalTopics.contains(tp.topic()))
  }

  private def getNonExistingPartitions(partitions: Set[Int],
                                       available: Map[String, Set[Int]]): Set[TopicPartition] = {
    available
      .map { case (topic, ps) => (topic, partitions -- ps) }
      .flatMap { case (topic, ps) => ps.map(p => new TopicPartition(topic, p)) }
      .toSet
  }

  private def missingPartitions(partitions: Set[TopicPartition]): Map[TopicPartition, Either[String, Long]] = {
    partitions.map(tp => tp -> Left("Partition not found")).toMap
  }

  private def missingTopics(topics: Set[String]): Map[TopicPartition, Either[String, Long]] = {
    topics.map(topic => new TopicPartition(topic, 0) -> Left("Topic not found")).toMap
  }

  private def getEndOffsets(consumer: Consumer[Array[Byte], Array[Byte]],
                            partitions: Set[TopicPartition]): Map[TopicPartition, Either[String, Long]] = {
    consumer.endOffsets(partitions)
      .map { case (tp, offset) => tp -> Right(Long2long(offset)) }.toMap
  }

  private def getBeginOffsets(consumer: Consumer[Array[Byte], Array[Byte]],
                              partitions: Set[TopicPartition]): Map[TopicPartition, Either[String, Long]] = {
    consumer.beginningOffsets(partitions)
      .map { case (tp, offset) => tp -> Right(Long2long(offset)) }.toMap
  }

  private def getOffsetsForTimestamp(consumer: Consumer[Array[Byte], Array[Byte]],
                                     partitions: Set[TopicPartition],
                                     timestamp: Long): Map[TopicPartition, Either[String, Long]] = {
    val timestamps = partitions.map(_ -> long2Long(timestamp)).toMap
    consumer.offsetsForTimes(timestamps)
      .map { case (tp, offsetTimestamp) => tp -> offsetTimestampToResult(offsetTimestamp) }
      .toMap
  }

  private def offsetTimestampToResult(offsetTimestamp: OffsetAndTimestamp): Either[String, Long] = {
    Option(offsetTimestamp)
      .map(ot => Right(ot.offset()))
      .getOrElse(Left("Offset for the specified timestamp not found"))
  }
}
