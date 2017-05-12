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
import kafka.common.Topic.InternalTopics
import kafka.utils.{CommandLineUtils, CoreUtils, Logging, ToolsUtils}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._


object GetOffsetShell extends Logging {

  private val CLIENT_ID = "GetOffsetShell"

  def main(args: Array[String]): Unit = {
    //TODO Check ConsoleConsumer on where to add logging statements
    val parser = new OptionParser
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                           .withRequiredArg
                           .describedAs("hostname:port,...,hostname:port")
                           .ofType(classOf[String])
    //TODO Rename to 'topics' to be consistent with 'partitions'
    val topicOpt = parser.accepts("topic", "The list of topics to get offsets from. If not specified, it will find offsets for all topics.")
                           .withRequiredArg
                           .describedAs("topic1,...,topicN")
                           .ofType(classOf[String])
                           .defaultsTo("")
    //TODO Don't hardcode the tool name
    val includeInternalTopicsOpt = parser.accepts("include-internal-topics", "By default, when the list if topics is not given, GetOffsetShell excludes internal topics like consumer offsets. This options forces GetOffsetShell to include them.")
    val partitionOpt = parser.accepts("partitions", "The list of partition ids. If not specified, it will find offsets for all partitions.")
                           .withRequiredArg
                           .describedAs("p1,...pM")
                           .ofType(classOf[String])
                           .defaultsTo("")
    val timeOpt = parser.accepts("time", "timestamp of the offsets before that")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1L)
    val consumerPropertyOpt = parser.accepts("consumer-property", "A mechanism to pass user-defined properties in the form key=value to the consumer.")
                           .withRequiredArg
                           .describedAs("property1=value1,...")
                           .ofType(classOf[String])
    val nOffsetsOpt = parser.accepts("offsets", "DEPRECATED AND IGNORED: Always one offset is returned for each partition. Number of offsets returned")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", s"DEPRECATED AND IGNORED: Use ${consumerPropertyOpt} and pass ${ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG} instead. The max amount of time each fetch request waits.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000)

   if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting consumer offsets.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt)

    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val topicList = options.valueOf(topicOpt)
    val includeInternalTopics = options.has(includeInternalTopicsOpt)
    val partitionList = options.valueOf(partitionOpt)
    //TODO Add support for -2(earliest) and other time values.
    //TODO Return error message when no offset for this timestamp.
    val timestamp = options.valueOf(timeOpt).longValue
    //TODO Pass props to KafkaConsumer
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt))

    //TODO Add support for non-provided topics
    val topics = CoreUtils.parseCsvList(topicList).toSet
    //TODO Add support for non-provided partitions
    val partitions = CoreUtils.parseCsvList(partitionList).map(_.toInt).toSet
   //TODO Add support for -2(earliest) and other time values
    val offsets = getOffsets(brokerList, topics, partitions, timestamp, includeInternalTopics)
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
    * @return A map of entries per each topic partition, each entry contains either the offset or an error message.
    * @throws IllegalArgumentException Bootstrap servers is null, topics is null, partitions is null.
    */
  def getOffsets(bootstrapServers: String,
                 topics: Set[String],
                 partitions: Set[Int],
                 timestamp: Long,
                 includeInternalTopics: Boolean): Map[TopicPartition, Either[String, Long]] = {
    require(bootstrapServers != null, "Bootstrap servers cannot be null")
    require(topics != null, "Topics cannot be null")
    require(partitions != null, "Partitions cannot be null")

    val consumerConfig = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    val consumer: Consumer[Array[Byte], Array[Byte]] = new KafkaConsumer(consumerConfig)

    val available = consumer.listTopics()
      .map { case (topic, pInfos) => topic -> pInfos.map(_.partition()).toSet }
      .toMap
    val (toError, toRequest) = extractExistingPartitions(topics, partitions, available, includeInternalTopics)
    val offsets = getEndOffsets(consumer, toRequest)
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
      val requestedPartitions = for {
        topic <- topics
        partition <- partitions
      } yield new TopicPartition(topic, partition)
      val availablePartitions = toPartitions(available)
      val (toError, toRequest) = requestedPartitions.partition(tp => !availablePartitions.contains(tp))
      (
        missingPartitions(toError),
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
    partitions.map(tp => tp -> Left(s"Partition for topic not found: ${tp.topic()}:${tp.partition()}")).toMap
  }

  private def missingTopics(topics: Set[String]): Map[TopicPartition, Either[String, Long]] = {
    topics.map(topic => new TopicPartition(topic, 0) -> Left(s"Topic not found: $topic")).toMap
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
      .map { case (tp, offsetTimestamp) => tp -> Right(offsetTimestamp.offset()) }.toMap
  }
}
