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
import kafka.utils.{CommandLineUtils, CoreUtils, Logging, ToolsUtils}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}


object GetOffsetShell extends Logging {

  private val CLIENT_ID = "GetOffsetShell"

  def main(args: Array[String]): Unit = {
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
    //TODO Add support for -2(earliest) and other time values
    val time = options.valueOf(timeOpt).longValue

    //TODO Organize imports
    import collection.JavaConversions._
    //TODO Pass props to KafkaConsumer
    val extraConsumerProps = CommandLineUtils.parseKeyValueArgs(options.valuesOf(consumerPropertyOpt))

    //TODO Add support for non-provided topics
    val topics = CoreUtils.parseCsvList(topicList).toSet
    //TODO Add support for non-provided partitions
    val partitions = CoreUtils.parseCsvList(partitionList).map(_.toInt)
    val topicPartitions = for {
      topic <- topics
      partition <- partitions
    } yield new TopicPartition(topic, partition)
    //TODO Add support for -2(earliest) and other time values
    val offsets = getLastOffsets(brokerList, topicPartitions, includeInternalTopics)
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
    * Gets last offsets for given topic partitions.
    * <p>
    * This method asks the broker for existing topics and partitions. If a given partition exists on the broker,
    * then the method returns the last offset for it. Otherwise (either topic or partition does not exist on the broker)
    * the method returns a string that describes the problem.
    * <p>
    * The last offset of a partition is the offset of the latest message in this partition plus one.
    * This implementation obtains last offsets by means of [[KafkaConsumer.endOffsets KafkaConsumer.endOffsets()]].
    * In total, this implementation makes at most two requests for the broker,
    * no matter how many topics and partitions are given in the argument:
    * get available partitions and get last offsets for partitions.
    * @param bootstrapServers Bootstrap Kafka servers - a comma separated list as KafkaConsumer accepts.
    * @param topicPartitions A set of topic partitions. If empty, then offsets for all partitions of all available topics will be retrieved.
    * @param includeInternalTopics When the topic partitions argument is empty, exclude internal topics (like consumer offsets) from consideration.
    * @return A map of Either objects per each topic partition. If partition exists, then the Either Right entry contains a long offset.
    *         For a non-existing partition, the Either Left entry contains a string with the problem description.
    * @throws IllegalArgumentException Bootstraps server is null, topic partitions is null.
    */
  //TODO Eliminate magic constants
  //TODO Check ConsoleConsumer on where to add logging statements
  def getLastOffsets(bootstrapServers: String,
                     topicPartitions: Set[TopicPartition],
                     includeInternalTopics: Boolean = false): Map[TopicPartition, Either[String, Long]] = {
    import collection.JavaConversions._

    require(bootstrapServers != null, "Bootstrap servers cannot be null")
    require(topicPartitions != null, "Topic partitions cannot be null")

    val consumerConfig = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer"
    )
    val consumer: Consumer[Array[Byte], Array[Byte]] = new KafkaConsumer(consumerConfig)
    val availableTopics: Map[String, List[PartitionInfo]] = consumer.listTopics().toMap.mapValues(asScalaBuffer(_).toList)
    val (nonExistingPartitions,  existingPartitions) =
      if (topicPartitions.isEmpty)
        (Map.empty, availablePartitions(availableTopics, includeInternalTopics))
      else
        extractExistingPartitions(topicPartitions, availableTopics)
    val offsets: Map[TopicPartition, Long] = consumer.endOffsets(existingPartitions).mapValues(Long2long).toMap
    nonExistingPartitions ++ offsets.map { case (tp, offset) => tp -> Right(offset) }
  }

  /**
    * Extracts existing topic partitions from the given set of requested partitions,
    * putting existing partitions aside of non-existing partitions.
    * @param requestedPartitions The set of requested partitions. Some of them may not exist.
    * @param availableTopics The map of available topics as received from [[KafkaConsumer.listTopics KafkaConsumer.listTopics()]].
    * @return A tuple: (non-existing partitions, existing partitions). Non-existing partitions is a map,
    *         that has descriptions per each partitions in the Left object.
    */
  private def extractExistingPartitions(requestedPartitions: Set[TopicPartition],
                                availableTopics: Map[String, List[PartitionInfo]]):
                                (Map[TopicPartition, Either[String, Long]], Set[TopicPartition]) = {
    val topicsPair = requestedPartitions.partition(tp => !availableTopics.contains(tp.topic()))
    val nonExistingTopics = topicsPair._1.map(tp => tp -> Left(s"Topic not found: ${tp.topic()}")).toMap
    val partitionsPair = topicsPair._2.partition(tp => !availableTopics(tp.topic()).exists(p => p.partition() == tp.partition()))
    val nonExistingPartitions = partitionsPair._1.map(tp => tp -> Left(s"Partition for topic not found: ${tp.topic()}:${tp.partition()}")).toMap
    (nonExistingTopics ++ nonExistingPartitions, partitionsPair._2)
  }

  private def availablePartitions(availableTopics: Map[String, List[PartitionInfo]],
                                  includeInternalTopics: Boolean): Set[TopicPartition] = {
    val availablePartitions = (for {
      (topic, pinfos) <- availableTopics
      pinfo <- pinfos
    } yield (new TopicPartition(topic, pinfo.partition()))).toSet

    import kafka.common.Topic.InternalTopics
    if (includeInternalTopics)
      availablePartitions
    else
      availablePartitions.filterNot(tp => InternalTopics.contains(tp.topic()))
  }
}
