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
import kafka.api.{OffsetRequest, OffsetResponse, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.cluster.BrokerEndPoint
import kafka.common.TopicAndPartition
import kafka.consumer._
import kafka.utils.{CommandLineUtils, Logging, ToolsUtils}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.Seq


object GetOffsetShell extends Logging {

  private val CLIENT_ID = "GetOffsetShell"

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The list of hostname and port of the server to connect to.")
                           .withRequiredArg
                           .describedAs("hostname:port,...,hostname:port")
                           .ofType(classOf[String])
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to get offset from.")
                           .withRequiredArg
                           .describedAs("topic")
                           .ofType(classOf[String])
    val partitionOpt = parser.accepts("partitions", "comma separated list of partition ids. If not specified, it will find offsets for all partitions")
                           .withRequiredArg
                           .describedAs("partition ids")
                           .ofType(classOf[String])
                           .defaultsTo("")
    val timeOpt = parser.accepts("time", "timestamp of the offsets before that")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1L)
    val nOffsetsOpt = parser.accepts("offsets", "number of offsets returned")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000)

   if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting consumer offsets.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt, topicOpt)

    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
    val topic = options.valueOf(topicOpt)
    val partitionList = options.valueOf(partitionOpt)
    val time = options.valueOf(timeOpt).longValue
    val nOffsets = options.valueOf(nOffsetsOpt).intValue
    val maxWaitMs = options.valueOf(maxWaitMsOpt).intValue()

    val partitions: Set[Int] = partitionList match {
      case "" => getTopicPartitionsFromMetadata(topic, metadataTargetBrokers, maxWaitMs)
      case _ => partitionList.split(',').map(_.toInt).toSet
    }

    /*TODO This implementation does not handle the situation when the first broker in the list is down.
    * After switching to KafkaConsumer, this problem will be resolved. */
    val targetBroker = metadataTargetBrokers.head
    val offsets = getOffsets(targetBroker.host,
      targetBroker.port,
      partitions.map(new TopicPartition(topic, _)),
      time,
      nOffsets)
    val report = offsets
      .toList.sortBy(e => (e._1.topic, e._1.partition))
      .map { case (topicPartition, errorOffsets) => errorOffsets match {
        case Right(offsets) => "%s:%d:%s".format(topicPartition.topic, topicPartition.partition, offsets.mkString(","))
        case Left(error) => "%s:%d: Exception: %s".format(topicPartition.topic, topicPartition.partition, error.exception.getMessage)
      }
      }
      .mkString("\n")
    println(report)
  }

  /**
    * TODO Complete the doc
    * TODO Cover with tests
    *
    * @param topic
    * @param brokers
    * @param timeoutMs
    * @throws InvalidTopicException
    */
  def getTopicPartitionsFromMetadata(topic: String, brokers: Seq[BrokerEndPoint], timeoutMs: Int): Set[Int] = {
    require(topic != null, "Topic name cannot be null")
    require(!topic.isEmpty, "Topic name cannot be an empty string")
    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), brokers, CLIENT_ID, timeoutMs).topicsMetadata
    if (topicsMetadata.size != 1 || !topic.equals(topicsMetadata.head.topic)) {
      throw new InvalidTopicException(("Error: no valid topic metadata for topic: %s, " + " probably the topic does not exist, run ").format(topic) +
        "kafka-list-topic.sh to verify")
    }
    topicsMetadata.head.partitionsMetadata.map(_.partitionId).toSet
  }

  /**
    * XXX Makes no sense to ask a leader of each partition. We are interested in offsets that we can read.
    * If a message is not replicated yet from the leader to replicas, we don't count it.
    * Just one request for all topics and partitions - it should be much faster.
    *
    * TODO Implement and test corner cases.
    * TODO Complete the doc.
    *
    * @param host
    * @param port
    * @param topicPartitions
    * @param time
    * @param maxNumOffsets
    * @return
    */
  def getOffsets(host: String,
                 port: Int,
                 topicPartitions: Set[TopicPartition],
                 time: Long,
                 maxNumOffsets: Int): Map[TopicPartition, Either[Errors, Seq[Long]]] = {
    //TODO Eliminate magic constants
    val consumer = new SimpleConsumer(host, port, 10000, 100000, CLIENT_ID)
    val partitionOffsetRequestInfo = PartitionOffsetRequestInfo(time, maxNumOffsets)
    val requestInfo: Map[TopicAndPartition, PartitionOffsetRequestInfo] = topicPartitions
      .map(tp => TopicAndPartition(tp.topic, tp.partition) -> partitionOffsetRequestInfo)
      .toMap
    val request = OffsetRequest(requestInfo)
    val offsetResponse: OffsetResponse = consumer.getOffsetsBefore(request)
    offsetResponse.partitionErrorAndOffsets
      .map { case (tp, partitionOffsetsResponse) => partitionOffsetsResponse.error match {
        case Errors.NONE => new TopicPartition(tp.topic, tp.partition) -> Right(partitionOffsetsResponse.offsets)
        case _ => new TopicPartition(tp.topic, tp.partition) -> Left(partitionOffsetsResponse.error)
      }
      }
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
    * @param topicPartitions A set of topic partitions.
    * @return A map of Either objects per each topic partition. If partition exists, then the Either Right entry contains a long offset.
    *         For a non-existing partition, the Either Left entry contains a string with the problem description.
    */
  def getLastOffsets(bootstrapServers: String, topicPartitions: Set[TopicPartition]): Map[TopicPartition, Either[String, Long]] = {
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
    val (nonExistingPartitions,  existingPartitions) = extractExistingPartitions(topicPartitions, availableTopics)
    val offsets: Map[TopicPartition, Long] = consumer.endOffsets(existingPartitions).mapValues(Long2long).toMap
    nonExistingPartitions ++ offsets.map { case (tp, offset) => tp -> Right(offset) }
  }

  /**
    * Extracts existing topic partitions, putting them aside of non-existing partitions.
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
}
