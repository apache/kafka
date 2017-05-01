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
import kafka.common.TopicAndPartition
import kafka.consumer._
import kafka.utils.{CommandLineUtils, Exit, Logging, ToolsUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors


object GetOffsetShell extends Logging {

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

    val clientId = "GetOffsetShell"
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val metadataTargetBrokers = ClientUtils.parseBrokerList(brokerList)
    val topic = options.valueOf(topicOpt)
    val partitionList = options.valueOf(partitionOpt)
    val time = options.valueOf(timeOpt).longValue
    val nOffsets = options.valueOf(nOffsetsOpt).intValue
    val maxWaitMs = options.valueOf(maxWaitMsOpt).intValue()

    val topicsMetadata = ClientUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).topicsMetadata
    if(topicsMetadata.size != 1 || !topicsMetadata.head.topic.equals(topic)) {
      System.err.println(("Error: no valid topic metadata for topic: %s, " + " probably the topic does not exist, run ").format(topic) +
        "kafka-list-topic.sh to verify")
      Exit.exit(1)
    }
    val partitions =
      if(partitionList == "") {
        topicsMetadata.head.partitionsMetadata.map(_.partitionId)
      } else {
        partitionList.split(",").map(_.toInt).toSeq
      }
    partitions.foreach { partitionId =>
      val partitionMetadataOpt = topicsMetadata.head.partitionsMetadata.find(_.partitionId == partitionId)
      partitionMetadataOpt match {
        case Some(metadata) =>
          metadata.leader match {
            case Some(leader) =>
              val consumer = new SimpleConsumer(leader.host, leader.port, 10000, 100000, clientId)
              val topicAndPartition = TopicAndPartition(topic, partitionId)
              val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
              val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets

              println("%s:%d:%s".format(topic, partitionId, offsets.mkString(",")))
            case None => System.err.println("Error: partition %d does not have a leader. Skip getting offsets".format(partitionId))
          }
        case None => System.err.println("Error: partition %d does not exist".format(partitionId))
      }
    }
  }

  /**
    * XXX Makes no sense to ask a leader of each partition. We are interested in offsets that we can read.
    * If a message is not replicated yet from the leader to replicas, we don't count it.
    * Just one request for all topics and partitions - it should be much faster.
    *
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
    val consumer = new SimpleConsumer(host, port, 10000, 100000, "GetOffsetShell")
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
}
