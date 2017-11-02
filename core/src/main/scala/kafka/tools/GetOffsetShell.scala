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

import kafka.consumer._
import joptsimple._
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.client.ClientUtils
import kafka.utils.{CommandLineUtils, Exit, ToolsUtils}


object GetOffsetShell {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val brokerListOpt = parser.accepts("broker-list", "The list of hostname and port of the server to connect to.")
                           .withRequiredArg
                           .describedAs("server(s) to connect to. e.g: hostname:port,...,hostname:port")
                           .ofType(classOf[String])
                           .required
    val topicOpt = parser.accepts("topic", "The topic to get offset from.")
                           .withRequiredArg
                           .describedAs("topic name")
                           .ofType(classOf[String])
                           .required
    val partitionOpt = parser.accepts("partitions", "Comma separated list of partition ids. If not specified, it will find offsets for all partitions.")
                           .withRequiredArg
                           .describedAs("partition id(s)")
                           .ofType(classOf[String])
                           .defaultsTo("")
    val timeOpt = parser.accepts("time", "Timestamp in milliseconds. The offsets before this timestamp will be fetched.")
                           .withRequiredArg
                           .describedAs("timestamp/-1(latest)/-2(earliest)")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(-1)
    val nOffsetsOpt = parser.accepts("offsets", "Number of offsets returned.")
                           .withRequiredArg
                           .describedAs("number of offsets returned")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    val maxWaitMsOpt = parser.accepts("max-wait-ms", "The max amount of time each fetch request waits.")
                           .withRequiredArg
                           .describedAs("max fetch request wait time (in ms)")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000)
    parser.accepts("help", "Print usage information.").forHelp
                      
   var commandDef: String = "An interactive shell for getting consumer offsets."
   if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, commandDef)

    val options = CommandLineUtils.tryParse(parser, args)

    if(options.has("help"))
        CommandLineUtils.printUsageAndDie(parser, commandDef)

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
}
