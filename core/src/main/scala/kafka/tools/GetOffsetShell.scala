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

import kafka.admin.AdminUtils
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.utils.{CommandLineUtils, Exit, ToolsUtils}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{Node, PartitionInfo, TopicPartition}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object GetOffsetShell {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
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
                           .defaultsTo(-1)
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
    val metadataTargetBrokers = AdminUtils.parseBrokerList(brokerList)
    val topic = options.valueOf(topicOpt)
    val partitionList = options.valueOf(partitionOpt)
    val time = options.valueOf(timeOpt).longValue
    val nOffsets = options.valueOf(nOffsetsOpt).intValue
    val maxWaitMs = options.valueOf(maxWaitMsOpt).intValue()

    val topicMetadata: List[PartitionInfo] =
      AdminUtils.fetchTopicMetadata(Set(topic), metadataTargetBrokers, clientId, maxWaitMs).get(topic).getOrElse(List())

    if(topicMetadata.isEmpty) {
      System.err.println(s"Error: no valid topic metadata for topic '$topic', probably the topic does not exist, run 'kafka-topics.sh --list' to verify")
      Exit.exit(1)
    }

    val groupedTopicMetadata = topicMetadata.groupBy(_.leader)

    val deserializer = (new StringDeserializer).getClass.getName
    val properties = new Properties
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer)

    groupedTopicMetadata.foreach {
      case (null, metadata) =>
        System.err.println(s"Error: These partitions do not have a leader: ${metadata.map(_.partition).mkString(", ")}. Skip getting offsets")
      case (leader, metadata) =>
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, leader.host + ":" + leader.port)
        val consumer = new KafkaConsumer(properties)
        val offsets = consumer.endOffsets(metadata.map(m => new TopicPartition(m.topic, m.partition)).asJava)
        offsets.asScala.foreach {
          offset => println(s"${offset._1.topic}:${offset._1.partition}:${offset._2}")
        }
    }
  }
}
