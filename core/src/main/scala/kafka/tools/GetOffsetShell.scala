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
import kafka.utils.{CommandLineUtils, Exit, ToolsUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.requests.ListOffsetRequest

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
                           .defaultsTo(-1L)
    parser.accepts("offsets", "DEPRECATED AND IGNORED: number of offsets returned")
                           .withRequiredArg
                           .describedAs("count")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    parser.accepts("max-wait-ms", "DEPRECATED AND IGNORED: The max amount of time each fetch request waits.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1000)

   if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "An interactive shell for getting consumer offsets.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, brokerListOpt, topicOpt)

    val clientId = "GetOffsetShell"
    val brokerList = options.valueOf(brokerListOpt)
    ToolsUtils.validatePortOrDie(parser, brokerList)
    val topic = options.valueOf(topicOpt)
    val partitionIdsRequested = options.valueOf(partitionOpt).split(",").map(_.toInt).toSet
    val listOffsetsTimestamp = options.valueOf(timeOpt).longValue

    val config = new Properties
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    val consumer = new KafkaConsumer(config)

    val partitionInfos = listPartitionInfos(consumer, topic, partitionIdsRequested) match {
      case None =>
        System.err.println(s"Topic $topic does not exist")
        Exit.exit(1)
      case Some(p) if p.isEmpty =>
        if (partitionIdsRequested.isEmpty)
          System.err.println(s"Topic $topic has 0 partitions")
        else
          System.err.println(s"Topic $topic does not have any of the requested partitions ${partitionIdsRequested.mkString(",")}")
        Exit.exit(1)
      case Some(p) => p
    }

    if (partitionIdsRequested.nonEmpty) {
      partitionIdsRequested.filterNot(partitionId => partitionInfos.exists(_.partition == partitionId)).foreach { partitionId =>
        s"Error: partition $partitionId does not exist"
      }
    }

    val topicPartitions = partitionInfos.flatMap { p =>
      if (p.leader == null) {
        System.err.println(s"Error: partition ${p.partition} does not have a leader. Skip getting offsets")
        None
      } else
        Some(new TopicPartition(p.topic, p.partition))
    }

    val partitionOffsets: collection.Map[TopicPartition, java.lang.Long] = listOffsetsTimestamp match {
      case ListOffsetRequest.EARLIEST_TIMESTAMP => consumer.beginningOffsets(topicPartitions.asJava).asScala
      case ListOffsetRequest.LATEST_TIMESTAMP => consumer.endOffsets(topicPartitions.asJava).asScala
      case _ =>
        val timestampsToSearch = topicPartitions.map(tp => tp -> (listOffsetsTimestamp: java.lang.Long)).toMap.asJava
        consumer.offsetsForTimes(timestampsToSearch).asScala.mapValues(_.offset)
    }

    partitionOffsets.foreach { case (tp, offset) =>
      println(s"$topic:${tp.partition}:$offset")
    }

  }

  /**
   * Return the partition infos for `topic`. If the topic does not exist, `None` is returned.
   */
  private def listPartitionInfos(consumer: KafkaConsumer[_, _], topic: String, partitionIds: Set[Int]): Option[Seq[PartitionInfo]] = {
    val partitionInfos = consumer.listTopics.asScala.filterKeys(_ == topic).values.flatMap(_.asScala).toBuffer
    if (partitionInfos.isEmpty)
      None
    else if (partitionIds.isEmpty)
      Some(partitionInfos)
    else
      Some(partitionInfos.filter(p => partitionIds.contains(p.partition)))
  }

}
