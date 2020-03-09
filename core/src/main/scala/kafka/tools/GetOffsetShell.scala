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

import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit, ToolsUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.JavaConverters._
import scala.collection.Seq

object GetOffsetShell {

  val helpText = "An interactive shell for getting topic offsets."
  val clientId = "GetOffsetShell"

  class GetOffsetShellOptions(args: Array[String]) extends CommandDefaultOptions(args)  {
    val brokerListOpt = parser.accepts("broker-list", "DEPRECATED, use --bootstrap-server instead; ignored if --bootstrap-server is specified. The list of hostname and port of the server to connect to in the form HOST1:PORT1,HOST2:PORT2.")
      .withRequiredArg
      .describedAs("HOST1:PORT1,...,HOST3:PORT3")
      .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED. The server(s) to connect to in the form HOST1:PORT1,HOST2:PORT2.")
      .requiredUnless("broker-list")
      .withRequiredArg
      .describedAs("HOST1:PORT1,...,HOST3:PORT3")
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
    val timeOpt = parser.accepts("time", "timestamp of the offsets before that. [Note: No offset is returned, if the timestamp greater than recently commited record timestamp is given.]")
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

    options = parser.parse(args : _*)

    def bootstrapServers: String = {
      val listOpt = if (options.has(bootstrapServerOpt))
        bootstrapServerOpt
      else
        brokerListOpt

      options.valueOf(listOpt)
    }
  }

  def validateAndParseArgs(args: Array[String]): GetOffsetShellOptions = {
    val opts = new GetOffsetShellOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(opts, helpText)

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt)

    ToolsUtils.validatePortOrDie(opts.parser, opts.bootstrapServers)
    opts
  }

  def main(args: Array[String]): Unit = {
    val opts = validateAndParseArgs(args)
    val options = opts.options

    val topic = options.valueOf(opts.topicOpt)
    val partitionIdsRequested: Set[Int] = {
      val partitionsString = options.valueOf(opts.partitionOpt)
      if (partitionsString.isEmpty)
        Set.empty
      else
        partitionsString.split(",").map { partitionString =>
          try partitionString.toInt
          catch {
            case _: NumberFormatException =>
              CommandLineUtils.printUsageAndDie(opts.parser, s"--partitions expects a comma separated list of numeric partition ids, but received: $partitionsString")
          }
        }.toSet
    }
    val listOffsetsTimestamp = options.valueOf(opts.timeOpt).longValue

    val config = new Properties
    config.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, opts.bootstrapServers)
    config.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    val consumer = new KafkaConsumer(config, new ByteArrayDeserializer, new ByteArrayDeserializer)

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
      (partitionIdsRequested -- partitionInfos.map(_.partition)).foreach { partitionId =>
        System.err.println(s"Error: partition $partitionId does not exist")
      }
    }

    val topicPartitions = partitionInfos.sortBy(_.partition).flatMap { p =>
      if (p.leader == null) {
        System.err.println(s"Error: partition ${p.partition} does not have a leader. Skip getting offsets")
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

    partitionOffsets.toSeq.sortBy { case (tp, _) => tp.partition }.foreach { case (tp, offset) =>
      println(s"$topic:${tp.partition}:${Option(offset).getOrElse("")}")
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
