/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.tools


import joptsimple._
import org.I0Itec.zkclient.ZkClient
import kafka.utils._
import kafka.consumer.SimpleConsumer
import kafka.api.{OffsetFetchResponse, OffsetFetchRequest, OffsetRequest}
import kafka.common.{OffsetMetadataAndError, ErrorMapping, BrokerNotAvailableException, TopicAndPartition}
import scala.collection._
import kafka.client.ClientUtils
import kafka.network.BlockingChannel
import kafka.api.PartitionOffsetRequestInfo
import scala.Some
import org.I0Itec.zkclient.exception.ZkNoNodeException

object ConsumerOffsetChecker extends Logging {

  private val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()
  private val offsetMap: mutable.Map[TopicAndPartition, Long] = mutable.Map()
  private var topicPidMap: immutable.Map[String, Seq[Int]] = immutable.Map()

  private def getConsumer(zkClient: ZkClient, bid: Int): Option[SimpleConsumer] = {
    try {
      ZkUtils.readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid)._1 match {
        case Some(brokerInfoString) =>
          Json.parseFull(brokerInfoString) match {
            case Some(m) =>
              val brokerInfo = m.asInstanceOf[Map[String, Any]]
              val host = brokerInfo.get("host").get.asInstanceOf[String]
              val port = brokerInfo.get("port").get.asInstanceOf[Int]
              Some(new SimpleConsumer(host, port, 10000, 100000, "ConsumerOffsetChecker"))
            case None =>
              throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
          }
        case None =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        println("Could not parse broker info due to " + t.getCause)
        None
    }
  }

  private def processPartition(zkClient: ZkClient,
                               group: String, topic: String, pid: Int) {
    val topicPartition = TopicAndPartition(topic, pid)
    val offsetOpt = offsetMap.get(topicPartition)
    val groupDirs = new ZKGroupTopicDirs(group, topic)
    val owner = ZkUtils.readDataMaybeNull(zkClient, groupDirs.consumerOwnerDir + "/%s".format(pid))._1
    ZkUtils.getLeaderForPartition(zkClient, topic, pid) match {
      case Some(bid) =>
        val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(zkClient, bid))
        consumerOpt match {
          case Some(consumer) =>
            val topicAndPartition = TopicAndPartition(topic, pid)
            val request =
              OffsetRequest(immutable.Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
            val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head

            val lagString = offsetOpt.map(o => if (o == -1) "unknown" else (logSize - o).toString)
            println("%-15s %-30s %-3s %-15s %-15s %-15s %s".format(group, topic, pid, offsetOpt.getOrElse("unknown"), logSize, lagString.getOrElse("unknown"),
                                                                   owner match {case Some(ownerStr) => ownerStr case None => "none"}))
          case None => // ignore
        }
      case None =>
        println("No broker for partition %s - %s".format(topic, pid))
    }
  }

  private def processTopic(zkClient: ZkClient, group: String, topic: String) {
    topicPidMap.get(topic) match {
      case Some(pids) =>
        pids.sorted.foreach {
          pid => processPartition(zkClient, group, topic, pid)
        }
      case None => // ignore
    }
  }

  private def printBrokerInfo() {
    println("BROKER INFO")
    for ((bid, consumerOpt) <- consumerMap)
      consumerOpt match {
        case Some(consumer) =>
          println("%s -> %s:%d".format(bid, consumer.host, consumer.port))
        case None => // ignore
      }
  }

  def main(args: Array[String]) {
    val parser = new OptionParser()

    val zkConnectOpt = parser.accepts("zookeeper", "ZooKeeper connect string.").
            withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String])
    val topicsOpt = parser.accepts("topic",
            "Comma-separated list of consumer topics (all topics if absent).").
            withRequiredArg().ofType(classOf[String])
    val groupOpt = parser.accepts("group", "Consumer group.").
            withRequiredArg().ofType(classOf[String])
    val channelSocketTimeoutMsOpt = parser.accepts("socket.timeout.ms", "Socket timeout to use when querying for offsets.").
            withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(6000)
    val channelRetryBackoffMsOpt = parser.accepts("retry.backoff.ms", "Retry back-off to use for failed offset queries.").
            withRequiredArg().ofType(classOf[java.lang.Integer]).defaultsTo(3000)

    parser.accepts("broker-info", "Print broker info")
    parser.accepts("help", "Print this message.")
    
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Check the offset of your consumers.")

    val options = parser.parse(args : _*)

    if (options.has("help")) {
       parser.printHelpOn(System.out)
       System.exit(0)
    }

    CommandLineUtils.checkRequiredArgs(parser, options, groupOpt, zkConnectOpt)

    val zkConnect = options.valueOf(zkConnectOpt)

    val group = options.valueOf(groupOpt)
    val groupDirs = new ZKGroupDirs(group)

    val channelSocketTimeoutMs = options.valueOf(channelSocketTimeoutMsOpt).intValue()
    val channelRetryBackoffMs = options.valueOf(channelRetryBackoffMsOpt).intValue()

    val topics = if (options.has(topicsOpt)) Some(options.valueOf(topicsOpt)) else None

    var zkClient: ZkClient = null
    var channel: BlockingChannel = null
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

      val topicList = topics match {
        case Some(x) => x.split(",").view.toList
        case None => ZkUtils.getChildren(zkClient, groupDirs.consumerGroupDir +  "/owners").toList
      }

      topicPidMap = immutable.Map(ZkUtils.getPartitionsForTopics(zkClient, topicList).toSeq:_*)
      val topicPartitions = topicPidMap.flatMap { case(topic, partitionSeq) => partitionSeq.map(TopicAndPartition(topic, _)) }.toSeq
      val channel = ClientUtils.channelToOffsetManager(group, zkClient, channelSocketTimeoutMs, channelRetryBackoffMs)

      debug("Sending offset fetch request to coordinator %s:%d.".format(channel.host, channel.port))
      channel.send(OffsetFetchRequest(group, topicPartitions))
      val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().buffer)
      debug("Received offset fetch response %s.".format(offsetFetchResponse))

      offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
        if (offsetAndMetadata == OffsetMetadataAndError.NoOffset) {
          val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
          // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
          // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
          try {
            val offset = ZkUtils.readData(zkClient, topicDirs.consumerOffsetDir + "/%d".format(topicAndPartition.partition))._1.toLong
            offsetMap.put(topicAndPartition, offset)
          } catch {
            case z: ZkNoNodeException =>
              if(ZkUtils.pathExists(zkClient,topicDirs.consumerOffsetDir))
                offsetMap.put(topicAndPartition,-1)
              else
                throw z
          }
        }
        else if (offsetAndMetadata.error == ErrorMapping.NoError)
          offsetMap.put(topicAndPartition, offsetAndMetadata.offset)
        else {
          println("Could not fetch offset for %s due to %s.".format(topicAndPartition, ErrorMapping.exceptionFor(offsetAndMetadata.error)))
        }
      }
      channel.disconnect()

      println("%-15s %-30s %-3s %-15s %-15s %-15s %s".format("Group", "Topic", "Pid", "Offset", "logSize", "Lag", "Owner"))
      topicList.sorted.foreach {
        topic => processTopic(zkClient, group, topic)
      }

      if (options.has("broker-info"))
        printBrokerInfo()

      for ((_, consumerOpt) <- consumerMap)
        consumerOpt match {
          case Some(consumer) => consumer.close()
          case None => // ignore
        }
    }
    catch {
      case t: Throwable =>
        println("Exiting due to: %s.".format(t.getMessage))
    }
    finally {
      for (consumerOpt <- consumerMap.values) {
        consumerOpt match {
          case Some(consumer) => consumer.close()
          case None => // ignore
        }
      }
      if (zkClient != null)
        zkClient.close()

      if (channel != null)
        channel.disconnect()
    }
  }
}
