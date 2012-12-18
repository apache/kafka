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

import org.I0Itec.zkclient.ZkClient
import kafka.consumer.{SimpleConsumer, ConsumerConfig}
import kafka.api.{PartitionOffsetRequestInfo, OffsetRequest}
import kafka.common.{TopicAndPartition, KafkaException}
import kafka.utils.{ZKGroupTopicDirs, ZkUtils, ZKStringSerializer, Utils}


/**
 *  A utility that updates the offset of every broker partition to the offset of earliest or latest log segment file, in ZK.
 */
object UpdateOffsetsInZK {
  val Earliest = "earliest"
  val Latest = "latest"

  def main(args: Array[String]) {
    if(args.length < 3)
      usage
    val config = new ConsumerConfig(Utils.loadProps(args(1)))
    val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs,
        config.zkConnectionTimeoutMs, ZKStringSerializer)
    args(0) match {
      case Earliest => getAndSetOffsets(zkClient, OffsetRequest.EarliestTime, config, args(2))
      case Latest => getAndSetOffsets(zkClient, OffsetRequest.LatestTime, config, args(2))
      case _ => usage
    }
  }

  private def getAndSetOffsets(zkClient: ZkClient, offsetOption: Long, config: ConsumerConfig, topic: String): Unit = {
    val partitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, List(topic))
    var partitions: Seq[Int] = Nil

    partitionsPerTopicMap.get(topic) match {
      case Some(l) =>  partitions = l.sortWith((s,t) => s < t)
      case _ => throw new RuntimeException("Can't find topic " + topic)
    }

    var numParts = 0
    for (partition <- partitions) {
      val brokerHostingPartition = ZkUtils.getLeaderForPartition(zkClient, topic, partition)

      val broker = brokerHostingPartition match {
        case Some(b) => b
        case None => throw new KafkaException("Broker " + brokerHostingPartition + " is unavailable. Cannot issue " +
          "getOffsetsBefore request")
      }

      ZkUtils.getBrokerInfo(zkClient, broker) match {
        case Some(brokerInfo) =>
          val consumer = new SimpleConsumer(brokerInfo.host, brokerInfo.port, 10000, 100 * 1024, "UpdateOffsetsInZk")
          val topicAndPartition = TopicAndPartition(topic, partition)
          val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(offsetOption, 1)))
          val offset = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
          val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)

          println("updating partition " + partition + " with new offset: " + offset)
          ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partition, offset.toString)
          numParts += 1
        case None => throw new KafkaException("Broker information for broker id %d does not exist in ZK".format(broker))
      }
    }
    println("updated the offset for " + numParts + " partitions")
  }

  private def usage() = {
    println("USAGE: " + UpdateOffsetsInZK.getClass.getName + " [earliest | latest] consumer.properties topic")
    System.exit(1)
  }
}
