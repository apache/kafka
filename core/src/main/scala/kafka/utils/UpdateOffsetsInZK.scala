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

package kafka.utils

import org.I0Itec.zkclient.ZkClient
import kafka.consumer.{SimpleConsumer, ConsumerConfig}
import kafka.cluster.Partition
import kafka.api.OffsetRequest
import java.lang.IllegalStateException

/**
 *  A utility that updates the offset of every broker partition to the offset of latest log segment file, in ZK.
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
    val cluster = ZkUtils.getCluster(zkClient)
    val partitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, List(topic).iterator)
    var partitions: List[String] = Nil

    partitionsPerTopicMap.get(topic) match {
      case Some(l) =>  partitions = l.sortWith((s,t) => s < t)
      case _ => throw new RuntimeException("Can't find topic " + topic)
    }

    var numParts = 0
    for (partString <- partitions) {
      val part = Partition.parse(partString)
      val broker = cluster.getBroker(part.brokerId) match {
        case Some(b) => b
        case None => throw new IllegalStateException("Broker " + part.brokerId + " is unavailable. Cannot issue " +
          "getOffsetsBefore request")
      }
      val consumer = new SimpleConsumer(broker.host, broker.port, 10000, 100 * 1024)
      val offsets = consumer.getOffsetsBefore(topic, part.partId, offsetOption, 1)
      val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)
      
      println("updating partition " + part.name + " with new offset: " + offsets(0))
      ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + part.name, offsets(0).toString)
      numParts += 1
    }
    println("updated the offset for " + numParts + " partitions")    
  }

  private def usage() = {
    println("USAGE: " + UpdateOffsetsInZK.getClass.getName + " [earliest | latest] consumer.properties topic")
    System.exit(1)
  }
}
