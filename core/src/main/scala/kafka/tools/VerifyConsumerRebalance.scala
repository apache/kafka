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

import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{Logging, ZKGroupTopicDirs, ZkUtils, ZKStringSerializer}

object VerifyConsumerRebalance extends Logging {
  def main(args: Array[String]) {
    val parser = new OptionParser()

    val zkConnectOpt = parser.accepts("zk.connect", "ZooKeeper connect string.").
      withRequiredArg().defaultsTo("localhost:2181").ofType(classOf[String]);
    val groupOpt = parser.accepts("group", "Consumer group.").
      withRequiredArg().ofType(classOf[String])
    parser.accepts("help", "Print this message.")

    val options = parser.parse(args : _*)

    if (options.has("help")) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    for (opt <- List(groupOpt))
      if (!options.has(opt)) {
        System.err.println("Missing required argument: %s".format(opt))
        parser.printHelpOn(System.err)
        System.exit(1)
      }

    val zkConnect = options.valueOf(zkConnectOpt)
    val group = options.valueOf(groupOpt)

    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

      debug("zkConnect = %s; group = %s".format(zkConnect, group))

      // check if the rebalancing operation succeeded.
      try {
        if(validateRebalancingOperation(zkClient, group))
          println("Rebalance operation successful !")
        else
          println("Rebalance operation failed !")
      } catch {
        case e2: Throwable => error("Error while verifying current rebalancing operation", e2)
      }
    }
    finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  private def validateRebalancingOperation(zkClient: ZkClient, group: String): Boolean = {
    info("Verifying rebalancing operation for consumer group " + group)
    var rebalanceSucceeded: Boolean = true
    /**
     * A successful rebalancing operation would select an owner for each available partition
     * This means that for each partition registered under /brokers/topics/[topic]/[broker-id], an owner exists
     * under /consumers/[consumer_group]/owners/[topic]/[broker_id-partition_id]
     */
    val consumersPerTopicMap = ZkUtils.getConsumersPerTopic(zkClient, group)
    val partitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient, consumersPerTopicMap.keys.iterator)

    partitionsPerTopicMap.foreach { partitionsForTopic =>
      val topic = partitionsForTopic._1
      val partitions = partitionsForTopic._2
      val topicDirs = new ZKGroupTopicDirs(group, topic)
      info("Alive partitions for topic %s are %s ".format(topic, partitions.toString))
      info("Alive consumers for topic %s => %s ".format(topic, consumersPerTopicMap.get(topic)))
      val partitionsWithOwners = ZkUtils.getChildrenParentMayNotExist(zkClient, topicDirs.consumerOwnerDir)
      if(partitionsWithOwners.size == 0) {
        error("No owners for any partitions for topic " + topic)
        rebalanceSucceeded = false
      }
      debug("Children of " + topicDirs.consumerOwnerDir + " = " + partitionsWithOwners.toString)
      val consumerIdsForTopic = consumersPerTopicMap.get(topic)

      // for each available partition for topic, check if an owner exists
      partitions.foreach { partition =>
      // check if there is a node for [partition]
        if(!partitionsWithOwners.exists(p => p.equals(partition))) {
          error("No owner for topic %s partition %s".format(topic, partition))
          rebalanceSucceeded = false
        }
        // try reading the partition owner path for see if a valid consumer id exists there
        val partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + partition
        val partitionOwner = ZkUtils.readDataMaybeNull(zkClient, partitionOwnerPath)
        if(partitionOwner == null) {
          error("No owner for topic %s partition %s".format(topic, partition))
          rebalanceSucceeded = false
        }
        else {
          // check if the owner is a valid consumer id
          consumerIdsForTopic match {
            case Some(consumerIds) =>
              if(!consumerIds.contains(partitionOwner)) {
                error("Owner %s for topic %s partition %s is not a valid member of consumer " +
                  "group %s".format(partitionOwner, topic, partition, group))
                rebalanceSucceeded = false
              }
              else
                info("Owner of topic %s partition %s is %s".format(topic, partition, partitionOwner))
            case None => {
              error("No consumer ids registered for topic " + topic)
              rebalanceSucceeded = false
            }
          }
        }
      }

    }

    rebalanceSucceeded
  }
}
