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

package kafka.admin

import joptsimple.OptionParser
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{Utils, ZKStringSerializer, ZkUtils}

object ListTopicCommand {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to be listed. Defaults to all existing topics.")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
                         .defaultsTo("")
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])
    val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                                                            "if set, only show under replicated partitions")
    val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                                                            "if set, only show partitions whose leader is not available")

    val options = parser.parse(args : _*)

    for(arg <- List(zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val reportUnderReplicatedPartitions = if (options.has(reportUnderReplicatedPartitionsOpt)) true else false
    val reportUnavailablePartitions = if (options.has(reportUnavailablePartitionsOpt)) true else false
    var zkClient: ZkClient = null
    try {
      var topicList: Seq[String] = Nil
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

      if (topic == "")
        topicList = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath).sorted
      else
        topicList = List(topic)

      if (topicList.size <= 0)
        println("no topics exist!")

      val liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).map(_.id).toSet
      for (t <- topicList)
        showTopic(t, zkClient, reportUnderReplicatedPartitions, reportUnavailablePartitions, liveBrokers)
    }
    catch {
      case e: Throwable =>
        println("list topic failed because of " + e.getMessage)
        println(Utils.stackTrace(e))
    }
    finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def showTopic(topic: String, zkClient: ZkClient, reportUnderReplicatedPartitions: Boolean,
                reportUnavailablePartitions: Boolean, liveBrokers: Set[Int]) {
    ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic) match {
      case Some(topicPartitionAssignment) =>
        val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
        for ((partitionId, assignedReplicas) <- sortedPartitions) {
          val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionId)
          val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionId)
          if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
              (reportUnderReplicatedPartitions && inSyncReplicas.size < assignedReplicas.size) ||
              (reportUnavailablePartitions && (!leader.isDefined || !liveBrokers.contains(leader.get)))) {
            print("topic: " + topic)
            print("\tpartition: " + partitionId)
            print("\tleader: " + (if(leader.isDefined) leader.get else "none"))
            print("\treplicas: " + assignedReplicas.mkString(","))
            println("\tisr: " + inSyncReplicas.mkString(","))
          }
        }
      case None =>
        println("topic " + topic + " doesn't exist!")
    }
  }
}