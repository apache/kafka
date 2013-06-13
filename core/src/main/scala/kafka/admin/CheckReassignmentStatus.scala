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
import kafka.utils._
import scala.collection.Map
import kafka.common.TopicAndPartition

object CheckReassignmentStatus extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val jsonFileOpt = parser.accepts("path-to-json-file", "REQUIRED: The JSON file with the list of partitions and the " +
      "new replicas they should be reassigned to")
      .withRequiredArg
      .describedAs("partition reassignment json file path")
      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    for(arg <- List(jsonFileOpt, zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val jsonFile = options.valueOf(jsonFileOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val jsonString = Utils.readFileAsString(jsonFile)
    val zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)

    try {
      // read the json file into a string
      val partitionsToBeReassigned = Json.parseFull(jsonString) match {
        case Some(reassignedPartitions) =>
          val partitions = reassignedPartitions.asInstanceOf[Array[Map[String, String]]]
          partitions.map { m =>
            val topic = m.asInstanceOf[Map[String, String]].get("topic").get
            val partition = m.asInstanceOf[Map[String, String]].get("partition").get.toInt
            val replicasList = m.asInstanceOf[Map[String, String]].get("replicas").get
            val newReplicas = replicasList.split(",").map(_.toInt)
            (TopicAndPartition(topic, partition), newReplicas.toSeq)
          }.toMap
        case None => Map.empty[TopicAndPartition, Seq[Int]]
      }

      val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkClient, partitionsToBeReassigned)
      reassignedPartitionsStatus.foreach { partition =>
        partition._2 match {
          case ReassignmentCompleted =>
            println("Partition %s reassignment completed successfully".format(partition._1))
          case ReassignmentFailed =>
            println("Partition %s reassignment failed".format(partition._1))
          case ReassignmentInProgress =>
            println("Partition %s reassignment in progress".format(partition._1))
        }
      }
    }
  }

  def checkIfReassignmentSucceeded(zkClient: ZkClient, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas)
    // for all partitions whose replica reassignment is complete, check the status
    partitionsToBeReassigned.map { topicAndPartition =>
      (topicAndPartition._1, checkIfPartitionReassignmentSucceeded(zkClient, topicAndPartition._1,
        topicAndPartition._2, partitionsToBeReassigned, partitionsBeingReassigned))
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkClient: ZkClient, topicAndPartition: TopicAndPartition,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if AR == RAR
        val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition)
        if(assignedReplicas == newReplicas)
          ReassignmentCompleted
        else
          ReassignmentFailed
    }
  }
}