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
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import scala.collection.mutable
import kafka.common.TopicAndPartition

object AddPartitionsCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic for which partitions need to be added.")
      .withRequiredArg
      .describedAs("topic")
      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val nPartitionsOpt = parser.accepts("partition", "REQUIRED: Number of partitions to add to the topic")
      .withRequiredArg
      .describedAs("# of partitions")
      .ofType(classOf[java.lang.Integer])
    val replicaAssignmentOpt = parser.accepts("replica-assignment-list", "For manually assigning replicas to brokers for the new partitions")
      .withRequiredArg
      .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2, " +
      "broker_id_for_part2_replica1 : broker_id_for_part2_replica2, ...")
      .ofType(classOf[String])
      .defaultsTo("")

    val options = parser.parse(args : _*)

    for(arg <- List(topicOpt, zkConnectOpt, nPartitionsOpt)) {
      if(!options.has(arg)) {
        System.err.println("***Please note that this tool can only be used to add partitions when data for a topic does not use a key.***\n" +
          "Missing required argument. " + " \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val nPartitions = options.valueOf(nPartitionsOpt).intValue
    val replicaAssignmentStr = options.valueOf(replicaAssignmentOpt)
    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      addPartitions(zkClient, topic, nPartitions, replicaAssignmentStr)
      println("adding partitions succeeded!")
    } catch {
      case e =>
        println("adding partitions failed because of " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def addPartitions(zkClient: ZkClient, topic: String, numPartitions: Int = 1, replicaAssignmentStr: String = "") {
    val existingPartitionsReplicaList = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
    if (existingPartitionsReplicaList.size == 0)
      throw new AdminOperationException("The topic %s does not exist".format(topic))

    val existingReplicaList = existingPartitionsReplicaList.get(TopicAndPartition(topic,0)).get

    // create the new partition replication list
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    val newPartitionReplicaList = if (replicaAssignmentStr == "")
      AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, existingReplicaList.size, existingReplicaList.head, existingPartitionsReplicaList.size)
    else
      getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet, existingPartitionsReplicaList.size)

    // check if manual assignment has the right replication factor
    val unmatchedRepFactorList = newPartitionReplicaList.values.filter(p => (p.size != existingReplicaList.size))
    if (unmatchedRepFactorList.size != 0)
      throw new AdminOperationException("The replication factor in manual replication assignment " +
        " is not equal to the existing replication factor for the topic " + existingReplicaList.size)

    info("Add partition list for %s is %s".format(topic, newPartitionReplicaList))
    val partitionReplicaList = existingPartitionsReplicaList.map(p => p._1.partition -> p._2)
    // add the new list
    partitionReplicaList ++= newPartitionReplicaList
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaList, update = true)
  }

  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int], startPartitionId: Int): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    var partitionId = startPartitionId
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdminOperationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdminOperationException("duplicate brokers in replica assignment: " + brokerList)
      if (!brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdminOperationException("some specified brokers not available. specified brokers: " + brokerList.toString +
          "available broker:" + availableBrokerList.toString)
      ret.put(partitionId, brokerList.toList)
      if (ret(partitionId).size != ret(startPartitionId).size)
        throw new AdminOperationException("partition " + i + " has different replication factor: " + brokerList)
      partitionId = partitionId + 1
    }
    ret.toMap
  }
}
