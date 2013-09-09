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
import kafka.common.Topic

object CreateTopicCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val topicOpt = parser.accepts("topic", "REQUIRED: The topic to be created.")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])
    val nPartitionsOpt = parser.accepts("partition", "number of partitions in the topic")
                           .withRequiredArg
                           .describedAs("# of partitions")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    val replicationFactorOpt = parser.accepts("replica", "replication factor for each partitions in the topic")
                           .withRequiredArg
                           .describedAs("replication factor")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
    val replicaAssignmentOpt = parser.accepts("replica-assignment-list", "for manually assigning replicas to brokers")
                           .withRequiredArg
                           .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2, " +
                                        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2, ...")
                           .ofType(classOf[String])
                           .defaultsTo("")

    val options = parser.parse(args : _*)

    for(arg <- List(topicOpt, zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    val topic = options.valueOf(topicOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val nPartitions = options.valueOf(nPartitionsOpt).intValue
    val replicationFactor = options.valueOf(replicationFactorOpt).intValue
    val replicaAssignmentStr = options.valueOf(replicaAssignmentOpt)
    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      createTopic(zkClient, topic, nPartitions, replicationFactor, replicaAssignmentStr)
      println("creation succeeded!")
    } catch {
      case e: Throwable =>
        println("creation failed because of " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def createTopic(zkClient: ZkClient, topic: String, numPartitions: Int = 1, replicationFactor: Int = 1, replicaAssignmentStr: String = "") {
    Topic.validate(topic)

    val brokerList = ZkUtils.getSortedBrokerList(zkClient)

    val partitionReplicaAssignment = if (replicaAssignmentStr == "")
      AdminUtils.assignReplicasToBrokers(brokerList, numPartitions, replicationFactor)
    else
      getManualReplicaAssignment(replicaAssignmentStr, brokerList.toSet)
    debug("Replica assignment list for %s is %s".format(topic, partitionReplicaAssignment))
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(topic, partitionReplicaAssignment, zkClient)
  }

  def getManualReplicaAssignment(replicaAssignmentList: String, availableBrokerList: Set[Int]): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      if (brokerList.size <= 0)
        throw new AdministrationException("replication factor must be larger than 0")
      if (brokerList.size != brokerList.toSet.size)
        throw new AdministrationException("duplicate brokers in replica assignment: " + brokerList)
      if (!brokerList.toSet.subsetOf(availableBrokerList))
        throw new AdministrationException("some specified brokers not available. specified brokers: " + brokerList.toString +
                "available broker:" + availableBrokerList.toString)
      ret.put(i, brokerList.toList)
      if (ret(i).size != ret(0).size)
        throw new AdministrationException("partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap
  }
}
