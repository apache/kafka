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
import collection._
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.common.{TopicAndPartition, AdminCommandFailedException}

object ReassignPartitionsCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val topicsToMoveJsonFileOpt = parser.accepts("topics-to-move-json-file", "The JSON file with the list of topics to reassign." +
      "This option or manual-assignment-json-file needs to be specified. The format to use is - \n" +
       "{\"topics\":\n\t[{\"topic\": \"foo\"},{\"topic\": \"foo1\"}],\n\"version\":1\n}")
      .withRequiredArg
      .describedAs("topics to reassign json file path")
      .ofType(classOf[String])

    val manualAssignmentJsonFileOpt = parser.accepts("manual-assignment-json-file", "The JSON file with the list of manual reassignments" +
      "This option or topics-to-move-json-file needs to be specified. The format to use is - \n" +
      "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3] }],\n\"version\":1\n}")
      .withRequiredArg
      .describedAs("manual assignment json file path")
      .ofType(classOf[String])

    val brokerListOpt = parser.accepts("broker-list", "The list of brokers to which the partitions need to be reassigned" +
      " in the form \"0,1,2\". This is required for automatic topic reassignment.")
      .withRequiredArg
      .describedAs("brokerlist")
      .ofType(classOf[String])

    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val executeOpt = parser.accepts("execute", "This option does the actual reassignment. By default, the tool does a dry run")
      .withOptionalArg()
      .describedAs("execute")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    for(arg <- List(zkConnectOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"")
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    if (options.has(topicsToMoveJsonFileOpt) && options.has(manualAssignmentJsonFileOpt)) {
      System.err.println("Only one of the json files should be specified")
      parser.printHelpOn(System.err)
      System.exit(1)
    }

    val zkConnect = options.valueOf(zkConnectOpt)
    var zkClient: ZkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
    try {

      var partitionsToBeReassigned : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()

      if(options.has(topicsToMoveJsonFileOpt)) {
        val topicsToMoveJsonFile = options.valueOf(topicsToMoveJsonFileOpt)
        val brokerList = options.valueOf(brokerListOpt)
        val topicsToMoveJsonString = Utils.readFileAsString(topicsToMoveJsonFile)
        val brokerListToReassign = brokerList.split(',') map (_.toInt)
        val topicsToReassign = ZkUtils.parseTopicsData(topicsToMoveJsonString)
        val topicPartitionsToReassign = ZkUtils.getReplicaAssignmentForTopics(zkClient, topicsToReassign)

        val groupedByTopic = topicPartitionsToReassign.groupBy(tp => tp._1.topic)
        groupedByTopic.foreach { topicInfo =>
          val assignedReplicas = AdminUtils.assignReplicasToBrokers(brokerListToReassign, topicInfo._2.size,
            topicInfo._2.head._2.size)
          partitionsToBeReassigned ++= assignedReplicas.map(replicaInfo => (TopicAndPartition(topicInfo._1, replicaInfo._1) -> replicaInfo._2))
        }

      } else if (options.has(manualAssignmentJsonFileOpt)) {
        val manualAssignmentJsonFile =  options.valueOf(manualAssignmentJsonFileOpt)
        val manualAssignmentJsonString = Utils.readFileAsString(manualAssignmentJsonFile)
        partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentData(manualAssignmentJsonString)
        if (partitionsToBeReassigned.isEmpty)
          throw new AdminCommandFailedException("Partition reassignment data file %s is empty".format(manualAssignmentJsonFileOpt))
      } else {
        System.err.println("Missing json file. One of the file needs to be specified")
        parser.printHelpOn(System.err)
        System.exit(1)
      }

      if (options.has(executeOpt)) {
        val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, partitionsToBeReassigned)

        if(reassignPartitionsCommand.reassignPartitions())
          println("Successfully started reassignment of partitions %s".format(partitionsToBeReassigned))
        else
          println("Failed to reassign partitions %s".format(partitionsToBeReassigned))
      } else {
        System.out.println("This is a dry run (Use --execute to do the actual reassignment. " +
          "The replica assignment is \n" + partitionsToBeReassigned.toString())
      }
    } catch {
      case e: Throwable =>
        println("Partitions reassignment failed due to " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }
}

class ReassignPartitionsCommand(zkClient: ZkClient, partitions: collection.Map[TopicAndPartition, collection.Seq[Int]])
  extends Logging {
  def reassignPartitions(): Boolean = {
    try {
      val validPartitions = partitions.filter(p => validatePartition(zkClient, p._1.topic, p._1.partition))
      val jsonReassignmentData = ZkUtils.getPartitionReassignmentZkData(validPartitions)
      ZkUtils.createPersistentPath(zkClient, ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
      true
    } catch {
      case ze: ZkNodeExistsException =>
        val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient)
        throw new AdminCommandFailedException("Partition reassignment currently in " +
        "progress for %s. Aborting operation".format(partitionsBeingReassigned))
      case e: Throwable => error("Admin command failed", e); false
    }
  }

  def validatePartition(zkClient: ZkClient, topic: String, partition: Int): Boolean = {
    // check if partition exists
    val partitionsOpt = ZkUtils.getPartitionsForTopics(zkClient, List(topic)).get(topic)
    partitionsOpt match {
      case Some(partitions) =>
        if(partitions.contains(partition)) {
          true
        }else{
          error("Skipping reassignment of partition [%s,%d] ".format(topic, partition) +
            "since it doesn't exist")
          false
        }
      case None => error("Skipping reassignment of partition " +
        "[%s,%d] since topic %s doesn't exist".format(topic, partition, topic))
        false
    }
  }
}

sealed trait ReassignmentStatus { def status: Int }
case object ReassignmentCompleted extends ReassignmentStatus { val status = 1 }
case object ReassignmentInProgress extends ReassignmentStatus { val status = 0 }
case object ReassignmentFailed extends ReassignmentStatus { val status = -1 }
