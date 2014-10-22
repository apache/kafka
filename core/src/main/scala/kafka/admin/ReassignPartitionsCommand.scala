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

    val opts = new ReassignPartitionsCommandOptions(args)

    // should have exactly one action
    val actions = Seq(opts.generateOpt, opts.executeOpt, opts.verifyOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --generate, --execute or --verify")

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)

    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    var zkClient: ZkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
    try {
      if(opts.options.has(opts.verifyOpt))
        verifyAssignment(zkClient, opts)
      else if(opts.options.has(opts.generateOpt))
        generateAssignment(zkClient, opts)
      else if (opts.options.has(opts.executeOpt))
        executeAssignment(zkClient, opts)
    } catch {
      case e: Throwable =>
        println("Partitions reassignment failed due to " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def verifyAssignment(zkClient: ZkClient, opts: ReassignPartitionsCommandOptions) {
    if(!opts.options.has(opts.reassignmentJsonFileOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "If --verify option is used, command must include --reassignment-json-file that was used during the --execute option")
    val jsonFile = opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val jsonString = Utils.readFileAsString(jsonFile)
    val partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentData(jsonString)

    println("Status of partition reassignment:")
    val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkClient, partitionsToBeReassigned)
    reassignedPartitionsStatus.foreach { partition =>
      partition._2 match {
        case ReassignmentCompleted =>
          println("Reassignment of partition %s completed successfully".format(partition._1))
        case ReassignmentFailed =>
          println("Reassignment of partition %s failed".format(partition._1))
        case ReassignmentInProgress =>
          println("Reassignment of partition %s is still in progress".format(partition._1))
      }
    }
  }

  def generateAssignment(zkClient: ZkClient, opts: ReassignPartitionsCommandOptions) {
    if(!(opts.options.has(opts.topicsToMoveJsonFileOpt) && opts.options.has(opts.brokerListOpt)))
      CommandLineUtils.printUsageAndDie(opts.parser, "If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options")
    val topicsToMoveJsonFile = opts.options.valueOf(opts.topicsToMoveJsonFileOpt)
    val brokerListToReassign = opts.options.valueOf(opts.brokerListOpt).split(',').map(_.toInt)
    val duplicateReassignments = Utils.duplicates(brokerListToReassign)
    if (duplicateReassignments.nonEmpty)
      throw new AdminCommandFailedException("Broker list contains duplicate entries: %s".format(duplicateReassignments.mkString(",")))
    val topicsToMoveJsonString = Utils.readFileAsString(topicsToMoveJsonFile)
    val topicsToReassign = ZkUtils.parseTopicsData(topicsToMoveJsonString)
    val duplicateTopicsToReassign = Utils.duplicates(topicsToReassign)
    if (duplicateTopicsToReassign.nonEmpty)
      throw new AdminCommandFailedException("List of topics to reassign contains duplicate entries: %s".format(duplicateTopicsToReassign.mkString(",")))
    val topicPartitionsToReassign = ZkUtils.getReplicaAssignmentForTopics(zkClient, topicsToReassign)

    var partitionsToBeReassigned : Map[TopicAndPartition, Seq[Int]] = new mutable.HashMap[TopicAndPartition, List[Int]]()
    val groupedByTopic = topicPartitionsToReassign.groupBy(tp => tp._1.topic)
    groupedByTopic.foreach { topicInfo =>
      val assignedReplicas = AdminUtils.assignReplicasToBrokers(brokerListToReassign, topicInfo._2.size,
        topicInfo._2.head._2.size)
      partitionsToBeReassigned ++= assignedReplicas.map(replicaInfo => (TopicAndPartition(topicInfo._1, replicaInfo._1) -> replicaInfo._2))
    }
    val currentPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, partitionsToBeReassigned.map(_._1.topic).toSeq)
    println("Current partition replica assignment\n\n%s"
      .format(ZkUtils.getPartitionReassignmentZkData(currentPartitionReplicaAssignment)))
    println("Proposed partition reassignment configuration\n\n%s".format(ZkUtils.getPartitionReassignmentZkData(partitionsToBeReassigned)))
  }

  def executeAssignment(zkClient: ZkClient, opts: ReassignPartitionsCommandOptions) {
    if(!opts.options.has(opts.reassignmentJsonFileOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "If --execute option is used, command must include --reassignment-json-file that was output " + "during the --generate option")
    val reassignmentJsonFile =  opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val reassignmentJsonString = Utils.readFileAsString(reassignmentJsonFile)
    val partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentDataWithoutDedup(reassignmentJsonString)
    if (partitionsToBeReassigned.isEmpty)
      throw new AdminCommandFailedException("Partition reassignment data file %s is empty".format(reassignmentJsonFile))
    val duplicateReassignedPartitions = Utils.duplicates(partitionsToBeReassigned.map{ case(tp,replicas) => tp})
    if (duplicateReassignedPartitions.nonEmpty)
      throw new AdminCommandFailedException("Partition reassignment contains duplicate topic partitions: %s".format(duplicateReassignedPartitions.mkString(",")))
    val duplicateEntries= partitionsToBeReassigned
      .map{ case(tp,replicas) => (tp, Utils.duplicates(replicas))}
      .filter{ case (tp,duplicatedReplicas) => duplicatedReplicas.nonEmpty }
    if (duplicateEntries.nonEmpty) {
      val duplicatesMsg = duplicateEntries
        .map{ case (tp,duplicateReplicas) => "%s contains multiple entries for %s".format(tp, duplicateReplicas.mkString(",")) }
        .mkString(". ")
      throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicatesMsg))
    }
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkClient, partitionsToBeReassigned.toMap)
    // before starting assignment, output the current replica assignment to facilitate rollback
    val currentPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, partitionsToBeReassigned.map(_._1.topic).toSeq)
    println("Current partition replica assignment\n\n%s\n\nSave this to use as the --reassignment-json-file option during rollback"
      .format(ZkUtils.getPartitionReassignmentZkData(currentPartitionReplicaAssignment)))
    // start the reassignment
    if(reassignPartitionsCommand.reassignPartitions())
      println("Successfully started reassignment of partitions %s".format(ZkUtils.getPartitionReassignmentZkData(partitionsToBeReassigned.toMap)))
    else
      println("Failed to reassign partitions %s".format(partitionsToBeReassigned))
  }

  private def checkIfReassignmentSucceeded(zkClient: ZkClient, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = ZkUtils.getPartitionsBeingReassigned(zkClient).mapValues(_.newReplicas)
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
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = ZkUtils.getReplicasForPartition(zkClient, topicAndPartition.topic, topicAndPartition.partition)
        if(assignedReplicas == newReplicas)
          ReassignmentCompleted
        else {
          println(("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
            " for partition %s").format(assignedReplicas.mkString(","), newReplicas.mkString(","), topicAndPartition))
          ReassignmentFailed
        }
    }
  }

  class ReassignPartitionsCommandOptions(args: Array[String]) {
    val parser = new OptionParser

    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
                      "form host:port. Multiple URLS can be given to allow fail-over.")
                      .withRequiredArg
                      .describedAs("urls")
                      .ofType(classOf[String])
    val generateOpt = parser.accepts("generate", "Generate a candidate partition reassignment configuration." +
      " Note that this only generates a candidate assignment, it does not execute it.")
    val executeOpt = parser.accepts("execute", "Kick off the reassignment as specified by the --reassignment-json-file option.")
    val verifyOpt = parser.accepts("verify", "Verify if the reassignment completed as specified by the --reassignment-json-file option.")
    val reassignmentJsonFileOpt = parser.accepts("reassignment-json-file", "The JSON file with the partition reassignment configuration" +
                      "The format to use is - \n" +
                      "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3] }],\n\"version\":1\n}")
                      .withRequiredArg
                      .describedAs("manual assignment json file path")
                      .ofType(classOf[String])
    val topicsToMoveJsonFileOpt = parser.accepts("topics-to-move-json-file", "Generate a reassignment configuration to move the partitions" +
                      " of the specified topics to the list of brokers specified by the --broker-list option. The format to use is - \n" +
                      "{\"topics\":\n\t[{\"topic\": \"foo\"},{\"topic\": \"foo1\"}],\n\"version\":1\n}")
                      .withRequiredArg
                      .describedAs("topics to reassign json file path")
                      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "The list of brokers to which the partitions need to be reassigned" +
                      " in the form \"0,1,2\". This is required if --topics-to-move-json-file is used to generate reassignment configuration")
                      .withRequiredArg
                      .describedAs("brokerlist")
                      .ofType(classOf[String])
                      
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This command moves topic partitions between replicas.")

    val options = parser.parse(args : _*)
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
