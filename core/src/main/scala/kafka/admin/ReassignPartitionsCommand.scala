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
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.security.JaasUtils

object ReassignPartitionsCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new ReassignPartitionsCommandOptions(args)

    // should have exactly one action
    val actions = Seq(opts.generateOpt, opts.executeOpt, opts.verifyOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --generate, --execute or --verify")

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)

    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect,
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled())
    try {
      if(opts.options.has(opts.verifyOpt))
        verifyAssignment(zkUtils, opts)
      else if(opts.options.has(opts.generateOpt))
        generateAssignment(zkUtils, opts)
      else if (opts.options.has(opts.executeOpt))
        executeAssignment(zkUtils, opts)
    } catch {
      case e: Throwable =>
        println("Partitions reassignment failed due to " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      val zkClient = zkUtils.zkClient
      if (zkClient != null)
        zkClient.close()
    }
  }

  def verifyAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    if(!opts.options.has(opts.reassignmentJsonFileOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "If --verify option is used, command must include --reassignment-json-file that was used during the --execute option")
    val jsonFile = opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val jsonString = Utils.readFileAsString(jsonFile)
    val partitionsToBeReassigned = zkUtils.parsePartitionReassignmentData(jsonString)

    println("Status of partition reassignment:")
    val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkUtils, partitionsToBeReassigned)
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

  def generateAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    if(!(opts.options.has(opts.topicsToMoveJsonFileOpt) && opts.options.has(opts.brokerListOpt)))
      CommandLineUtils.printUsageAndDie(opts.parser, "If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options")
    val topicsToMoveJsonFile = opts.options.valueOf(opts.topicsToMoveJsonFileOpt)
    val brokerListToReassign = opts.options.valueOf(opts.brokerListOpt).split(',').map(_.toInt)
    val duplicateReassignments = CoreUtils.duplicates(brokerListToReassign)
    if (duplicateReassignments.nonEmpty)
      throw new AdminCommandFailedException("Broker list contains duplicate entries: %s".format(duplicateReassignments.mkString(",")))
    val topicsToMoveJsonString = Utils.readFileAsString(topicsToMoveJsonFile)
    val disableRackAware = opts.options.has(opts.disableRackAware)
    val (proposedAssignments, currentAssignments) = generateAssignment(zkUtils, brokerListToReassign, topicsToMoveJsonString, disableRackAware)
    println("Current partition replica assignment\n\n%s".format(zkUtils.formatAsReassignmentJson(currentAssignments)))
    println("Proposed partition reassignment configuration\n\n%s".format(zkUtils.formatAsReassignmentJson(proposedAssignments)))
  }

  def generateAssignment(zkUtils: ZkUtils, brokerListToReassign: Seq[Int], topicsToMoveJsonString: String, disableRackAware: Boolean): (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]]) = {
    val topicsToReassign = zkUtils.parseTopicsData(topicsToMoveJsonString)
    val duplicateTopicsToReassign = CoreUtils.duplicates(topicsToReassign)
    if (duplicateTopicsToReassign.nonEmpty)
      throw new AdminCommandFailedException("List of topics to reassign contains duplicate entries: %s".format(duplicateTopicsToReassign.mkString(",")))
    val currentAssignment = zkUtils.getReplicaAssignmentForTopics(topicsToReassign)

    val groupedByTopic = currentAssignment.groupBy { case (tp, _) => tp.topic }
    val rackAwareMode = if (disableRackAware) RackAwareMode.Disabled else RackAwareMode.Enforced
    val brokerMetadatas = AdminUtils.getBrokerMetadatas(zkUtils, rackAwareMode, Some(brokerListToReassign))

    val partitionsToBeReassigned = mutable.Map[TopicAndPartition, Seq[Int]]()
    groupedByTopic.foreach { case (topic, assignment) =>
      val (_, replicas) = assignment.head
      val assignedReplicas = AdminUtils.assignReplicasToBrokers(brokerMetadatas, assignment.size, replicas.size)
      partitionsToBeReassigned ++= assignedReplicas.map { case (partition, replicas) =>
        (TopicAndPartition(topic, partition) -> replicas)
      }
    }

    (partitionsToBeReassigned, currentAssignment)
  }

  def executeAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    if(!opts.options.has(opts.reassignmentJsonFileOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "If --execute option is used, command must include --reassignment-json-file that was output " + "during the --generate option")
    val reassignmentJsonFile =  opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val reassignmentJsonString = Utils.readFileAsString(reassignmentJsonFile)
    executeAssignment(zkUtils, reassignmentJsonString)
  }

  def executeAssignment(zkUtils: ZkUtils,reassignmentJsonString: String){

    val partitionsToBeReassigned = zkUtils.parsePartitionReassignmentDataWithoutDedup(reassignmentJsonString)
    if (partitionsToBeReassigned.isEmpty)
      throw new AdminCommandFailedException("Partition reassignment data file is empty")
    val duplicateReassignedPartitions = CoreUtils.duplicates(partitionsToBeReassigned.map{ case(tp,replicas) => tp})
    if (duplicateReassignedPartitions.nonEmpty)
      throw new AdminCommandFailedException("Partition reassignment contains duplicate topic partitions: %s".format(duplicateReassignedPartitions.mkString(",")))
    val duplicateEntries= partitionsToBeReassigned
      .map{ case(tp,replicas) => (tp, CoreUtils.duplicates(replicas))}
      .filter{ case (tp,duplicatedReplicas) => duplicatedReplicas.nonEmpty }
    if (duplicateEntries.nonEmpty) {
      val duplicatesMsg = duplicateEntries
        .map{ case (tp,duplicateReplicas) => "%s contains multiple entries for %s".format(tp, duplicateReplicas.mkString(",")) }
        .mkString(". ")
      throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicatesMsg))
    }
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, partitionsToBeReassigned.toMap)
    // before starting assignment, output the current replica assignment to facilitate rollback
    val currentPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(partitionsToBeReassigned.map(_._1.topic))
    println("Current partition replica assignment\n\n%s\n\nSave this to use as the --reassignment-json-file option during rollback"
      .format(zkUtils.formatAsReassignmentJson(currentPartitionReplicaAssignment)))
    // start the reassignment
    if(reassignPartitionsCommand.reassignPartitions())
      println("Successfully started reassignment of partitions %s".format(zkUtils.formatAsReassignmentJson(partitionsToBeReassigned.toMap)))
    else
      println("Failed to reassign partitions %s".format(partitionsToBeReassigned))
  }

  private def checkIfReassignmentSucceeded(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
    partitionsToBeReassigned.map { topicAndPartition =>
      (topicAndPartition._1, checkIfPartitionReassignmentSucceeded(zkUtils,topicAndPartition._1,
        topicAndPartition._2, partitionsToBeReassigned, partitionsBeingReassigned))
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition,
                                            reassignedReplicas: Seq[Int],
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(partition) => ReassignmentInProgress
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)
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
    val disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment")

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This command moves topic partitions between replicas.")

    val options = parser.parse(args : _*)
  }
}

class ReassignPartitionsCommand(zkUtils: ZkUtils, partitions: collection.Map[TopicAndPartition, collection.Seq[Int]])
  extends Logging {
  def reassignPartitions(): Boolean = {
    try {
      val validPartitions = partitions.filter(p => validatePartition(zkUtils, p._1.topic, p._1.partition))
      if(validPartitions.isEmpty) {
        false
      }
      else {
        val jsonReassignmentData = zkUtils.formatAsReassignmentJson(validPartitions)
        zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
        true
      }
    } catch {
      case ze: ZkNodeExistsException =>
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
        throw new AdminCommandFailedException("Partition reassignment currently in " +
        "progress for %s. Aborting operation".format(partitionsBeingReassigned))
      case e: Throwable => error("Admin command failed", e); false
    }
  }

  def validatePartition(zkUtils: ZkUtils, topic: String, partition: Int): Boolean = {
    // check if partition exists
    val partitionsOpt = zkUtils.getPartitionsForTopics(List(topic)).get(topic)
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
