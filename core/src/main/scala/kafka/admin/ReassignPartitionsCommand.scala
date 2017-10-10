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

import java.util.Properties
import java.util.concurrent.ExecutionException

import scala.collection._
import scala.collection.JavaConverters._
import kafka.server.{ConfigType, DynamicConfig}
import kafka.utils._
import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.log.LogConfig
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.TopicPartitionReplica
import org.apache.kafka.common.errors.{LogDirNotFoundException, ReplicaNotAvailableException}
import org.apache.kafka.clients.admin.{AdminClientConfig, AlterReplicaLogDirsOptions, AdminClient => JAdminClient}
import LogConfig._
import joptsimple.OptionParser
import org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult.ReplicaLogDirInfo

object ReassignPartitionsCommand extends Logging {

  case class Throttle(value: Long, postUpdateAction: () => Unit = () => ())

  private[admin] val NoThrottle = Throttle(-1)
  private[admin] val AnyLogDir = "any"

  def main(args: Array[String]): Unit = {

    val opts = validateAndParseArgs(args)
    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect,
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled())
    val adminClientOpt = createAdminClient(opts)

    try {
      if(opts.options.has(opts.verifyOpt))
        verifyAssignment(zkUtils, adminClientOpt, opts)
      else if(opts.options.has(opts.generateOpt))
        generateAssignment(zkUtils, opts)
      else if (opts.options.has(opts.executeOpt))
        executeAssignment(zkUtils, adminClientOpt, opts)
    } catch {
      case e: Throwable =>
        println("Partitions reassignment failed due to " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally zkUtils.close()
  }

  private def createAdminClient(opts: ReassignPartitionsCommandOptions): Option[JAdminClient] = {
    if (opts.options.has(opts.bootstrapServerOpt)) {
      val props = new Properties()
      props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
      props.put(AdminClientConfig.CLIENT_ID_CONFIG, "reassign-partitions-tool")
      Some(JAdminClient.create(props))
    } else {
      None
    }
  }

  def verifyAssignment(zkUtils: ZkUtils, adminClientOpt: Option[JAdminClient], opts: ReassignPartitionsCommandOptions) {
    val jsonFile = opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val jsonString = Utils.readFileAsString(jsonFile)
    verifyAssignment(zkUtils, adminClientOpt, jsonString)
  }

  def verifyAssignment(zkUtils: ZkUtils, adminClientOpt: Option[JAdminClient], jsonString: String): Unit = {
    println("Status of partition reassignment: ")
    val (partitionsToBeReassigned, replicaAssignment) = parsePartitionReassignmentData(jsonString)
    val reassignedPartitionsStatus = checkIfPartitionReassignmentSucceeded(zkUtils, partitionsToBeReassigned.toMap)
    val replicaReassignmentStatus = checkIfReplicaReassignmentSucceeded(adminClientOpt, replicaAssignment)

    reassignedPartitionsStatus.foreach { case (topicPartition, status) =>
      status match {
        case ReassignmentCompleted =>
          println("Reassignment of partition %s completed successfully".format(topicPartition))
        case ReassignmentFailed =>
          println("Reassignment of partition %s failed".format(topicPartition))
        case ReassignmentInProgress =>
          println("Reassignment of partition %s is still in progress".format(topicPartition))
      }
    }

    replicaReassignmentStatus.foreach { case (replica, status) =>
      status match {
        case ReassignmentCompleted =>
          println("Reassignment of replica %s completed successfully".format(replica))
        case ReassignmentFailed =>
          println("Reassignment of replica %s failed".format(replica))
        case ReassignmentInProgress =>
          println("Reassignment of replica %s is still in progress".format(replica))
      }
    }

    removeThrottle(zkUtils, partitionsToBeReassigned.toMap, reassignedPartitionsStatus)
  }

  private[admin] def removeThrottle(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]], reassignedPartitionsStatus: Map[TopicAndPartition, ReassignmentStatus], admin: AdminUtilities = AdminUtils): Unit = {
    var changed = false

    //If all partitions have completed remove the throttle
    if (reassignedPartitionsStatus.forall { case (_, status) => status == ReassignmentCompleted }) {
      //Remove the throttle limit from all brokers in the cluster
      //(as we no longer know which specific brokers were involved in the move)
      for (brokerId <- zkUtils.getAllBrokersInCluster().map(_.id)) {
        val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Broker, brokerId.toString)
        // bitwise OR as we don't want to short-circuit
        if (configs.remove(DynamicConfig.Broker.LeaderReplicationThrottledRateProp) != null
          | configs.remove(DynamicConfig.Broker.FollowerReplicationThrottledRateProp) != null){
          admin.changeBrokerConfig(zkUtils, Seq(brokerId), configs)
          changed = true
        }
      }

      //Remove the list of throttled replicas from all topics with partitions being moved
      val topics = partitionsToBeReassigned.keySet.map(tp => tp.topic).toSeq.distinct
      for (topic <- topics) {
        val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
        // bitwise OR as we don't want to short-circuit
        if (configs.remove(LogConfig.LeaderReplicationThrottledReplicasProp) != null
          | configs.remove(LogConfig.FollowerReplicationThrottledReplicasProp) != null){
          admin.changeTopicConfig(zkUtils, topic, configs)
          changed = true
        }
      }
      if (changed)
        println("Throttle was removed.")
    }
  }

  def generateAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    val topicsToMoveJsonFile = opts.options.valueOf(opts.topicsToMoveJsonFileOpt)
    val brokerListToReassign = opts.options.valueOf(opts.brokerListOpt).split(',').map(_.toInt)
    val duplicateReassignments = CoreUtils.duplicates(brokerListToReassign)
    if (duplicateReassignments.nonEmpty)
      throw new AdminCommandFailedException("Broker list contains duplicate entries: %s".format(duplicateReassignments.mkString(",")))
    val topicsToMoveJsonString = Utils.readFileAsString(topicsToMoveJsonFile)
    val disableRackAware = opts.options.has(opts.disableRackAware)
    val (proposedAssignments, currentAssignments) = generateAssignment(zkUtils, brokerListToReassign, topicsToMoveJsonString, disableRackAware)
    println("Current partition replica assignment\n%s\n".format(formatAsReassignmentJson(currentAssignments, Map.empty)))
    println("Proposed partition reassignment configuration\n%s".format(formatAsReassignmentJson(proposedAssignments, Map.empty)))
  }

  def generateAssignment(zkUtils: ZkUtils, brokerListToReassign: Seq[Int], topicsToMoveJsonString: String, disableRackAware: Boolean): (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]]) = {
    val topicsToReassign = ZkUtils.parseTopicsData(topicsToMoveJsonString)
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
        TopicAndPartition(topic, partition) -> replicas
      }
    }
    (partitionsToBeReassigned, currentAssignment)
  }

  def executeAssignment(zkUtils: ZkUtils, adminClientOpt: Option[JAdminClient], opts: ReassignPartitionsCommandOptions) {
    val reassignmentJsonFile =  opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val reassignmentJsonString = Utils.readFileAsString(reassignmentJsonFile)
    val throttle = opts.options.valueOf(opts.throttleOpt)
    val timeoutMs = opts.options.valueOf(opts.timeoutOpt)
    executeAssignment(zkUtils, adminClientOpt, reassignmentJsonString, Throttle(throttle), timeoutMs)
  }

  def executeAssignment(zkUtils: ZkUtils, adminClientOpt: Option[JAdminClient], reassignmentJsonString: String, throttle: Throttle, timeoutMs: Long = 10000L) {
    val (partitionAssignment, replicaAssignment) = parseAndValidate(zkUtils, reassignmentJsonString)
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, adminClientOpt, partitionAssignment.toMap, replicaAssignment)

    // If there is an existing rebalance running, attempt to change its throttle
    if (zkUtils.pathExists(ZkUtils.ReassignPartitionsPath)) {
      println("There is an existing assignment running.")
      reassignPartitionsCommand.maybeLimit(throttle)
    } else {
      printCurrentAssignment(zkUtils, partitionAssignment.map(_._1.topic))
      if (throttle.value >= 0)
        println(String.format("Warning: You must run Verify periodically, until the reassignment completes, to ensure the throttle is removed. You can also alter the throttle by rerunning the Execute command passing a new value."))
      if (reassignPartitionsCommand.reassignPartitions(throttle, timeoutMs)) {
        println("Successfully started reassignment of partitions.")
      } else
        println("Failed to reassign partitions %s".format(partitionAssignment))
    }
  }

  def printCurrentAssignment(zkUtils: ZkUtils, topics: Seq[String]): Unit = {
    // before starting assignment, output the current replica assignment to facilitate rollback
    val currentPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(topics)
    println("Current partition replica assignment\n\n%s\n\nSave this to use as the --reassignment-json-file option during rollback"
      .format(formatAsReassignmentJson(currentPartitionReplicaAssignment, Map.empty)))
  }

  def formatAsReassignmentJson(partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                               replicaLogDirAssignment: Map[TopicPartitionReplica, String]): String = {
    Json.encode(Map(
      "version" -> 1,
      "partitions" -> partitionsToBeReassigned.map { case (TopicAndPartition(topic, partition), replicas) =>
        Map(
          "topic" -> topic,
          "partition" -> partition,
          "replicas" -> replicas,
          "log_dirs" -> replicas.map(r => replicaLogDirAssignment.getOrElse(new TopicPartitionReplica(topic, partition, r), AnyLogDir))
        )
      }
    ))
  }

  // Parses without deduplicating keys so the data can be checked before allowing reassignment to proceed
  def parsePartitionReassignmentData(jsonData: String): (Seq[(TopicAndPartition, Seq[Int])], Map[TopicPartitionReplica, String]) = {
    val partitionAssignment = mutable.ListBuffer.empty[(TopicAndPartition, Seq[Int])]
    val replicaAssignment = mutable.Map.empty[TopicPartitionReplica, String]
    for {
      js <- Json.parseFull(jsonData).toSeq
      partitionsSeq <- js.asJsonObject.get("partitions").toSeq
      p <- partitionsSeq.asJsonArray.iterator
    } {
      val partitionFields = p.asJsonObject
      val topic = partitionFields("topic").to[String]
      val partition = partitionFields("partition").to[Int]
      val newReplicas = partitionFields("replicas").to[Seq[Int]]
      val newLogDirs = partitionFields.get("log_dirs") match {
        case Some(jsonValue) => jsonValue.to[Seq[String]]
        case None => newReplicas.map(r => AnyLogDir)
      }
      if (newReplicas.size != newLogDirs.size)
        throw new AdminCommandFailedException(s"Size of replicas list $newReplicas is different from " +
          s"size of log dirs list $newLogDirs for partition ${TopicAndPartition(topic, partition)}")
      partitionAssignment += (TopicAndPartition(topic, partition) -> newReplicas)
      replicaAssignment ++= newReplicas.zip(newLogDirs).map { case (replica, logDir) =>
        new TopicPartitionReplica(topic, partition, replica) -> logDir
      }.filter(_._2 != AnyLogDir)
    }
    (partitionAssignment, replicaAssignment)
  }

  def parseAndValidate(zkUtils: ZkUtils, reassignmentJsonString: String): (Seq[(TopicAndPartition, Seq[Int])], Map[TopicPartitionReplica, String]) = {
    val (partitionsToBeReassigned, replicaAssignment) = parsePartitionReassignmentData(reassignmentJsonString)

    if (partitionsToBeReassigned.isEmpty)
      throw new AdminCommandFailedException("Partition reassignment data file is empty")
    if (partitionsToBeReassigned.exists(_._2.isEmpty)) {
      throw new AdminCommandFailedException("Partition replica list cannot be empty")
    }
    val duplicateReassignedPartitions = CoreUtils.duplicates(partitionsToBeReassigned.map { case (tp, _) => tp })
    if (duplicateReassignedPartitions.nonEmpty)
      throw new AdminCommandFailedException("Partition reassignment contains duplicate topic partitions: %s".format(duplicateReassignedPartitions.mkString(",")))
    val duplicateEntries = partitionsToBeReassigned
      .map { case (tp, replicas) => (tp, CoreUtils.duplicates(replicas))}
      .filter { case (_, duplicatedReplicas) => duplicatedReplicas.nonEmpty }
    if (duplicateEntries.nonEmpty) {
      val duplicatesMsg = duplicateEntries
        .map { case (tp, duplicateReplicas) => "%s contains multiple entries for %s".format(tp, duplicateReplicas.mkString(",")) }
        .mkString(". ")
      throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicatesMsg))
    }
    // check that all partitions in the proposed assignment exist in the cluster
    val proposedTopics = partitionsToBeReassigned.map { case (tp, _) => tp.topic }.distinct
    val existingAssignment = zkUtils.getReplicaAssignmentForTopics(proposedTopics)
    val nonExistentPartitions = partitionsToBeReassigned.map { case (tp, _) => tp }.filterNot(existingAssignment.contains)
    if (nonExistentPartitions.nonEmpty)
      throw new AdminCommandFailedException("The proposed assignment contains non-existent partitions: " +
        nonExistentPartitions)

    // check that all brokers in the proposed assignment exist in the cluster
    val existingBrokerIDs = zkUtils.getSortedBrokerList()
    val nonExistingBrokerIDs = partitionsToBeReassigned.toMap.values.flatten.filterNot(existingBrokerIDs.contains).toSet
    if (nonExistingBrokerIDs.nonEmpty)
      throw new AdminCommandFailedException("The proposed assignment contains non-existent brokerIDs: " + nonExistingBrokerIDs.mkString(","))

    // check that replica will always be moved to another broker if a particular log directory is specified for it.
    // We will support moving replica within broker after KIP-113 is implemented
    replicaAssignment.foreach { case (replica, logDir) =>
      if (existingAssignment.getOrElse(TopicAndPartition(replica.topic(), replica.partition()), Seq.empty).contains(replica.brokerId()))
        throw new AdminCommandFailedException(s"The proposed assignment intends to move an existing replica $replica to " +
          s"another log directory $logDir on the same broker. This is not currently supported")
    }

    (partitionsToBeReassigned, replicaAssignment)
  }

  private def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
    partitionsToBeReassigned.keys.map { topicAndPartition =>
      (topicAndPartition, checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition, partitionsToBeReassigned,
        partitionsBeingReassigned))
    }.toMap
  }

  private def checkIfReplicaReassignmentSucceeded(adminClientOpt: Option[JAdminClient], replicaAssignment: Map[TopicPartitionReplica, String])
  :Map[TopicPartitionReplica, ReassignmentStatus] = {

    val replicaLogDirInfos = {
      if (replicaAssignment.nonEmpty) {
        val adminClient = adminClientOpt.getOrElse(
          throw new AdminCommandFailedException("bootstrap-server needs to be provided in order to reassign replica to the specified log directory"))
        adminClient.describeReplicaLogDirs(replicaAssignment.keySet.asJava).all().get().asScala
      } else {
        Map.empty[TopicPartitionReplica, ReplicaLogDirInfo]
      }
    }

    replicaAssignment.map { case (replica, newLogDir) =>
      val status: ReassignmentStatus = replicaLogDirInfos.get(replica) match {
        case Some(replicaLogDirInfo) =>
          if (replicaLogDirInfo.getCurrentReplicaLogDir == null) {
            println(s"Partition ${replica.topic()}-${replica.partition()} is not found in any live log dir on " +
              s"broker ${replica.brokerId()}. There is likely offline log directory on the broker.")
            ReassignmentFailed
          } else if (replicaLogDirInfo.getFutureReplicaLogDir == newLogDir) {
            ReassignmentInProgress
          } else if (replicaLogDirInfo.getFutureReplicaLogDir != null) {
            println(s"Partition ${replica.topic()}-${replica.partition()} on broker ${replica.brokerId()} " +
              s"is being moved to log dir ${replicaLogDirInfo.getFutureReplicaLogDir} instead of $newLogDir")
            ReassignmentFailed
          } else if (replicaLogDirInfo.getCurrentReplicaLogDir == newLogDir) {
            ReassignmentCompleted
          } else {
            println(s"Partition ${replica.topic()}-${replica.partition()} on broker ${replica.brokerId()} " +
              s"is not being moved from log dir ${replicaLogDirInfo.getCurrentReplicaLogDir} to $newLogDir")
            ReassignmentFailed
          }
        case None =>
          println(s"Partition ${replica.topic()}-${replica.partition()} is not found in any live log dir on broker ${replica.brokerId()}.")
          ReassignmentFailed
      }
      (replica, status)
    }
  }

  def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition,
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(_) => ReassignmentInProgress
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

  def validateAndParseArgs(args: Array[String]): ReassignPartitionsCommandOptions = {
    val opts = new ReassignPartitionsCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "This command moves topic partitions between replicas.")

    // Should have exactly one action
    val actions = Seq(opts.generateOpt, opts.executeOpt, opts.verifyOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --generate, --execute or --verify")

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)

    //Validate arguments for each action
    if(opts.options.has(opts.verifyOpt)) {
      if(!opts.options.has(opts.reassignmentJsonFileOpt))
        CommandLineUtils.printUsageAndDie(opts.parser, "If --verify option is used, command must include --reassignment-json-file that was used during the --execute option")
      CommandLineUtils.checkInvalidArgs(opts.parser, opts.options, opts.verifyOpt, Set(opts.throttleOpt, opts.topicsToMoveJsonFileOpt, opts.disableRackAware, opts.brokerListOpt))
    }
    else if(opts.options.has(opts.generateOpt)) {
      if(!(opts.options.has(opts.topicsToMoveJsonFileOpt) && opts.options.has(opts.brokerListOpt)))
        CommandLineUtils.printUsageAndDie(opts.parser, "If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options")
      CommandLineUtils.checkInvalidArgs(opts.parser, opts.options, opts.generateOpt, Set(opts.throttleOpt, opts.reassignmentJsonFileOpt))
    }
    else if (opts.options.has(opts.executeOpt)){
      if(!opts.options.has(opts.reassignmentJsonFileOpt))
        CommandLineUtils.printUsageAndDie(opts.parser, "If --execute option is used, command must include --reassignment-json-file that was output " + "during the --generate option")
      CommandLineUtils.checkInvalidArgs(opts.parser, opts.options, opts.executeOpt, Set(opts.topicsToMoveJsonFileOpt, opts.disableRackAware, opts.brokerListOpt))
    }
    opts
  }

  class ReassignPartitionsCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "the server(s) to use for bootstrapping. REQUIRED if " +
                      "an absolution path of the log directory is specified for any replica in the reassignment json file")
                      .withRequiredArg
                      .describedAs("Server(s) to use for bootstrapping")
                      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
                      "form host:port. Multiple URLS can be given to allow fail-over.")
                      .withRequiredArg
                      .describedAs("urls")
                      .ofType(classOf[String])
    val generateOpt = parser.accepts("generate", "Generate a candidate partition reassignment configuration." +
                      " Note that this only generates a candidate assignment, it does not execute it.")
    val executeOpt = parser.accepts("execute", "Kick off the reassignment as specified by the --reassignment-json-file option.")
    val verifyOpt = parser.accepts("verify", "Verify if the reassignment completed as specified by the --reassignment-json-file option. If there is a throttle engaged for the replicas specified, and the rebalance has completed, the throttle will be removed")
    val reassignmentJsonFileOpt = parser.accepts("reassignment-json-file", "The JSON file with the partition reassignment configuration" +
                      "The format to use is - \n" +
                      "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3],\n\t  \"log_dirs\": [\"dir1\",\"dir2\",\"dir3\"] }],\n\"version\":1\n}\n" +
                      "Note that \"log_dirs\" is optional. When it is specified, its length must equal the length of the replicas list. The value in this list " +
                      "can be either \"any\" or the absolution path of the log directory on the broker. If absolute log directory path is specified, it is currently required that " +
                      "the replica has not already been created on that broker. The replica will then be created in the specified log directory on the broker later.")
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
    val throttleOpt = parser.accepts("throttle", "The movement of partitions will be throttled to this value (bytes/sec). Rerunning with this option, whilst a rebalance is in progress, will alter the throttle value. The throttle rate should be at least 1 KB/s.")
                      .withRequiredArg()
                      .describedAs("throttle")
                      .ofType(classOf[Long])
                      .defaultsTo(-1)
    val timeoutOpt = parser.accepts("timeout", "The maximum time in ms allowed to wait for partition reassignment execution to be successfully initiated")
                      .withRequiredArg()
                      .describedAs("timeout")
                      .ofType(classOf[Long])
                      .defaultsTo(10000)
    val options = parser.parse(args : _*)
  }
}

class ReassignPartitionsCommand(zkUtils: ZkUtils,
                                adminClientOpt: Option[JAdminClient],
                                proposedPartitionAssignment: Map[TopicAndPartition, Seq[Int]],
                                proposedReplicaAssignment: Map[TopicPartitionReplica, String] = Map.empty,
                                admin: AdminUtilities = AdminUtils)
  extends Logging {

  import ReassignPartitionsCommand._

  def existingAssignment(): Map[TopicAndPartition, Seq[Int]] = {
    val proposedTopics = proposedPartitionAssignment.keySet.map(_.topic).toSeq
    zkUtils.getReplicaAssignmentForTopics(proposedTopics)
  }

  private def maybeThrottle(throttle: Throttle): Unit = {
    if (throttle.value >= 0) {
      assignThrottledReplicas(existingAssignment(), proposedPartitionAssignment)
      maybeLimit(throttle)
      throttle.postUpdateAction()
      println(s"The throttle limit was set to ${throttle.value} B/s")
    }
  }

  /**
    * Limit the throttle on currently moving replicas. Note that this command can use used to alter the throttle, but
    * it may not alter all limits originally set, if some of the brokers have completed their rebalance.
    */
  def maybeLimit(throttle: Throttle) {
    if (throttle.value >= 0) {
      val existingBrokers = existingAssignment().values.flatten.toSeq
      val proposedBrokers = proposedPartitionAssignment.values.flatten.toSeq
      val brokers = (existingBrokers ++ proposedBrokers).distinct

      for (id <- brokers) {
        val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Broker, id.toString)
        configs.put(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, throttle.value.toString)
        configs.put(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, throttle.value.toString)
        admin.changeBrokerConfig(zkUtils, Seq(id), configs)
      }
    }
  }

  /** Set throttles to replicas that are moving. Note: this method should only be used when the assignment is initiated. */
  private[admin] def assignThrottledReplicas(allExisting: Map[TopicAndPartition, Seq[Int]], allProposed: Map[TopicAndPartition, Seq[Int]], admin: AdminUtilities = AdminUtils): Unit = {
    for (topic <- allProposed.keySet.map(_.topic).toSeq) {
      val (existing, proposed) = filterBy(topic, allExisting, allProposed)

      //Apply the leader throttle to all replicas that exist before the re-balance.
      val leader = format(preRebalanceReplicaForMovingPartitions(existing, proposed))

      //Apply a follower throttle to all "move destinations".
      val follower = format(postRebalanceReplicasThatMoved(existing, proposed))

      val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
      configs.put(LeaderReplicationThrottledReplicasProp, leader)
      configs.put(FollowerReplicationThrottledReplicasProp, follower)
      admin.changeTopicConfig(zkUtils, topic, configs)

      debug(s"Updated leader-throttled replicas for topic $topic with: $leader")
      debug(s"Updated follower-throttled replicas for topic $topic with: $follower")
    }
  }

  private def postRebalanceReplicasThatMoved(existing: Map[TopicAndPartition, Seq[Int]], proposed: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    //For each partition in the proposed list, filter out any replicas that exist now, and hence aren't being moved.
    proposed.map { case (tp, proposedReplicas) =>
      tp -> (proposedReplicas.toSet -- existing(tp)).toSeq
    }
  }

  private def preRebalanceReplicaForMovingPartitions(existing: Map[TopicAndPartition, Seq[Int]], proposed: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    def moving(before: Seq[Int], after: Seq[Int]) = (after.toSet -- before.toSet).nonEmpty
    //For any moving partition, throttle all the original (pre move) replicas (as any one might be a leader)
    existing.filter { case (tp, preMoveReplicas) =>
      proposed.contains(tp) && moving(preMoveReplicas, proposed(tp))
    }
  }

  def format(moves: Map[TopicAndPartition, Seq[Int]]): String =
    moves.flatMap { case (tp, moves) =>
      moves.map(replicaId => s"${tp.partition}:${replicaId}")
    }.mkString(",")

  def filterBy(topic: String, allExisting: Map[TopicAndPartition, Seq[Int]], allProposed: Map[TopicAndPartition, Seq[Int]]): (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]]) = {
    (allExisting.filter { case (tp, _) => tp.topic == topic },
      allProposed.filter { case (tp, _) => tp.topic == topic })
  }

  def reassignPartitions(throttle: Throttle = NoThrottle, timeoutMs: Long = 10000L): Boolean = {
    maybeThrottle(throttle)
    try {
      val validPartitions = proposedPartitionAssignment.filter { case (p, _) => validatePartition(zkUtils, p.topic, p.partition) }
      if (validPartitions.isEmpty) false
      else {
        if (proposedReplicaAssignment.nonEmpty) {
          // Send AlterReplicaLogDirsRequest to allow broker to create replica in the right log dir later if the replica
          // has not been created it. This allows us to rebalance load across log directories in the cluster even if
          // we can not move replicas between log directories on the same broker. We will be able to move replicas
          // between log directories on the same broker after KIP-113 is implemented.
          val adminClient = adminClientOpt.getOrElse(
            throw new AdminCommandFailedException("bootstrap-server needs to be provided in order to reassign replica to the specified log directory"))
          val alterReplicaDirResult = adminClient.alterReplicaLogDirs(
            proposedReplicaAssignment.asJava, new AlterReplicaLogDirsOptions().timeoutMs(timeoutMs.toInt))
          alterReplicaDirResult.values().asScala.foreach { case (replica, future) => {
              try {
                /*
                 * Before KIP-113 is fully implemented, user can only specify the destination log directory of the replica
                 * if the replica has not already been created on the broker; otherwise the log directory specified in the
                 * json file will not be enforced. Therefore we want to verify that broker will return ReplicaNotAvailableException
                 * for this replica.
                 *
                 * After KIP-113 is fully implemented, we will not need to verify that the broker returns this ReplicaNotAvailableException
                 * in this step. And after the reassignment znode is created, we will need to re-send AlterReplicaLogDirsRequest to broker
                 * if broker returns ReplicaNotAvailableException for any replica in the request.
                 */
                future.get()
                throw new AdminCommandFailedException(s"Partition ${replica.topic()}-${replica.partition()} already exists on broker ${replica.brokerId()}." +
                  s" Reassign replica to another log directory on the same broker is currently not supported.")
              } catch {
                case t: ExecutionException =>
                  t.getCause match {
                    case e: ReplicaNotAvailableException => // It is OK if the replica is not available
                    case e: Throwable => throw e
                  }
              }
          }}
        }
        val jsonReassignmentData = ZkUtils.formatAsReassignmentJson(validPartitions)
        zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
        true
      }
    } catch {
      case _: ZkNodeExistsException =>
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
        throw new AdminCommandFailedException("Partition reassignment currently in " +
          "progress for %s. Aborting operation".format(partitionsBeingReassigned))
      case e: LogDirNotFoundException =>
        throw new AdminCommandFailedException(s"The proposed replica assignment $proposedReplicaAssignment contains " +
          s"invalid log directory. Aborting operation", e)
      case e: AdminCommandFailedException => throw e
      case e: Throwable =>
        error("Admin command failed", e)
        false
    }
  }

  def validatePartition(zkUtils: ZkUtils, topic: String, partition: Int): Boolean = {
    // check if partition exists
    val partitionsOpt = zkUtils.getPartitionsForTopics(List(topic)).get(topic)
    partitionsOpt match {
      case Some(partitions) =>
        if(partitions.contains(partition)) {
          true
        } else {
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

