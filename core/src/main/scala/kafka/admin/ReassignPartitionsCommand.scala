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

import java.util
import java.util.Optional
import java.util.concurrent.ExecutionException

import kafka.common.AdminCommandFailedException
import kafka.log.LogConfig
import kafka.server.{ConfigType, DynamicConfig}
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, CoreUtils, Exit, Json, Logging}
import kafka.utils.Implicits._
import kafka.utils.json.JsonValue
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp, ConfigEntry, NewPartitionReassignment, PartitionReassignment, TopicDescription}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{ReplicaNotAvailableException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, KafkaFuture, TopicPartition, TopicPartitionReplica}

import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, mutable}
import scala.math.Ordered.orderingToOrdered


object ReassignPartitionsCommand extends Logging {
  private[admin] val AnyLogDir = "any"

  val helpText = "This tool helps to move topic partitions between replicas."

  /**
   * The earliest version of the partition reassignment JSON.  We will default to this
   * version if no other version number is given.
   */
  private[admin] val EarliestVersion = 1

  /**
   * The earliest version of the JSON for each partition reassignment topic.  We will
   * default to this version if no other version number is given.
   */
  private[admin] val EarliestTopicsJsonVersion = 1

  // Throttles that are set at the level of an individual broker.
  private[admin] val brokerLevelLeaderThrottle =
    DynamicConfig.Broker.LeaderReplicationThrottledRateProp
  private[admin] val brokerLevelFollowerThrottle =
    DynamicConfig.Broker.FollowerReplicationThrottledRateProp
  private[admin] val brokerLevelLogDirThrottle =
    DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp
  private[admin] val brokerLevelThrottles = Seq(
    brokerLevelLeaderThrottle,
    brokerLevelFollowerThrottle,
    brokerLevelLogDirThrottle
  )

  // Throttles that are set at the level of an individual topic.
  private[admin] val topicLevelLeaderThrottle =
    LogConfig.LeaderReplicationThrottledReplicasProp
  private[admin] val topicLevelFollowerThrottle =
    LogConfig.FollowerReplicationThrottledReplicasProp
  private[admin] val topicLevelThrottles = Seq(
    topicLevelLeaderThrottle,
    topicLevelFollowerThrottle
  )

  private[admin] val cannotExecuteBecauseOfExistingMessage = "Cannot execute because " +
    "there is an existing partition assignment.  Use --additional to override this and " +
    "create a new partition assignment in addition to the existing one. The --additional " +
    "flag can also be used to change the throttle by resubmitting the current reassignment."

  private[admin] val youMustRunVerifyPeriodicallyMessage = "Warning: You must run " +
    "--verify periodically, until the reassignment completes, to ensure the throttle " +
    "is removed."

  /**
   * A map from topic names to partition movements.
   */
  type MoveMap = mutable.Map[String, mutable.Map[Int, PartitionMove]]

  /**
   * A partition movement.  The source and destination brokers may overlap.
   *
   * @param sources         The source brokers.
   * @param destinations    The destination brokers.
   */
  sealed case class PartitionMove(sources: mutable.Set[Int],
                                  destinations: mutable.Set[Int]) { }

  /**
   * The state of a partition reassignment.  The current replicas and target replicas
   * may overlap.
   *
   * @param currentReplicas The current replicas.
   * @param targetReplicas  The target replicas.
   * @param done            True if the reassignment is done.
   */
  sealed case class PartitionReassignmentState(currentReplicas: Seq[Int],
                                               targetReplicas: Seq[Int],
                                               done: Boolean) {}

  /**
   * The state of a replica log directory movement.
   */
  sealed trait LogDirMoveState {
    /**
     * True if the move is done without errors.
     */
    def done: Boolean
  }

  /**
   * A replica log directory move state where the source log directory is missing.
   *
   * @param targetLogDir        The log directory that we wanted the replica to move to.
   */
  sealed case class MissingReplicaMoveState(targetLogDir: String)
      extends LogDirMoveState {
    override def done = false
  }

  /**
   * A replica log directory move state where the source replica is missing.
   *
   * @param targetLogDir        The log directory that we wanted the replica to move to.
   */
  sealed case class MissingLogDirMoveState(targetLogDir: String)
      extends LogDirMoveState {
    override def done = false
  }

  /**
   * A replica log directory move state where the move is in progress.
   *
   * @param currentLogDir       The current log directory.
   * @param futureLogDir        The log directory that the replica is moving to.
   * @param targetLogDir        The log directory that we wanted the replica to move to.
   */
  sealed case class ActiveMoveState(currentLogDir: String,
                                    targetLogDir: String,
                                    futureLogDir: String)
      extends LogDirMoveState {
    override def done = false
  }

  /**
   * A replica log directory move state where there is no move in progress, but we did not
   * reach the target log directory.
   *
   * @param currentLogDir       The current log directory.
   * @param targetLogDir        The log directory that we wanted the replica to move to.
   */
  sealed case class CancelledMoveState(currentLogDir: String,
                                       targetLogDir: String)
      extends LogDirMoveState {
    override def done = true
  }

  /**
   * The completed replica log directory move state.
   *
   * @param targetLogDir        The log directory that we wanted the replica to move to.
   */
  sealed case class CompletedMoveState(targetLogDir: String)
      extends LogDirMoveState {
    override def done = true
  }

  /**
   * An exception thrown to indicate that the command has failed, but we don't want to
   * print a stack trace.
   *
   * @param message     The message to print out before exiting.  A stack trace will not
   *                    be printed.
   */
  class TerseReassignmentFailureException(message: String) extends KafkaException(message) {
  }

  def main(args: Array[String]): Unit = {
    val opts = validateAndParseArgs(args)
    var toClose: Option[AutoCloseable] = None
    var failed = true

    try {
      if (opts.options.has(opts.bootstrapServerOpt)) {
        if (opts.options.has(opts.zkConnectOpt)) {
          println("Warning: ignoring deprecated --zookeeper option because " +
            "--bootstrap-server was specified.  The --zookeeper option will " +
            "be removed in a future version of Kafka.")
        }
        val props = if (opts.options.has(opts.commandConfigOpt))
          Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
        else
          new util.Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "reassign-partitions-tool")
        val adminClient = Admin.create(props)
        toClose = Some(adminClient)
        handleAction(adminClient, opts)
      } else {
        println("Warning: --zookeeper is deprecated, and will be removed in a future " +
          "version of Kafka.")
        val zkClient = KafkaZkClient(opts.options.valueOf(opts.zkConnectOpt),
          JaasUtils.isZkSaslEnabled, 30000, 30000, Int.MaxValue, Time.SYSTEM)
        toClose = Some(zkClient)
        handleAction(zkClient, opts)
      }
      failed = false
    } catch {
      case e: TerseReassignmentFailureException =>
        println(e.getMessage)
      case e: Throwable =>
        println("Error: " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      // Close the AdminClient or ZooKeeper client, as appropriate.
      // It's good to do this after printing any error stack trace.
      toClose.foreach(_.close())
    }
    // If the command failed, exit with a non-zero exit code.
    if (failed) {
      Exit.exit(1)
    }
  }

  private def handleAction(adminClient: Admin,
                           opts: ReassignPartitionsCommandOptions): Unit = {
    if (opts.options.has(opts.verifyOpt)) {
      verifyAssignment(adminClient,
        Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
        opts.options.has(opts.preserveThrottlesOpt))
    } else if (opts.options.has(opts.generateOpt)) {
      generateAssignment(adminClient,
        Utils.readFileAsString(opts.options.valueOf(opts.topicsToMoveJsonFileOpt)),
        opts.options.valueOf(opts.brokerListOpt),
        !opts.options.has(opts.disableRackAware))
    } else if (opts.options.has(opts.executeOpt)) {
      executeAssignment(adminClient,
        opts.options.has(opts.additionalOpt),
        Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
        opts.options.valueOf(opts.interBrokerThrottleOpt),
        opts.options.valueOf(opts.replicaAlterLogDirsThrottleOpt),
        opts.options.valueOf(opts.timeoutOpt))
    } else if (opts.options.has(opts.cancelOpt)) {
      cancelAssignment(adminClient,
        Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
        opts.options.has(opts.preserveThrottlesOpt),
        opts.options.valueOf(opts.timeoutOpt))
    } else if (opts.options.has(opts.listOpt)) {
      listReassignments(adminClient)
    } else {
      throw new RuntimeException("Unsupported action.")
    }
  }

  private def handleAction(zkClient: KafkaZkClient,
                           opts: ReassignPartitionsCommandOptions): Unit = {
    if (opts.options.has(opts.verifyOpt)) {
      verifyAssignment(zkClient,
        Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
        opts.options.has(opts.preserveThrottlesOpt))
    } else if (opts.options.has(opts.generateOpt)) {
      generateAssignment(zkClient,
        Utils.readFileAsString(opts.options.valueOf(opts.topicsToMoveJsonFileOpt)),
        opts.options.valueOf(opts.brokerListOpt),
        !opts.options.has(opts.disableRackAware))
    } else if (opts.options.has(opts.executeOpt)) {
      executeAssignment(zkClient,
        Utils.readFileAsString(opts.options.valueOf(opts.reassignmentJsonFileOpt)),
        opts.options.valueOf(opts.interBrokerThrottleOpt))
    } else {
      throw new RuntimeException("Unsupported action.")
    }
  }

  /**
   * A result returned from verifyAssignment.
   *
   * @param partStates    A map from partitions to reassignment states.
   * @param partsOngoing  True if there are any ongoing partition reassignments.
   * @param moveStates    A map from log directories to movement states.
   * @param movesOngoing  True if there are any ongoing moves that we know about.
   */
  case class VerifyAssignmentResult(partStates: Map[TopicPartition, PartitionReassignmentState],
                                    partsOngoing: Boolean = false,
                                    moveStates: Map[TopicPartitionReplica, LogDirMoveState] = Map.empty,
                                    movesOngoing: Boolean = false)

  /**
   * The entry point for the --verify command.
   *
   * @param adminClient           The AdminClient to use.
   * @param jsonString            The JSON string to use for the topics and partitions to verify.
   * @param preserveThrottles     True if we should avoid changing topic or broker throttles.
   *
   * @return                      A result that is useful for testing.
   */
  def verifyAssignment(adminClient: Admin, jsonString: String, preserveThrottles: Boolean)
                      : VerifyAssignmentResult = {
    val (targetParts, targetLogDirs) = parsePartitionReassignmentData(jsonString)
    val (partStates, partsOngoing) = verifyPartitionAssignments(adminClient, targetParts)
    val (moveStates, movesOngoing) = verifyReplicaMoves(adminClient, targetLogDirs)
    if (!partsOngoing && !movesOngoing && !preserveThrottles) {
      // If the partition assignments and replica assignments are done, clear any throttles
      // that were set.  We have to clear all throttles, because we don't have enough
      // information to know all of the source brokers that might have been involved in the
      // previous reassignments.
      clearAllThrottles(adminClient, targetParts)
    }
    VerifyAssignmentResult(partStates, partsOngoing, moveStates, movesOngoing)
  }

  /**
   * Verify the partition reassignments specified by the user.
   *
   * @param adminClient           The AdminClient to use.
   * @param targets               The partition reassignments specified by the user.
   *
   * @return                      A tuple of the partition reassignment states, and a
   *                              boolean which is true if there are no ongoing
   *                              reassignments (including reassignments not described
   *                              in the JSON file.)
   */
  def verifyPartitionAssignments(adminClient: Admin,
                                 targets: Seq[(TopicPartition, Seq[Int])])
                                 : (Map[TopicPartition, PartitionReassignmentState], Boolean) = {
    val (partStates, partsOngoing) = findPartitionReassignmentStates(adminClient, targets)
    println(partitionReassignmentStatesToString(partStates))
    (partStates, partsOngoing)
  }

  /**
   * The deprecated entry point for the --verify command.
   *
   * @param zkClient              The ZooKeeper client to use.
   * @param jsonString            The JSON string to use for the topics and partitions to verify.
   * @param preserveThrottles     True if we should avoid changing topic or broker throttles.
   *
   * @return                      A result that is useful for testing.  Note that anything that
   *                              would require AdminClient to see will be left out of this result.
   */
  def verifyAssignment(zkClient: KafkaZkClient, jsonString: String, preserveThrottles: Boolean)
                       : VerifyAssignmentResult = {
    val (targetParts, targetLogDirs) = parsePartitionReassignmentData(jsonString)
    if (targetLogDirs.nonEmpty) {
      throw new AdminCommandFailedException("bootstrap-server needs to be provided when " +
        "replica reassignments are present.")
    }
    println("Warning: because you are using the deprecated --zookeeper option, the results " +
      "may be incomplete.  Use --bootstrap-server instead for more accurate results.")
    val (partStates, partsOngoing) = verifyPartitionAssignments(zkClient, targetParts.toMap)
    if (!partsOngoing && !preserveThrottles) {
      clearAllThrottles(zkClient, targetParts)
    }
    VerifyAssignmentResult(partStates, partsOngoing, Map.empty, false)
  }

  /**
   * Verify the partition reassignments specified by the user.
   *
   * @param zkClient              The ZooKeeper client to use.
   * @param targets               The partition reassignments specified by the user.
   *
   * @return                      A tuple of partition states and whether there are any
   *                              ongoing reassignments found in the legacy reassign
   *                              partitions ZNode.
   */
  def verifyPartitionAssignments(zkClient: KafkaZkClient,
                                 targets: Map[TopicPartition, Seq[Int]])
                                 : (Map[TopicPartition, PartitionReassignmentState], Boolean) = {
    val (partStates, partsOngoing) = findPartitionReassignmentStates(zkClient, targets)
    println(partitionReassignmentStatesToString(partStates))
    (partStates, partsOngoing)
  }

  def compareTopicPartitions(a: TopicPartition, b: TopicPartition): Boolean = {
    (a.topic(), a.partition()) < (b.topic(), b.partition())
  }

  def compareTopicPartitionReplicas(a: TopicPartitionReplica, b: TopicPartitionReplica): Boolean = {
    (a.brokerId(), a.topic(), a.partition()) < (b.brokerId(), b.topic(), b.partition())
  }

  /**
   * Convert partition reassignment states to a human-readable string.
   *
   * @param states      A map from topic partitions to states.
   * @return            A string summarizing the partition reassignment states.
   */
  def partitionReassignmentStatesToString(states: Map[TopicPartition, PartitionReassignmentState])
                                          : String = {
    val bld = new mutable.ArrayBuffer[String]()
    bld.append("Status of partition reassignment:")
    states.keySet.toBuffer.sortWith(compareTopicPartitions).foreach { topicPartition =>
      val state = states(topicPartition)
      if (state.done) {
        if (state.currentReplicas.equals(state.targetReplicas)) {
          bld.append("Reassignment of partition %s is complete.".
            format(topicPartition.toString))
        } else {
          bld.append(s"There is no active reassignment of partition ${topicPartition}, " +
            s"but replica set is ${state.currentReplicas.mkString(",")} rather than " +
            s"${state.targetReplicas.mkString(",")}.")
        }
      } else {
        bld.append("Reassignment of partition %s is still in progress.".format(topicPartition))
      }
    }
    bld.mkString(System.lineSeparator())
  }

  /**
   * Find the state of the specified partition reassignments.
   *
   * @param adminClient          The Admin client to use.
   * @param targetReassignments  The reassignments we want to learn about.
   *
   * @return                     A tuple containing the reassignment states for each topic
   *                             partition, plus whether there are any ongoing reassignments.
   */
  def findPartitionReassignmentStates(adminClient: Admin,
                                      targetReassignments: Seq[(TopicPartition, Seq[Int])])
                                      : (Map[TopicPartition, PartitionReassignmentState], Boolean) = {
    val currentReassignments = adminClient.
      listPartitionReassignments.reassignments.get().asScala
    val (foundReassignments, notFoundReassignments) = targetReassignments.partition {
      case (part, _) => currentReassignments.contains(part)
    }
    val foundResults = foundReassignments.map {
      case (part, targetReplicas) => (part,
        PartitionReassignmentState(
          currentReassignments(part).replicas.
            asScala.map(i => i.asInstanceOf[Int]),
          targetReplicas,
          false))
    }
    val topicNamesToLookUp = new mutable.HashSet[String]()
    notFoundReassignments.foreach { case (part, _) =>
      if (!currentReassignments.contains(part))
        topicNamesToLookUp.add(part.topic)
    }
    val topicDescriptions = adminClient.
      describeTopics(topicNamesToLookUp.asJava).values().asScala
    val notFoundResults = notFoundReassignments.map {
      case (part, targetReplicas) =>
        currentReassignments.get(part) match {
          case Some(reassignment) => (part,
            PartitionReassignmentState(
              reassignment.replicas.asScala.map(_.asInstanceOf[Int]),
              targetReplicas,
              false))
          case None =>
            (part, topicDescriptionFutureToState(part.partition,
              topicDescriptions(part.topic), targetReplicas))
        }
    }
    val allResults = foundResults ++ notFoundResults
    (allResults.toMap, currentReassignments.nonEmpty)
  }

  private def topicDescriptionFutureToState(partition: Int,
                                            future: KafkaFuture[TopicDescription],
                                            targetReplicas: Seq[Int]): PartitionReassignmentState = {
    try {
      val topicDescription = future.get()
      if (topicDescription.partitions().size() < partition) {
        throw new ExecutionException("Too few partitions found", new UnknownTopicOrPartitionException())
      }
      PartitionReassignmentState(
        topicDescription.partitions.get(partition).replicas.asScala.map(_.id),
        targetReplicas,
        true)
    } catch {
      case t: ExecutionException if t.getCause.isInstanceOf[UnknownTopicOrPartitionException] =>
        PartitionReassignmentState(Seq(), targetReplicas, true)
    }
  }

  /**
   * Find the state of the specified partition reassignments.
   *
   * @param zkClient              The ZooKeeper client to use.
   * @param targetReassignments   The reassignments we want to learn about.
   *
   * @return                      A tuple containing the reassignment states for each topic
   *                              partition, plus whether there are any ongoing reassignments
   *                              found in the legacy reassign partitions znode.
   */
  def findPartitionReassignmentStates(zkClient: KafkaZkClient,
                                      targetReassignments: Map[TopicPartition, Seq[Int]])
                                      : (Map[TopicPartition, PartitionReassignmentState], Boolean) = {
    val partitionsBeingReassigned = zkClient.getPartitionReassignment
    val results = new mutable.HashMap[TopicPartition, PartitionReassignmentState]()
    targetReassignments.groupBy(_._1.topic).forKeyValue { (topic, partitions) =>
      val replicasForTopic = zkClient.getReplicaAssignmentForTopics(Set(topic))
      partitions.forKeyValue { (partition, targetReplicas) =>
        val currentReplicas = replicasForTopic.getOrElse(partition, Seq())
        results.put(partition, new PartitionReassignmentState(
          currentReplicas, targetReplicas, !partitionsBeingReassigned.contains(partition)))
      }
    }
    (results, partitionsBeingReassigned.nonEmpty)
  }

  /**
   * Verify the replica reassignments specified by the user.
   *
   * @param adminClient           The AdminClient to use.
   * @param targetReassignments   The replica reassignments specified by the user.
   *
   * @return                      A tuple of the replica states, and a boolean which is true
   *                              if there are any ongoing replica moves.
   *
   *                              Note: Unlike in verifyPartitionAssignments, we will
   *                              return false here even if there are unrelated ongoing
   *                              reassignments. (We don't have an efficient API that
   *                              returns all ongoing replica reassignments.)
   */
  def verifyReplicaMoves(adminClient: Admin,
                         targetReassignments: Map[TopicPartitionReplica, String])
                         : (Map[TopicPartitionReplica, LogDirMoveState], Boolean) = {
    val moveStates = findLogDirMoveStates(adminClient, targetReassignments)
    println(replicaMoveStatesToString(moveStates))
    (moveStates, !moveStates.values.forall(_.done))
  }

  /**
   * Find the state of the specified partition reassignments.
   *
   * @param adminClient           The AdminClient to use.
   * @param targetMoves           The movements we want to learn about.  The map is keyed
   *                              by TopicPartitionReplica, and its values are target log
   *                              directories.
   *
   * @return                      The states for each replica movement.
   */
  def findLogDirMoveStates(adminClient: Admin,
                           targetMoves: Map[TopicPartitionReplica, String])
                           : Map[TopicPartitionReplica, LogDirMoveState] = {
    val replicaLogDirInfos = adminClient.describeReplicaLogDirs(
      targetMoves.keySet.asJava).all().get().asScala
    targetMoves.map { case (replica, targetLogDir) =>
      val moveState = replicaLogDirInfos.get(replica) match {
        case None => MissingReplicaMoveState(targetLogDir)
        case Some(info) => if (info.getCurrentReplicaLogDir == null) {
            MissingLogDirMoveState(targetLogDir)
          } else if (info.getFutureReplicaLogDir == null) {
            if (info.getCurrentReplicaLogDir.equals(targetLogDir)) {
              CompletedMoveState(targetLogDir)
            } else {
              CancelledMoveState(info.getCurrentReplicaLogDir, targetLogDir)
            }
          } else {
            ActiveMoveState(info.getCurrentReplicaLogDir(),
              targetLogDir,
              info.getFutureReplicaLogDir)
          }
      }
      (replica, moveState)
    }
  }

  /**
   * Convert replica move states to a human-readable string.
   *
   * @param states          A map from topic partition replicas to states.
   * @return                A tuple of a summary string, and a boolean describing
   *                        whether there are any active replica moves.
   */
  def replicaMoveStatesToString(states: Map[TopicPartitionReplica, LogDirMoveState])
                                : String = {
    val bld = new mutable.ArrayBuffer[String]
    states.keySet.toBuffer.sortWith(compareTopicPartitionReplicas).foreach { replica =>
      val state = states(replica)
      state match {
        case MissingLogDirMoveState(_) =>
          bld.append(s"Partition ${replica.topic}-${replica.partition} is not found " +
            s"in any live log dir on broker ${replica.brokerId}. There is likely an " +
            s"offline log directory on the broker.")
        case MissingReplicaMoveState(_) =>
          bld.append(s"Partition ${replica.topic}-${replica.partition} cannot be found " +
            s"in any live log directory on broker ${replica.brokerId}.")
        case ActiveMoveState(_, targetLogDir, futureLogDir) =>
          if (targetLogDir.equals(futureLogDir)) {
            bld.append(s"Reassignment of replica $replica is still in progress.")
          } else {
            bld.append(s"Partition ${replica.topic}-${replica.partition} on broker " +
              s"${replica.brokerId} is being moved to log dir $futureLogDir " +
              s"instead of $targetLogDir.")
          }
        case CancelledMoveState(currentLogDir, targetLogDir) =>
          bld.append(s"Partition ${replica.topic}-${replica.partition} on broker " +
            s"${replica.brokerId} is not being moved from log dir $currentLogDir to " +
            s"$targetLogDir.")
        case CompletedMoveState(_) =>
          bld.append(s"Reassignment of replica $replica completed successfully.")
      }
    }
    bld.mkString(System.lineSeparator())
  }

  /**
   * Clear all topic-level and broker-level throttles.
   *
   * @param adminClient     The AdminClient to use.
   * @param targetParts     The target partitions loaded from the JSON file.
   */
  def clearAllThrottles(adminClient: Admin,
                        targetParts: Seq[(TopicPartition, Seq[Int])]): Unit = {
    val activeBrokers = adminClient.describeCluster().nodes().get().asScala.map(_.id()).toSet
    val brokers = activeBrokers ++ targetParts.flatMap(_._2).toSet
    println("Clearing broker-level throttles on broker%s %s".format(
      if (brokers.size == 1) "" else "s", brokers.mkString(",")))
    clearBrokerLevelThrottles(adminClient, brokers)

    val topics = targetParts.map(_._1.topic()).toSet
    println("Clearing topic-level throttles on topic%s %s".format(
      if (topics.size == 1) "" else "s", topics.mkString(",")))
    clearTopicLevelThrottles(adminClient, topics)
  }

  /**
   * Clear all topic-level and broker-level throttles.
   *
   * @param zkClient        The ZooKeeper client to use.
   * @param targetParts     The target partitions loaded from the JSON file.
   */
  def clearAllThrottles(zkClient: KafkaZkClient,
                        targetParts: Seq[(TopicPartition, Seq[Int])]): Unit = {
    val activeBrokers = zkClient.getAllBrokersInCluster.map(_.id).toSet
    val brokers = activeBrokers ++ targetParts.flatMap(_._2).toSet
    println("Clearing broker-level throttles on broker%s %s".format(
      if (brokers.size == 1) "" else "s", brokers.mkString(",")))
    clearBrokerLevelThrottles(zkClient, brokers)

    val topics = targetParts.map(_._1.topic()).toSet
    println("Clearing topic-level throttles on topic%s %s".format(
      if (topics.size == 1) "" else "s", topics.mkString(",")))
    clearTopicLevelThrottles(zkClient, topics)
  }

  /**
   * Clear all throttles which have been set at the broker level.
   *
   * @param adminClient       The AdminClient to use.
   * @param brokers           The brokers to clear the throttles for.
   */
  def clearBrokerLevelThrottles(adminClient: Admin, brokers: Set[Int]): Unit = {
    val configOps = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    brokers.foreach { brokerId =>
      configOps.put(
        new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString),
        brokerLevelThrottles.map(throttle => new AlterConfigOp(
          new ConfigEntry(throttle, null), OpType.DELETE)).asJava)
    }
    adminClient.incrementalAlterConfigs(configOps).all().get()
  }

  /**
   * Clear all throttles which have been set at the broker level.
   *
   * @param zkClient          The ZooKeeper client to use.
   * @param brokers           The brokers to clear the throttles for.
   */
  def clearBrokerLevelThrottles(zkClient: KafkaZkClient, brokers: Set[Int]): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)
    for (brokerId <- brokers) {
      val configs = adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.toString)
      if (brokerLevelThrottles.flatMap(throttle => Option(configs.remove(throttle))).nonEmpty) {
        adminZkClient.changeBrokerConfig(Seq(brokerId), configs)
      }
    }
  }

  /**
   * Clear the reassignment throttles for the specified topics.
   *
   * @param adminClient           The AdminClient to use.
   * @param topics                The topics to clear the throttles for.
   */
  def clearTopicLevelThrottles(adminClient: Admin, topics: Set[String]): Unit = {
    val configOps = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    topics.foreach {
      topicName => configOps.put(
        new ConfigResource(ConfigResource.Type.TOPIC, topicName),
        topicLevelThrottles.map(throttle => new AlterConfigOp(new ConfigEntry(throttle, null),
          OpType.DELETE)).asJava)
    }
    adminClient.incrementalAlterConfigs(configOps).all().get()
  }

  /**
   * Clear the reassignment throttles for the specified topics.
   *
   * @param zkClient              The ZooKeeper client to use.
   * @param topics                The topics to clear the throttles for.
   */
  def clearTopicLevelThrottles(zkClient: KafkaZkClient, topics: Set[String]): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)
    for (topic <- topics) {
      val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
      if (topicLevelThrottles.flatMap(throttle => Option(configs.remove(throttle))).nonEmpty) {
        adminZkClient.changeTopicConfig(topic, configs)
      }
    }
  }

  /**
   * The entry point for the --generate command.
   *
   * @param adminClient           The AdminClient to use.
   * @param reassignmentJson      The JSON string to use for the topics to reassign.
   * @param brokerListString      The comma-separated string of broker IDs to use.
   * @param enableRackAwareness   True if rack-awareness should be enabled.
   *
   * @return                      A tuple containing the proposed assignment and the
   *                              current assignment.
   */
  def generateAssignment(adminClient: Admin,
                         reassignmentJson: String,
                         brokerListString: String,
                         enableRackAwareness: Boolean)
                         : (Map[TopicPartition, Seq[Int]], Map[TopicPartition, Seq[Int]]) = {
    val (brokersToReassign, topicsToReassign) =
      parseGenerateAssignmentArgs(reassignmentJson, brokerListString)
    val currentAssignments = getReplicaAssignmentForTopics(adminClient, topicsToReassign)
    val brokerMetadatas = getBrokerMetadata(adminClient, brokersToReassign, enableRackAwareness)
    val proposedAssignments = calculateAssignment(currentAssignments, brokerMetadatas)
    println("Current partition replica assignment\n%s\n".
      format(formatAsReassignmentJson(currentAssignments, Map.empty)))
    println("Proposed partition reassignment configuration\n%s".
      format(formatAsReassignmentJson(proposedAssignments, Map.empty)))
    (proposedAssignments, currentAssignments)
  }

  /**
   * The legacy entry point for the --generate command.
   *
   * @param zkClient              The ZooKeeper client to use.
   * @param reassignmentJson      The JSON string to use for the topics to reassign.
   * @param brokerListString      The comma-separated string of broker IDs to use.
   * @param enableRackAwareness   True if rack-awareness should be enabled.
   *
   * @return                      A tuple containing the proposed assignment and the
   *                              current assignment.
   */
  def generateAssignment(zkClient: KafkaZkClient,
                         reassignmentJson: String,
                         brokerListString: String,
                         enableRackAwareness: Boolean)
                         : (Map[TopicPartition, Seq[Int]], Map[TopicPartition, Seq[Int]]) = {
    val (brokersToReassign, topicsToReassign) =
      parseGenerateAssignmentArgs(reassignmentJson, brokerListString)
    val currentAssignments = zkClient.getReplicaAssignmentForTopics(topicsToReassign.toSet)
    val brokerMetadatas = getBrokerMetadata(zkClient, brokersToReassign, enableRackAwareness)
    val proposedAssignments = calculateAssignment(currentAssignments, brokerMetadatas)
    println("Current partition replica assignment\n%s\n".
      format(formatAsReassignmentJson(currentAssignments, Map.empty)))
    println("Proposed partition reassignment configuration\n%s".
      format(formatAsReassignmentJson(proposedAssignments, Map.empty)))
    (proposedAssignments, currentAssignments)
  }

  /**
   * Calculate the new partition assignments to suggest in --generate.
   *
   * @param currentAssignment  The current partition assignments.
   * @param brokerMetadatas    The rack information for each broker.
   *
   * @return                   A map from partitions to the proposed assignments for each.
   */
  def calculateAssignment(currentAssignment: Map[TopicPartition, Seq[Int]],
                          brokerMetadatas: Seq[BrokerMetadata])
                          : Map[TopicPartition, Seq[Int]] = {
    val groupedByTopic = currentAssignment.groupBy { case (tp, _) => tp.topic }
    val proposedAssignments = mutable.Map[TopicPartition, Seq[Int]]()
    groupedByTopic.forKeyValue { (topic, assignment) =>
      val (_, replicas) = assignment.head
      val assignedReplicas = AdminUtils.
        assignReplicasToBrokers(brokerMetadatas, assignment.size, replicas.size)
      proposedAssignments ++= assignedReplicas.map { case (partition, replicas) =>
        new TopicPartition(topic, partition) -> replicas
      }
    }
    proposedAssignments
  }

  private def describeTopics(adminClient: Admin,
                             topics: Set[String])
                             : Map[String, TopicDescription] = {
    adminClient.describeTopics(topics.asJava).values.asScala.map { case (topicName, topicDescriptionFuture) =>
      try topicName -> topicDescriptionFuture.get
      catch {
        case t: ExecutionException if t.getCause.isInstanceOf[UnknownTopicOrPartitionException] =>
          throw new ExecutionException(
            new UnknownTopicOrPartitionException(s"Topic $topicName not found."))
      }
    }
  }

  /**
   * Get the current replica assignments for some topics.
   *
   * @param adminClient     The AdminClient to use.
   * @param topics          The topics to get information about.
   * @return                A map from partitions to broker assignments.
   *                        If any topic can't be found, an exception will be thrown.
   */
  def getReplicaAssignmentForTopics(adminClient: Admin,
                                    topics: Seq[String])
                                    : Map[TopicPartition, Seq[Int]] = {
    describeTopics(adminClient, topics.toSet).flatMap {
      case (topicName, topicDescription) => topicDescription.partitions.asScala.map { info =>
        (new TopicPartition(topicName, info.partition), info.replicas.asScala.map(_.id))
      }
    }
  }

  /**
   * Get the current replica assignments for some partitions.
   *
   * @param adminClient     The AdminClient to use.
   * @param partitions      The partitions to get information about.
   * @return                A map from partitions to broker assignments.
   *                        If any topic can't be found, an exception will be thrown.
   */
  def getReplicaAssignmentForPartitions(adminClient: Admin,
                                        partitions: Set[TopicPartition])
                                        : Map[TopicPartition, Seq[Int]] = {
    describeTopics(adminClient, partitions.map(_.topic)).flatMap {
      case (topicName, topicDescription) => topicDescription.partitions.asScala.flatMap { info =>
        val tp = new TopicPartition(topicName, info.partition)
        if (partitions.contains(tp)) {
          Some(tp, info.replicas.asScala.map(_.id))
        } else {
          None
        }
      }
    }
  }

  /**
   * Find the rack information for some brokers.
   *
   * @param adminClient         The AdminClient object.
   * @param brokers             The brokers to gather metadata about.
   * @param enableRackAwareness True if we should return rack information, and throw an
   *                            exception if it is inconsistent.
   *
   * @return                    The metadata for each broker that was found.
   *                            Brokers that were not found will be omitted.
   */
  def getBrokerMetadata(adminClient: Admin,
                        brokers: Seq[Int],
                        enableRackAwareness: Boolean): Seq[BrokerMetadata] = {
    val brokerSet = brokers.toSet
    val results = adminClient.describeCluster().nodes.get().asScala.
      filter(node => brokerSet.contains(node.id)).
      map {
        node => if (enableRackAwareness && node.rack != null) {
          BrokerMetadata(node.id, Some(node.rack))
        } else {
          BrokerMetadata(node.id, None)
        }
      }.toSeq
    val numRackless = results.count(_.rack.isEmpty)
    if (enableRackAwareness && numRackless != 0 && numRackless != results.size) {
      throw new AdminOperationException("Not all brokers have rack information. Add " +
        "--disable-rack-aware in command line to make replica assignment without rack " +
        "information.")
    }
    results
  }

  /**
   * Find the metadata for some brokers.
   *
   * @param zkClient              The ZooKeeper client to use.
   * @param brokers               The brokers to gather metadata about.
   * @param enableRackAwareness   True if we should return rack information, and throw an
   *                              exception if it is inconsistent.
   *
   * @return                      The metadata for each broker that was found.
   *                              Brokers that were not found will be omitted.
   */
  def getBrokerMetadata(zkClient: KafkaZkClient,
                        brokers: Seq[Int],
                        enableRackAwareness: Boolean): Seq[BrokerMetadata] = {
    val adminZkClient = new AdminZkClient(zkClient)
    adminZkClient.getBrokerMetadatas(if (enableRackAwareness)
      RackAwareMode.Enforced else RackAwareMode.Disabled, Some(brokers))
  }

  /**
   * Parse and validate data gathered from the command-line for --generate
   * In particular, we parse the JSON and validate that duplicate brokers and
   * topics don't appear.
   *
   * @param reassignmentJson       The JSON passed to --generate .
   * @param brokerList             A list of brokers passed to --generate.
   *
   * @return                       A tuple of brokers to reassign, topics to reassign
   */
  def parseGenerateAssignmentArgs(reassignmentJson: String,
                                  brokerList: String): (Seq[Int], Seq[String]) = {
    val brokerListToReassign = brokerList.split(',').map(_.toInt)
    val duplicateReassignments = CoreUtils.duplicates(brokerListToReassign)
    if (duplicateReassignments.nonEmpty)
      throw new AdminCommandFailedException("Broker list contains duplicate entries: %s".
        format(duplicateReassignments.mkString(",")))
    val topicsToReassign = parseTopicsData(reassignmentJson)
    val duplicateTopicsToReassign = CoreUtils.duplicates(topicsToReassign)
    if (duplicateTopicsToReassign.nonEmpty)
      throw new AdminCommandFailedException("List of topics to reassign contains duplicate entries: %s".
        format(duplicateTopicsToReassign.mkString(",")))
    (brokerListToReassign, topicsToReassign)
  }

  /**
   * The entry point for the --execute and --execute-additional commands.
   *
   * @param adminClient                 The AdminClient to use.
   * @param additional                  Whether --additional was passed.
   * @param reassignmentJson            The JSON string to use for the topics to reassign.
   * @param interBrokerThrottle         The inter-broker throttle to use, or a negative
   *                                    number to skip using a throttle.
   * @param logDirThrottle              The replica log directory throttle to use, or a
   *                                    negative number to skip using a throttle.
   * @param timeoutMs                   The maximum time in ms to wait for log directory
   *                                    replica assignment to begin.
   * @param time                        The Time object to use.
   */
  def executeAssignment(adminClient: Admin,
                        additional: Boolean,
                        reassignmentJson: String,
                        interBrokerThrottle: Long = -1L,
                        logDirThrottle: Long = -1L,
                        timeoutMs: Long = 10000L,
                        time: Time = Time.SYSTEM): Unit = {
    val (proposedParts, proposedReplicas) = parseExecuteAssignmentArgs(reassignmentJson)
    val currentReassignments = adminClient.
      listPartitionReassignments().reassignments().get().asScala
    // If there is an existing assignment, check for --additional before proceeding.
    // This helps avoid surprising users.
    if (!additional && currentReassignments.nonEmpty) {
      throw new TerseReassignmentFailureException(cannotExecuteBecauseOfExistingMessage)
    }
    verifyBrokerIds(adminClient, proposedParts.values.flatten.toSet)
    val currentParts = getReplicaAssignmentForPartitions(adminClient, proposedParts.keySet.toSet)
    println(currentPartitionReplicaAssignmentToString(proposedParts, currentParts))

    if (interBrokerThrottle >= 0 || logDirThrottle >= 0) {
      println(youMustRunVerifyPeriodicallyMessage)

      if (interBrokerThrottle >= 0) {
        val moveMap = calculateProposedMoveMap(currentReassignments, proposedParts, currentParts)
        modifyReassignmentThrottle(adminClient, moveMap, interBrokerThrottle)
      }

      if (logDirThrottle >= 0) {
        val movingBrokers = calculateMovingBrokers(proposedReplicas.keySet.toSet)
        modifyLogDirThrottle(adminClient, movingBrokers, logDirThrottle)
      }
    }

    // Execute the partition reassignments.
    val errors = alterPartitionReassignments(adminClient, proposedParts)
    if (errors.nonEmpty) {
      throw new TerseReassignmentFailureException(
        "Error reassigning partition(s):%n%s".format(
          errors.keySet.toBuffer.sortWith(compareTopicPartitions).map { part =>
            s"$part: ${errors(part).getMessage}"
          }.mkString(System.lineSeparator())))
    }
    println("Successfully started partition reassignment%s for %s".format(
      if (proposedParts.size == 1) "" else "s",
      proposedParts.keySet.toBuffer.sortWith(compareTopicPartitions).mkString(",")))
    if (proposedReplicas.nonEmpty) {
      executeMoves(adminClient, proposedReplicas, timeoutMs, time)
    }
  }

  /**
   * Execute some partition log directory movements.
   *
   * @param adminClient                 The AdminClient to use.
   * @param proposedReplicas            A map from TopicPartitionReplicas to the
   *                                    directories to move them to.
   * @param timeoutMs                   The maximum time in ms to wait for log directory
   *                                    replica assignment to begin.
   * @param time                        The Time object to use.
   */
  def executeMoves(adminClient: Admin,
                   proposedReplicas: Map[TopicPartitionReplica, String],
                   timeoutMs: Long,
                   time: Time): Unit = {
    val startTimeMs = time.milliseconds()
    val pendingReplicas = new mutable.HashMap[TopicPartitionReplica, String]()
    pendingReplicas ++= proposedReplicas
    var done = false
    do {
      val completed = alterReplicaLogDirs(adminClient, pendingReplicas)
      if (completed.nonEmpty) {
        println("Successfully started log directory move%s for: %s".format(
          if (completed.size == 1) "" else "s",
          completed.toBuffer.sortWith(compareTopicPartitionReplicas).mkString(",")))
      }
      pendingReplicas --= completed
      if (pendingReplicas.isEmpty) {
        done = true
      } else if (time.milliseconds() >= startTimeMs + timeoutMs) {
        throw new TerseReassignmentFailureException(
          "Timed out before log directory move%s could be started for: %s".format(
            if (pendingReplicas.size == 1) "" else "s",
            pendingReplicas.keySet.toBuffer.sortWith(compareTopicPartitionReplicas).
              mkString(",")))
      } else {
        // If a replica has been moved to a new host and we also specified a particular
        // log directory, we will have to keep retrying the alterReplicaLogDirs
        // call.  It can't take effect until the replica is moved to that host.
        time.sleep(100)
      }
    } while (!done)
  }

  /**
   * Entry point for the --list command.
   *
   * @param adminClient   The AdminClient to use.
   */
  def listReassignments(adminClient: Admin): Unit = {
    println(curReassignmentsToString(adminClient))
  }

  /**
   * Convert the current partition reassignments to text.
   *
   * @param adminClient   The AdminClient to use.
   * @return              A string describing the current partition reassignments.
   */
  def curReassignmentsToString(adminClient: Admin): String = {
    val currentReassignments = adminClient.
      listPartitionReassignments().reassignments().get().asScala
    val text = currentReassignments.keySet.toBuffer.sortWith(compareTopicPartitions).map { part =>
      val reassignment = currentReassignments(part)
      val replicas = reassignment.replicas.asScala
      val addingReplicas = reassignment.addingReplicas.asScala
      val removingReplicas = reassignment.removingReplicas.asScala
      "%s: replicas: %s.%s%s".format(part, replicas.mkString(","),
        if (addingReplicas.isEmpty) "" else
          " adding: %s.".format(addingReplicas.mkString(",")),
        if (removingReplicas.isEmpty) "" else
          " removing: %s.".format(removingReplicas.mkString(",")))
    }.mkString(System.lineSeparator())
    if (text.isEmpty) {
      "No partition reassignments found."
    } else {
      "Current partition reassignments:%n%s".format(text)
    }
  }

  /**
   * Verify that all the brokers in an assignment exist.
   *
   * @param adminClient                 The AdminClient to use.
   * @param brokers                     The broker IDs to verify.
   */
  def verifyBrokerIds(adminClient: Admin, brokers: Set[Int]): Unit = {
    val allNodeIds = adminClient.describeCluster().nodes().get().asScala.map(_.id).toSet
    brokers.find(!allNodeIds.contains(_)).map {
      id => throw new AdminCommandFailedException(s"Unknown broker id ${id}")
    }
  }

  /**
   * The entry point for the --execute command.
   *
   * @param zkClient                    The ZooKeeper client to use.
   * @param reassignmentJson            The JSON string to use for the topics to reassign.
   * @param interBrokerThrottle         The inter-broker throttle to use, or a negative number
   *                                    to skip using a throttle.
   */
  def executeAssignment(zkClient: KafkaZkClient,
                        reassignmentJson: String,
                        interBrokerThrottle: Long): Unit = {
    val (proposedParts, proposedReplicas) = parseExecuteAssignmentArgs(reassignmentJson)
    if (proposedReplicas.nonEmpty) {
      throw new AdminCommandFailedException("bootstrap-server needs to be provided when " +
        "replica reassignments are present.")
    }
    verifyReplicasAndBrokersInAssignment(zkClient, proposedParts)

    // Check for the presence of the legacy partition reassignment ZNode.  This actually
    // won't detect all rebalances... only ones initiated by the legacy method.
    // This is a limitation of the legacy ZK API.
    val reassignPartitionsInProgress = zkClient.reassignPartitionsInProgress
    if (reassignPartitionsInProgress) {
      // Note: older versions of this tool would modify the broker quotas here (but not
      // topic quotas, for some reason).  Since it might interfere with other ongoing
      // reassignments, this behavior was dropped as part of the KIP-455 changes. The
      // user can still alter existing throttles by resubmitting the current reassignment
      // and providing the --additional flag.
      throw new TerseReassignmentFailureException(cannotExecuteBecauseOfExistingMessage)
    }
    val currentParts = zkClient.getReplicaAssignmentForTopics(
      proposedParts.map(_._1.topic()).toSet)
    println(currentPartitionReplicaAssignmentToString(proposedParts, currentParts))

    if (interBrokerThrottle >= 0) {
      println(youMustRunVerifyPeriodicallyMessage)
      val moveMap = calculateProposedMoveMap(Map.empty, proposedParts, currentParts)
      val leaderThrottles = calculateLeaderThrottles(moveMap)
      val followerThrottles = calculateFollowerThrottles(moveMap)
      modifyTopicThrottles(zkClient, leaderThrottles, followerThrottles)
      val reassigningBrokers = calculateReassigningBrokers(moveMap)
      modifyBrokerThrottles(zkClient, reassigningBrokers, interBrokerThrottle)
      println(s"The inter-broker throttle limit was set to ${interBrokerThrottle} B/s")
    }
    zkClient.createPartitionReassignment(proposedParts)
    println("Successfully started partition reassignment%s for %s".format(
      if (proposedParts.size == 1) "" else "s",
      proposedParts.keySet.toBuffer.sortWith(compareTopicPartitions).mkString(",")))
  }

  /**
   * Return the string which we want to print to describe the current partition assignment.
   *
   * @param proposedParts               The proposed partition assignment.
   * @param currentParts                The current partition assignment.
   *
   * @return                            The string to print.  We will only print information about
   *                                    partitions that appear in the proposed partition assignment.
   */
  def currentPartitionReplicaAssignmentToString(proposedParts: Map[TopicPartition, Seq[Int]],
                                                currentParts: Map[TopicPartition, Seq[Int]]): String = {
    "Current partition replica assignment%n%n%s%n%nSave this to use as the %s".
        format(formatAsReassignmentJson(currentParts.filter { case (k, _) => proposedParts.contains(k) }.toMap, Map.empty),
              "--reassignment-json-file option during rollback")
  }

  /**
   * Verify that the replicas and brokers referenced in the given partition assignment actually
   * exist.  This is necessary when using the deprecated ZK API, since ZooKeeper itself can't
   * validate what we're applying.
   *
   * @param zkClient                    The ZooKeeper client to use.
   * @param proposedParts               The partition assignment.
   */
  def verifyReplicasAndBrokersInAssignment(zkClient: KafkaZkClient,
                                           proposedParts: Map[TopicPartition, Seq[Int]]): Unit = {
    // check that all partitions in the proposed assignment exist in the cluster
    val proposedTopics = proposedParts.map { case (tp, _) => tp.topic }
    val existingAssignment = zkClient.getReplicaAssignmentForTopics(proposedTopics.toSet)
    val nonExistentPartitions = proposedParts.map { case (tp, _) => tp }.filterNot(existingAssignment.contains)
    if (nonExistentPartitions.nonEmpty)
      throw new AdminCommandFailedException("The proposed assignment contains non-existent partitions: " +
        nonExistentPartitions)

    // check that all brokers in the proposed assignment exist in the cluster
    val existingBrokerIDs = zkClient.getSortedBrokerList
    val nonExistingBrokerIDs = proposedParts.toMap.values.flatten.filterNot(existingBrokerIDs.contains).toSet
    if (nonExistingBrokerIDs.nonEmpty)
      throw new AdminCommandFailedException("The proposed assignment contains non-existent brokerIDs: " + nonExistingBrokerIDs.mkString(","))
  }

  /**
   * Execute the given partition reassignments.
   *
   * @param adminClient       The admin client object to use.
   * @param reassignments     A map from topic names to target replica assignments.
   * @return                  A map from partition objects to error strings.
   */
  def alterPartitionReassignments(adminClient: Admin,
                                  reassignments: Map[TopicPartition, Seq[Int]]): Map[TopicPartition, Throwable] = {
    val results = adminClient.alterPartitionReassignments(reassignments.map { case (part, replicas) =>
      (part, Optional.of(new NewPartitionReassignment(replicas.map(Integer.valueOf).asJava)))
    }.asJava).values().asScala
    results.flatMap {
      case (part, future) => {
        try {
          future.get()
          None
        } catch {
          case t: ExecutionException => Some(part, t.getCause())
        }
      }
    }
  }

  /**
   * Cancel the given partition reassignments.
   *
   * @param adminClient       The admin client object to use.
   * @param reassignments     The partition reassignments to cancel.
   * @return                  A map from partition objects to error strings.
   */
  def cancelPartitionReassignments(adminClient: Admin,
                                   reassignments: Set[TopicPartition])
  : Map[TopicPartition, Throwable] = {
    val results = adminClient.alterPartitionReassignments(reassignments.map {
      (_, Optional.empty[NewPartitionReassignment]())
    }.toMap.asJava).values().asScala
    results.flatMap { case (part, future) =>
      try {
        future.get()
        None
      } catch {
        case t: ExecutionException => Some(part, t.getCause())
      }
    }
  }

  /**
   * Compute the in progress partition move from the current reassignments.
   * @param currentReassignments All replicas, adding replicas and removing replicas of target partitions
   */
  private def calculateCurrentMoveMap(currentReassignments: Map[TopicPartition, PartitionReassignment]): MoveMap = {
    val moveMap = new mutable.HashMap[String, mutable.Map[Int, PartitionMove]]()
    // Add the current reassignments to the move map.
    currentReassignments.forKeyValue { (part, reassignment) =>
      val allReplicas = reassignment.replicas().asScala.map(Int.unbox)
      val addingReplicas = reassignment.addingReplicas.asScala.map(Int.unbox)

      // The addingReplicas is included in the replicas during reassignment
      val sources = mutable.Set[Int]() ++ allReplicas.diff(addingReplicas)
      val destinations = mutable.Set[Int]() ++ addingReplicas

      val partMoves = moveMap.getOrElseUpdate(part.topic, new mutable.HashMap[Int, PartitionMove])
      partMoves.put(part.partition, PartitionMove(sources, destinations))
    }
    moveMap
  }

  /**
   * Calculate the global map of all partitions that are moving.
   *
   * @param currentReassignments    The currently active reassignments.
   * @param proposedParts           The proposed location of the partitions (destinations replicas only).
   * @param currentParts            The current location of the partitions that we are
   *                                proposing to move.
   * @return                        A map from topic name to partition map.
   *                                The partition map is keyed on partition index and contains
   *                                the movements for that partition.
   */
  def calculateProposedMoveMap(currentReassignments: Map[TopicPartition, PartitionReassignment],
                               proposedParts: Map[TopicPartition, Seq[Int]],
                               currentParts: Map[TopicPartition, Seq[Int]]): MoveMap = {
    val moveMap = calculateCurrentMoveMap(currentReassignments)

    proposedParts.forKeyValue { (part, replicas) =>
      val partMoves = moveMap.getOrElseUpdate(part.topic, new mutable.HashMap[Int, PartitionMove])

      // If there is a reassignment in progress, use the sources from moveMap, otherwise
      // use the sources from currentParts
      val sources = mutable.Set[Int]() ++ (partMoves.get(part.partition) match {
        case Some(move) => move.sources.toSeq
        case None => currentParts.getOrElse(part,
          throw new RuntimeException(s"Trying to reassign a topic partition $part with 0 replicas"))
      })
      val destinations = mutable.Set[Int]() ++ replicas.diff(sources.toSeq)

      partMoves.put(part.partition,
        PartitionMove(sources, destinations))
    }
    moveMap
  }

  /**
   * Calculate the leader throttle configurations to use.
   *
   * @param moveMap   The movements.
   * @return          A map from topic names to leader throttle configurations.
   */
  def calculateLeaderThrottles(moveMap: MoveMap): Map[String, String] = {
    moveMap.map {
      case (topicName, partMoveMap) => {
        val components = new mutable.TreeSet[String]
        partMoveMap.forKeyValue { (partId, move) =>
          move.sources.foreach(source => components.add("%d:%d".format(partId, source)))
        }
        (topicName, components.mkString(","))
      }
    }
  }

  /**
   * Calculate the follower throttle configurations to use.
   *
   * @param moveMap   The movements.
   * @return          A map from topic names to follower throttle configurations.
   */
  def calculateFollowerThrottles(moveMap: MoveMap): Map[String, String] = {
    moveMap.map {
      case (topicName, partMoveMap) => {
        val components = new mutable.TreeSet[String]
        partMoveMap.forKeyValue { (partId, move) =>
          move.destinations.foreach(destination =>
            if (!move.sources.contains(destination)) {
              components.add("%d:%d".format(partId, destination))
            })
        }
        (topicName, components.mkString(","))
      }
    }
  }

  /**
   * Calculate all the brokers which are involved in the given partition reassignments.
   *
   * @param moveMap       The partition movements.
   * @return              A set of all the brokers involved.
   */
  def calculateReassigningBrokers(moveMap: MoveMap): Set[Int] = {
    val reassigningBrokers = new mutable.TreeSet[Int]
    moveMap.values.foreach {
      _.values.foreach {
        partMove =>
          partMove.sources.foreach(reassigningBrokers.add)
          partMove.destinations.foreach(reassigningBrokers.add)
      }
    }
    reassigningBrokers.toSet
  }

  /**
   * Calculate all the brokers which are involved in the given directory movements.
   *
   * @param replicaMoves  The replica movements.
   * @return              A set of all the brokers involved.
   */
  def calculateMovingBrokers(replicaMoves: Set[TopicPartitionReplica]): Set[Int] = {
    replicaMoves.map(_.brokerId())
  }

  /**
   * Modify the topic configurations that control inter-broker throttling.
   *
   * @param adminClient         The adminClient object to use.
   * @param leaderThrottles     A map from topic names to leader throttle configurations.
   * @param followerThrottles   A map from topic names to follower throttle configurations.
   */
  def modifyTopicThrottles(adminClient: Admin,
                           leaderThrottles: Map[String, String],
                           followerThrottles: Map[String, String]): Unit = {
    val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
    val topicNames = leaderThrottles.keySet ++ followerThrottles.keySet
    topicNames.foreach { topicName =>
      val ops = new util.ArrayList[AlterConfigOp]
      leaderThrottles.get(topicName).foreach { value =>
        ops.add(new AlterConfigOp(new ConfigEntry(topicLevelLeaderThrottle, value), OpType.SET))
      }
      followerThrottles.get(topicName).foreach { value =>
        ops.add(new AlterConfigOp(new ConfigEntry(topicLevelFollowerThrottle, value), OpType.SET))
      }
      if (!ops.isEmpty) {
        configs.put(new ConfigResource(ConfigResource.Type.TOPIC, topicName), ops)
      }
    }
    adminClient.incrementalAlterConfigs(configs).all().get()
  }

  /**
   * Modify the topic configurations that control inter-broker throttling.
   *
   * @param zkClient            The ZooKeeper client to use.
   * @param leaderThrottles     A map from topic names to leader throttle configurations.
   * @param followerThrottles   A map from topic names to follower throttle configurations.
   */
  def modifyTopicThrottles(zkClient: KafkaZkClient,
                           leaderThrottles: Map[String, String],
                           followerThrottles: Map[String, String]): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)
    val topicNames = leaderThrottles.keySet ++ followerThrottles.keySet
    topicNames.foreach { topicName =>
      val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topicName)
      leaderThrottles.get(topicName).map(configs.put(topicLevelLeaderThrottle, _))
      followerThrottles.get(topicName).map(configs.put(topicLevelFollowerThrottle, _))
      adminZkClient.changeTopicConfig(topicName, configs)
    }
  }

  private def modifyReassignmentThrottle(admin: Admin, moveMap: MoveMap, interBrokerThrottle: Long): Unit = {
    val leaderThrottles = calculateLeaderThrottles(moveMap)
    val followerThrottles = calculateFollowerThrottles(moveMap)
    modifyTopicThrottles(admin, leaderThrottles, followerThrottles)

    val reassigningBrokers = calculateReassigningBrokers(moveMap)
    modifyInterBrokerThrottle(admin, reassigningBrokers, interBrokerThrottle)
  }

  /**
   * Modify the leader/follower replication throttles for a set of brokers.
   *
   * @param adminClient The Admin instance to use
   * @param reassigningBrokers The set of brokers involved in the reassignment
   * @param interBrokerThrottle The new throttle (ignored if less than 0)
   */
  def modifyInterBrokerThrottle(adminClient: Admin,
                                reassigningBrokers: Set[Int],
                                interBrokerThrottle: Long): Unit = {
    if (interBrokerThrottle >= 0) {
      val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
      reassigningBrokers.foreach { brokerId =>
        val ops = new util.ArrayList[AlterConfigOp]
        ops.add(new AlterConfigOp(new ConfigEntry(brokerLevelLeaderThrottle,
          interBrokerThrottle.toString), OpType.SET))
        ops.add(new AlterConfigOp(new ConfigEntry(brokerLevelFollowerThrottle,
          interBrokerThrottle.toString), OpType.SET))
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString), ops)
      }
      adminClient.incrementalAlterConfigs(configs).all().get()
      println(s"The inter-broker throttle limit was set to $interBrokerThrottle B/s")
    }
  }

  /**
   * Modify the log dir reassignment throttle for a set of brokers.
   *
   * @param admin The Admin instance to use
   * @param movingBrokers The set of broker to alter the throttle of
   * @param logDirThrottle The new throttle (ignored if less than 0)
   */
  def modifyLogDirThrottle(admin: Admin,
                           movingBrokers: Set[Int],
                           logDirThrottle: Long): Unit = {
    if (logDirThrottle >= 0) {
      val configs = new util.HashMap[ConfigResource, util.Collection[AlterConfigOp]]()
      movingBrokers.foreach { brokerId =>
        val ops = new util.ArrayList[AlterConfigOp]
        ops.add(new AlterConfigOp(new ConfigEntry(brokerLevelLogDirThrottle, logDirThrottle.toString), OpType.SET))
        configs.put(new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString), ops)
      }
      admin.incrementalAlterConfigs(configs).all().get()
      println(s"The replica-alter-dir throttle limit was set to $logDirThrottle B/s")
    }
  }

  /**
   * Modify the broker-level configurations for leader and follower throttling.
   *
   * @param zkClient            The ZooKeeper client to use.
   * @param reassigningBrokers  The brokers to reconfigure.
   * @param interBrokerThrottle The throttle value to set.
   */
  def modifyBrokerThrottles(zkClient: KafkaZkClient,
                            reassigningBrokers: Set[Int],
                            interBrokerThrottle: Long): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)
    for (id <- reassigningBrokers) {
      val configs = adminZkClient.fetchEntityConfig(ConfigType.Broker, id.toString)
      configs.put(brokerLevelLeaderThrottle, interBrokerThrottle.toString)
      configs.put(brokerLevelFollowerThrottle, interBrokerThrottle.toString)
      adminZkClient.changeBrokerConfig(Seq(id), configs)
    }
  }

  /**
   * Parse the reassignment JSON string passed to the --execute command.
   *
   * @param reassignmentJson  The JSON string.
   * @return                  A tuple of the partitions to be reassigned and the replicas
   *                          to be reassigned.
   */
  def parseExecuteAssignmentArgs(reassignmentJson: String)
      : (Map[TopicPartition, Seq[Int]], Map[TopicPartitionReplica, String]) = {
    val (partitionsToBeReassigned, replicaAssignment) = parsePartitionReassignmentData(reassignmentJson)
    if (partitionsToBeReassigned.isEmpty)
      throw new AdminCommandFailedException("Partition reassignment list cannot be empty")
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
    (partitionsToBeReassigned.toMap, replicaAssignment)
  }

  /**
   * The entry point for the --cancel command.
   *
   * @param adminClient           The AdminClient to use.
   * @param jsonString            The JSON string to use for the topics and partitions to cancel.
   * @param preserveThrottles     True if we should avoid changing topic or broker throttles.
   * @param timeoutMs             The maximum time in ms to wait for log directory
   *                              replica assignment to begin.
   * @param time                  The Time object to use.
   *
   * @return                      A tuple of the partition reassignments that were cancelled,
   *                              and the replica movements that were cancelled.
   */
  def cancelAssignment(adminClient: Admin,
                       jsonString: String,
                       preserveThrottles: Boolean,
                       timeoutMs: Long = 10000L,
                       time: Time = Time.SYSTEM)
                       : (Set[TopicPartition], Set[TopicPartitionReplica]) = {
    val (targetParts, targetReplicas) = parsePartitionReassignmentData(jsonString)
    val targetPartsSet = targetParts.map(_._1).toSet
    val curReassigningParts = adminClient.listPartitionReassignments(targetPartsSet.asJava).
        reassignments().get().asScala.flatMap {
      case (part, reassignment) => if (!reassignment.addingReplicas().isEmpty ||
          !reassignment.removingReplicas().isEmpty) {
        Some(part)
      } else {
        None
      }
    }.toSet
    if (curReassigningParts.nonEmpty) {
      val errors = cancelPartitionReassignments(adminClient, curReassigningParts)
      if (errors.nonEmpty) {
        throw new TerseReassignmentFailureException(
          "Error cancelling partition reassignment%s for:%n%s".format(
            if (errors.size == 1) "" else "s",
            errors.keySet.toBuffer.sortWith(compareTopicPartitions).map {
              part => s"${part}: ${errors(part).getMessage}"
            }.mkString(System.lineSeparator())))
      }
      println("Successfully cancelled partition reassignment%s for: %s".format(
        if (curReassigningParts.size == 1) "" else "s",
        s"${curReassigningParts.toBuffer.sortWith(compareTopicPartitions).mkString(",")}"))
    } else {
      println("None of the specified partition reassignments are active.")
    }
    val curMovingParts = findLogDirMoveStates(adminClient, targetReplicas).flatMap {
      case (part, moveState) => moveState match {
        case state: ActiveMoveState => Some(part, state.currentLogDir)
        case _ => None
      }
    }.toMap
    if (curMovingParts.isEmpty) {
      println("None of the specified partition moves are active.")
    } else {
      executeMoves(adminClient, curMovingParts, timeoutMs, time)
    }
    if (!preserveThrottles) {
      clearAllThrottles(adminClient, targetParts)
    }
    (curReassigningParts, curMovingParts.keySet)
  }

  def formatAsReassignmentJson(partitionsToBeReassigned: Map[TopicPartition, Seq[Int]],
                               replicaLogDirAssignment: Map[TopicPartitionReplica, String]): String = {
    Json.encodeAsString(Map(
      "version" -> 1,
      "partitions" -> partitionsToBeReassigned.keySet.toBuffer.sortWith(compareTopicPartitions).map {
        tp =>
          val replicas = partitionsToBeReassigned(tp)
          Map(
            "topic" -> tp.topic,
            "partition" -> tp.partition,
            "replicas" -> replicas.asJava,
            "log_dirs" -> replicas.map(r => replicaLogDirAssignment.getOrElse(new TopicPartitionReplica(tp.topic, tp.partition, r), AnyLogDir)).asJava
          ).asJava
      }.asJava
    ).asJava)
  }

  def parseTopicsData(jsonData: String): Seq[String] = {
    Json.parseFull(jsonData) match {
      case Some(js) =>
        val version = js.asJsonObject.get("version") match {
          case Some(jsonValue) => jsonValue.to[Int]
          case None => EarliestTopicsJsonVersion
        }
        parseTopicsData(version, js)
      case None => throw new AdminOperationException("The input string is not a valid JSON")
    }
  }

  def parseTopicsData(version: Int, js: JsonValue): Seq[String] = {
    version match {
      case 1 =>
        for {
          partitionsSeq <- js.asJsonObject.get("topics").toSeq
          p <- partitionsSeq.asJsonArray.iterator
        } yield p.asJsonObject("topic").to[String]
      case _ => throw new AdminOperationException(s"Not supported version field value $version")
    }
  }

  def parsePartitionReassignmentData(jsonData: String): (Seq[(TopicPartition, Seq[Int])], Map[TopicPartitionReplica, String]) = {
    Json.tryParseFull(jsonData) match {
      case Right(js) =>
        val version = js.asJsonObject.get("version") match {
          case Some(jsonValue) => jsonValue.to[Int]
          case None => EarliestVersion
        }
        parsePartitionReassignmentData(version, js)
      case Left(f) =>
        throw new AdminOperationException(f)
    }
  }

  // Parses without deduplicating keys so the data can be checked before allowing reassignment to proceed
  def parsePartitionReassignmentData(version:Int, jsonData: JsonValue): (Seq[(TopicPartition, Seq[Int])], Map[TopicPartitionReplica, String]) = {
    version match {
      case 1 =>
        val partitionAssignment = mutable.ListBuffer.empty[(TopicPartition, Seq[Int])]
        val replicaAssignment = mutable.Map.empty[TopicPartitionReplica, String]
        for {
          partitionsSeq <- jsonData.asJsonObject.get("partitions").toSeq
          p <- partitionsSeq.asJsonArray.iterator
        } {
          val partitionFields = p.asJsonObject
          val topic = partitionFields("topic").to[String]
          val partition = partitionFields("partition").to[Int]
          val newReplicas = partitionFields("replicas").to[Seq[Int]]
          val newLogDirs = partitionFields.get("log_dirs") match {
            case Some(jsonValue) => jsonValue.to[Seq[String]]
            case None => newReplicas.map(_ => AnyLogDir)
          }
          if (newReplicas.size != newLogDirs.size)
            throw new AdminCommandFailedException(s"Size of replicas list $newReplicas is different from " +
              s"size of log dirs list $newLogDirs for partition ${new TopicPartition(topic, partition)}")
          partitionAssignment += (new TopicPartition(topic, partition) -> newReplicas)
          replicaAssignment ++= newReplicas.zip(newLogDirs).map { case (replica, logDir) =>
            new TopicPartitionReplica(topic, partition, replica) -> logDir
          }.filter(_._2 != AnyLogDir)
        }
        (partitionAssignment, replicaAssignment)
      case _ => throw new AdminOperationException(s"Not supported version field value $version")
    }
  }

  def validateAndParseArgs(args: Array[String]): ReassignPartitionsCommandOptions = {
    val opts = new ReassignPartitionsCommandOptions(args)

    CommandLineUtils.printHelpAndExitIfNeeded(opts, helpText)

    // Determine which action we should perform.
    val validActions = Seq(opts.generateOpt, opts.executeOpt, opts.verifyOpt,
                           opts.cancelOpt, opts.listOpt)
    val allActions = validActions.filter(opts.options.has _)
    if (allActions.size != 1) {
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: %s".format(
        validActions.map("--" + _.options().get(0)).mkString(", ")))
    }
    val action = allActions(0)

    // Check that we have either the --zookeeper option or the --bootstrap-server set.
    // It would be nice to enforce that we can only have one of these options set at once.  Unfortunately,
    // previous versions of this tool supported setting both options together.  To avoid breaking backwards
    // compatibility, we will follow suit, for now.  This issue will eventually be resolved when we remove
    // the --zookeeper option.
    if (!opts.options.has(opts.zkConnectOpt) && !opts.options.has(opts.bootstrapServerOpt))
      CommandLineUtils.printUsageAndDie(opts.parser, "Please specify --bootstrap-server")

    // Make sure that we have all the required arguments for our action.
    val requiredArgs = Map(
      opts.verifyOpt -> collection.immutable.Seq(
        opts.reassignmentJsonFileOpt
      ),
      opts.generateOpt -> collection.immutable.Seq(
        opts.topicsToMoveJsonFileOpt,
        opts.brokerListOpt
      ),
      opts.executeOpt -> collection.immutable.Seq(
        opts.reassignmentJsonFileOpt
      ),
      opts.cancelOpt -> collection.immutable.Seq(
        opts.reassignmentJsonFileOpt
      ),
      opts.listOpt -> collection.immutable.Seq.empty
    )
    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, requiredArgs(action): _*)

    // Make sure that we didn't specify any arguments that are incompatible with our chosen action.
    val permittedArgs = Map(
      opts.verifyOpt -> Seq(
        opts.bootstrapServerOpt,
        opts.commandConfigOpt,
        opts.preserveThrottlesOpt,
        opts.zkConnectOpt
      ),
      opts.generateOpt -> Seq(
        opts.bootstrapServerOpt,
        opts.brokerListOpt,
        opts.commandConfigOpt,
        opts.disableRackAware,
        opts.zkConnectOpt
      ),
      opts.executeOpt -> Seq(
        opts.additionalOpt,
        opts.bootstrapServerOpt,
        opts.commandConfigOpt,
        opts.interBrokerThrottleOpt,
        opts.replicaAlterLogDirsThrottleOpt,
        opts.timeoutOpt,
        opts.zkConnectOpt
      ),
      opts.cancelOpt -> Seq(
        opts.bootstrapServerOpt,
        opts.commandConfigOpt,
        opts.preserveThrottlesOpt,
        opts.timeoutOpt
      ),
      opts.listOpt -> Seq(
        opts.bootstrapServerOpt,
        opts.commandConfigOpt
      )
    )
    opts.options.specs.forEach(opt => {
      if (!opt.equals(action) &&
          !requiredArgs(action).contains(opt) &&
          !permittedArgs(action).contains(opt)) {
        CommandLineUtils.printUsageAndDie(opts.parser,
          """Option "%s" can't be used with action "%s"""".format(opt, action))
      }
    })
    if (!opts.options.has(opts.bootstrapServerOpt)) {
      val bootstrapServerOnlyArgs = Seq(
        opts.additionalOpt,
        opts.cancelOpt,
        opts.commandConfigOpt,
        opts.replicaAlterLogDirsThrottleOpt,
        opts.listOpt,
        opts.timeoutOpt
      )
      bootstrapServerOnlyArgs.foreach {
        opt => if (opts.options.has(opt)) {
          throw new RuntimeException("You must specify --bootstrap-server " +
            """when using "%s"""".format(opt))
        }
      }
    }
    opts
  }

  def alterReplicaLogDirs(adminClient: Admin,
                          assignment: Map[TopicPartitionReplica, String])
                          : Set[TopicPartitionReplica] = {
    adminClient.alterReplicaLogDirs(assignment.asJava).values().asScala.flatMap {
      case (replica, future) => {
        try {
          future.get()
          Some(replica)
        } catch {
          case t: ExecutionException =>
            t.getCause match {
              // Ignore ReplicaNotAvailableException.  It is OK if the replica is not
              // available at this moment.
              case _: ReplicaNotAvailableException => None
              case e: Throwable =>
                throw new AdminCommandFailedException(s"Failed to alter dir for $replica", e)
            }
        }
      }
    }.toSet
  }

  sealed class ReassignPartitionsCommandOptions(args: Array[String]) extends CommandDefaultOptions(args)  {
    // Actions
    val verifyOpt = parser.accepts("verify", "Verify if the reassignment completed as specified by the --reassignment-json-file option. If there is a throttle engaged for the replicas specified, and the rebalance has completed, the throttle will be removed")
    val generateOpt = parser.accepts("generate", "Generate a candidate partition reassignment configuration." +
      " Note that this only generates a candidate assignment, it does not execute it.")
    val executeOpt = parser.accepts("execute", "Kick off the reassignment as specified by the --reassignment-json-file option.")
    val cancelOpt = parser.accepts("cancel", "Cancel an active reassignment.")
    val listOpt = parser.accepts("list", "List all active partition reassignments.")

    // Arguments
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "the server(s) to use for bootstrapping. REQUIRED if " +
                      "an absolute path of the log directory is specified for any replica in the reassignment json file, " +
                      "or if --zookeeper is not given.")
                      .withRequiredArg
                      .describedAs("Server(s) to use for bootstrapping")
                      .ofType(classOf[String])
    val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
                      .withRequiredArg
                      .describedAs("Admin client property file")
                      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "DEPRECATED: The connection string for the zookeeper connection in the " +
                      "form host:port. Multiple URLS can be given to allow fail-over.  Please use --bootstrap-server instead.")
                      .withRequiredArg
                      .describedAs("urls")
                      .ofType(classOf[String])
    val reassignmentJsonFileOpt = parser.accepts("reassignment-json-file", "The JSON file with the partition reassignment configuration" +
                      "The format to use is - \n" +
                      "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3],\n\t  \"log_dirs\": [\"dir1\",\"dir2\",\"dir3\"] }],\n\"version\":1\n}\n" +
                      "Note that \"log_dirs\" is optional. When it is specified, its length must equal the length of the replicas list. The value in this list " +
                      "can be either \"any\" or the absolution path of the log directory on the broker. If absolute log directory path is specified, the replica will be moved to the specified log directory on the broker.")
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
    val interBrokerThrottleOpt = parser.accepts("throttle", "The movement of partitions between brokers will be throttled to this value (bytes/sec). " +
      "This option can be included with --execute when a reassignment is started, and it can be altered by resubmitting the current reassignment " +
      "along with the --additional flag. The throttle rate should be at least 1 KB/s.")
      .withRequiredArg()
      .describedAs("throttle")
      .ofType(classOf[Long])
      .defaultsTo(-1)
    val replicaAlterLogDirsThrottleOpt = parser.accepts("replica-alter-log-dirs-throttle",
      "The movement of replicas between log directories on the same broker will be throttled to this value (bytes/sec). " +
        "This option can be included with --execute when a reassignment is started, and it can be altered by resubmitting the current reassignment " +
        "along with the --additional flag. The throttle rate should be at least 1 KB/s.")
      .withRequiredArg()
      .describedAs("replicaAlterLogDirsThrottle")
      .ofType(classOf[Long])
      .defaultsTo(-1)
    val timeoutOpt = parser.accepts("timeout", "The maximum time in ms to wait for log directory replica assignment to begin.")
                      .withRequiredArg()
                      .describedAs("timeout")
                      .ofType(classOf[Long])
                      .defaultsTo(10000)
    val additionalOpt = parser.accepts("additional", "Execute this reassignment in addition to any " +
      "other ongoing ones. This option can also be used to change the throttle of an ongoing reassignment.")
    val preserveThrottlesOpt = parser.accepts("preserve-throttles", "Do not modify broker or topic throttles.")
    options = parser.parse(args : _*)
  }
}
