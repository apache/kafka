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

import joptsimple._
import kafka.common.AdminCommandFailedException
import kafka.utils.Implicits._
import kafka.utils.Whitelist
import kafka.log.LogConfig
import kafka.server.ConfigType
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.errors.{InvalidTopicException, TopicExistsException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.zookeeper.KeeperException.NodeExistsException
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection._

object TopicCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new TopicCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Create, delete, describe, or change a topic.")

    // should have exactly one action
    val actions = Seq(opts.createOpt, opts.listOpt, opts.alterOpt, opts.describeOpt, opts.deleteOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete")

    opts.checkArgs()

    val time = Time.SYSTEM
    val zkClient = KafkaZkClient(opts.options.valueOf(opts.zkConnectOpt), JaasUtils.isZkSecurityEnabled, 30000, 30000,
      Int.MaxValue, time)

    var exitCode = 0
    try {
      if(opts.options.has(opts.createOpt))
        createTopic(zkClient, opts)
      else if(opts.options.has(opts.alterOpt))
        alterTopic(zkClient, opts)
      else if(opts.options.has(opts.listOpt))
        listTopics(zkClient, opts)
      else if(opts.options.has(opts.describeOpt))
        describeTopic(zkClient, opts)
      else if(opts.options.has(opts.deleteOpt))
        deleteTopic(zkClient, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      zkClient.close()
      Exit.exit(exitCode)
    }

  }

  private def getTopics(zkClient: KafkaZkClient, opts: TopicCommandOptions): Seq[String] = {
    val allTopics = zkClient.getAllTopicsInCluster.sorted
    if (opts.options.has(opts.topicOpt)) {
      val topicsSpec = opts.options.valueOf(opts.topicOpt)
      val topicsFilter = new Whitelist(topicsSpec)
      allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics = false))
    } else
      allTopics
  }

  def createTopic(zkClient: KafkaZkClient, opts: TopicCommandOptions) {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    val ifNotExists = opts.options.has(opts.ifNotExistsOpt)
    if (Topic.hasCollisionChars(topic))
      println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide. To avoid issues it is best to use either, but not both.")
    val adminZkClient = new AdminZkClient(zkClient)
    try {
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        adminZkClient.createOrUpdateTopicPartitionAssignmentPathInZK(topic, assignment, configs, update = false)
      } else {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
        val rackAwareMode = if (opts.options.has(opts.disableRackAware)) RackAwareMode.Disabled
                            else RackAwareMode.Enforced
        adminZkClient.createTopic(topic, partitions, replicas, configs, rackAwareMode)
      }
      println("Created topic \"%s\".".format(topic))
    } catch  {
      case e: TopicExistsException => if (!ifNotExists) throw e
    }
  }

  def alterTopic(zkClient: KafkaZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    val ifExists = opts.options.has(opts.ifExistsOpt)
    if (topics.isEmpty && !ifExists) {
      throw new IllegalArgumentException("Topic %s does not exist on ZK path %s".format(opts.options.valueOf(opts.topicOpt),
          opts.options.valueOf(opts.zkConnectOpt)))
    }
    val adminZkClient = new AdminZkClient(zkClient)
    topics.foreach { topic =>
      val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
      if(opts.options.has(opts.configOpt) || opts.options.has(opts.deleteConfigOpt)) {
        println("WARNING: Altering topic configuration from this script has been deprecated and may be removed in future releases.")
        println("         Going forward, please use kafka-configs.sh for this functionality")

        val configsToBeAdded = parseTopicConfigsToBeAdded(opts)
        val configsToBeDeleted = parseTopicConfigsToBeDeleted(opts)
        // compile the final set of configs
        configs ++= configsToBeAdded
        configsToBeDeleted.foreach(config => configs.remove(config))
        adminZkClient.changeTopicConfig(topic, configs)
        println("Updated config for topic \"%s\".".format(topic))
      }

      if(opts.options.has(opts.partitionsOpt)) {
        if (topic == Topic.GROUP_METADATA_TOPIC_NAME) {
          throw new IllegalArgumentException("The number of partitions for the offsets topic cannot be changed.")
        }
        println("WARNING: If partitions are increased for a topic that has a key, the partition " +
          "logic or ordering of the messages will be affected")
        val nPartitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val existingAssignment = zkClient.getReplicaAssignmentForTopics(immutable.Set(topic)).map {
          case (topicPartition, replicas) => topicPartition.partition -> replicas
        }
        if (existingAssignment.isEmpty)
          throw new InvalidTopicException(s"The topic $topic does not exist")
        val replicaAssignmentStr = opts.options.valueOf(opts.replicaAssignmentOpt)
        val newAssignment = Option(replicaAssignmentStr).filter(_.nonEmpty).map { replicaAssignmentString =>
          val startPartitionId = existingAssignment.size
          val partitionList = replicaAssignmentString.split(",").drop(startPartitionId)
          AdminUtils.parseReplicaAssignment(partitionList.mkString(","), startPartitionId)
        }
        val allBrokers = adminZkClient.getBrokerMetadatas()
        adminZkClient.addPartitions(topic, existingAssignment, allBrokers, nPartitions, newAssignment)
        println("Adding partitions succeeded!")
      }
    }
  }

  def listTopics(zkClient: KafkaZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    for(topic <- topics) {
      if (zkClient.isTopicMarkedForDeletion(topic)) {
        println("%s - marked for deletion".format(topic))
      } else {
        println(topic)
      }
    }
  }

  def deleteTopic(zkClient: KafkaZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    val ifExists = opts.options.has(opts.ifExistsOpt)
    if (topics.isEmpty && !ifExists) {
      throw new IllegalArgumentException("Topic %s does not exist on ZK path %s".format(opts.options.valueOf(opts.topicOpt),
          opts.options.valueOf(opts.zkConnectOpt)))
    }
    topics.foreach { topic =>
      try {
        if (Topic.isInternal(topic)) {
          throw new AdminOperationException("Topic %s is a kafka internal topic and is not allowed to be marked for deletion.".format(topic))
        } else {
          zkClient.createDeleteTopicPath(topic)
          println("Topic %s is marked for deletion.".format(topic))
          println("Note: This will have no impact if delete.topic.enable is not set to true.")
        }
      } catch {
        case _: NodeExistsException =>
          println("Topic %s is already marked for deletion.".format(topic))
        case e: AdminOperationException =>
          throw e
        case _: Throwable =>
          throw new AdminOperationException("Error while deleting topic %s".format(topic))
      }
    }
  }

  def describeTopic(zkClient: KafkaZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    val reportUnderReplicatedPartitions = opts.options.has(opts.reportUnderReplicatedPartitionsOpt)
    val reportUnavailablePartitions = opts.options.has(opts.reportUnavailablePartitionsOpt)
    val reportOverriddenConfigs = opts.options.has(opts.topicsWithOverridesOpt)
    val liveBrokers = zkClient.getAllBrokersInCluster.map(_.id).toSet
    val adminZkClient = new AdminZkClient(zkClient)

    for (topic <- topics) {
       zkClient.getPartitionAssignmentForTopics(immutable.Set(topic)).get(topic) match {
        case Some(topicPartitionAssignment) =>
          val describeConfigs: Boolean = !reportUnavailablePartitions && !reportUnderReplicatedPartitions
          val describePartitions: Boolean = !reportOverriddenConfigs
          val sortedPartitions = topicPartitionAssignment.toSeq.sortBy(_._1)
          val markedForDeletion = zkClient.isTopicMarkedForDeletion(topic)
          if (describeConfigs) {
            val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic).asScala
            if (!reportOverriddenConfigs || configs.nonEmpty) {
              val numPartitions = topicPartitionAssignment.size
              val replicationFactor = topicPartitionAssignment.head._2.size
              val configsAsString = configs.map { case (k, v) => s"$k=$v" }.mkString(",")
              val markedForDeletionString = if (markedForDeletion) "\tMarkedForDeletion:true" else ""
              println("Topic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s%s"
                .format(topic, numPartitions, replicationFactor, configsAsString, markedForDeletionString))
            }
          }
          if (describePartitions) {
            for ((partitionId, assignedReplicas) <- sortedPartitions) {
              val leaderIsrEpoch = zkClient.getTopicPartitionState(new TopicPartition(topic, partitionId))
              val inSyncReplicas = if (leaderIsrEpoch.isEmpty) Seq.empty[Int] else leaderIsrEpoch.get.leaderAndIsr.isr
              val leader = if (leaderIsrEpoch.isEmpty) None else Option(leaderIsrEpoch.get.leaderAndIsr.leader)

              if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
                  (reportUnderReplicatedPartitions && inSyncReplicas.size < assignedReplicas.size) ||
                  (reportUnavailablePartitions && (leader.isEmpty || !liveBrokers.contains(leader.get)))) {

                val markedForDeletionString =
                  if (markedForDeletion && !describeConfigs) "\tMarkedForDeletion: true" else ""
                print("\tTopic: " + topic)
                print("\tPartition: " + partitionId)
                print("\tLeader: " + (if(leader.isDefined) leader.get else "none"))
                print("\tReplicas: " + assignedReplicas.mkString(","))
                print("\tIsr: " + inSyncReplicas.mkString(","))
                print(markedForDeletionString)
                println()
              }
            }
          }
        case None =>
          println("Topic " + topic + " doesn't exist!")
      }
    }
  }

  def parseTopicConfigsToBeAdded(opts: TopicCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.configOpt).asScala.map(_.split("""\s*=\s*"""))
    require(configsToBeAdded.forall(config => config.length == 2),
      "Invalid topic config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    LogConfig.validate(props)
    if (props.containsKey(LogConfig.MessageFormatVersionProp)) {
      println(s"WARNING: The configuration ${LogConfig.MessageFormatVersionProp}=${props.getProperty(LogConfig.MessageFormatVersionProp)} is specified. " +
      s"This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker.")
    }
    props
  }

  def parseTopicConfigsToBeDeleted(opts: TopicCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfigOpt)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfigOpt).asScala.map(_.trim())
      val propsToBeDeleted = new Properties
      configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
      LogConfig.validateNames(propsToBeDeleted)
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      val duplicateBrokers = CoreUtils.duplicates(brokerList)
      if (duplicateBrokers.nonEmpty)
        throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicateBrokers.mkString(",")))
      ret.put(i, brokerList.toList)
      if (ret(i).size != ret(0).size)
        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap
  }

  class TopicCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple hosts can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("hosts")
                           .ofType(classOf[String])
    val listOpt = parser.accepts("list", "List all available topics.")
    val createOpt = parser.accepts("create", "Create a new topic.")
    val deleteOpt = parser.accepts("delete", "Delete a topic")
    val alterOpt = parser.accepts("alter", "Alter the number of partitions, replica assignment, and/or configuration for the topic.")
    val describeOpt = parser.accepts("describe", "List details for the given topics.")
    val helpOpt = parser.accepts("help", "Print usage information.")
    val topicOpt = parser.accepts("topic", "The topic to be create, alter or describe. Can also accept a regular " +
                                           "expression except for --create option")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val nl = System.getProperty("line.separator")
    val configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered."  +
                                             "The following is a list of valid configurations: " + nl + LogConfig.configNames.map("\t" + _).mkString(nl) + nl +
                                             "See the Kafka documentation for full details on the topic configs.")
                           .withRequiredArg
                           .describedAs("name=value")
                           .ofType(classOf[String])
    val deleteConfigOpt = parser.accepts("delete-config", "A topic configuration override to be removed for an existing topic (see the list of configurations under the --config option).")
                           .withRequiredArg
                           .describedAs("name")
                           .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
      "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
                           .withRequiredArg
                           .describedAs("# of partitions")
                           .ofType(classOf[java.lang.Integer])
    val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
                           .withRequiredArg
                           .describedAs("replication factor")
                           .ofType(classOf[java.lang.Integer])
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")
                           .withRequiredArg
                           .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                                        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                           .ofType(classOf[String])
    val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                                                            "if set when describing topics, only show under replicated partitions")
    val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                                                            "if set when describing topics, only show partitions whose leader is not available")
    val topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                                                "if set when describing topics, only show topics that have overridden configs")
    val ifExistsOpt = parser.accepts("if-exists",
                                     "if set when altering or deleting topics, the action will only execute if the topic exists")
    val ifNotExistsOpt = parser.accepts("if-not-exists",
                                        "if set when creating topics, the action will only execute if the topic does not already exist")

    val disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment")

    val forceOpt = parser.accepts("force", "Suppress console prompts")

    val options = parser.parse(args : _*)

    val allTopicLevelOpts: Set[OptionSpec[_]] = Set(alterOpt, createOpt, describeOpt, listOpt, deleteOpt)

    def checkArgs() {
      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      if (!options.has(listOpt) && !options.has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, configOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, deleteConfigOpt, allTopicLevelOpts -- Set(alterOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, partitionsOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicationFactorOpt, allTopicLevelOpts -- Set(createOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, allTopicLevelOpts -- Set(createOpt,alterOpt))
      if(options.has(createOpt))
          CommandLineUtils.checkInvalidArgs(parser, options, replicaAssignmentOpt, Set(partitionsOpt, replicationFactorOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnderReplicatedPartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnavailablePartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, reportUnavailablePartitionsOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + topicsWithOverridesOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, topicsWithOverridesOpt,
        allTopicLevelOpts -- Set(describeOpt) + reportUnderReplicatedPartitionsOpt + reportUnavailablePartitionsOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, ifExistsOpt, allTopicLevelOpts -- Set(alterOpt, deleteOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, ifNotExistsOpt, allTopicLevelOpts -- Set(createOpt))
    }
  }

  def askToProceed(): Unit = {
    println("Are you sure you want to continue? [y/n]")
    if (!Console.readLine().equalsIgnoreCase("y")) {
      println("Ending your session")
      Exit.exit(0)
    }
  }

}

