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
import java.util.{Collections, Properties}

import joptsimple._
import kafka.common.AdminCommandFailedException
import kafka.log.LogConfig
import kafka.server.ConfigType
import kafka.utils.Implicits._
import kafka.utils._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{ListTopicsOptions, NewPartitions, NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.ConfigResource.Type
import org.apache.kafka.common.errors.{InvalidTopicException, TopicExistsException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.zookeeper.KeeperException.NodeExistsException

import scala.collection.JavaConverters._
import scala.collection._
import scala.io.StdIn

object TopicCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val opts = new TopicCommandOptions(args)
    opts.checkArgs()

    val topicService = if (opts.zkConnect.isDefined)
      ZookeeperTopicService(opts.zkConnect)
    else
      AdminClientTopicService(opts.commandConfig, opts.bootstrapServer)

    var exitCode = 0
    try {
      if (opts.hasCreateOption)
        topicService.createTopic(opts)
      else if (opts.hasAlterOption)
        topicService.alterTopic(opts)
      else if (opts.hasListOption)
        topicService.listTopics(opts)
      else if (opts.hasDescribeOption)
        topicService.describeTopic(opts)
      else if (opts.hasDeleteOption)
        topicService.deleteTopic(opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command : " + e.getMessage)
        error(Utils.stackTrace(e))
        exitCode = 1
    } finally {
      topicService.close()
      Exit.exit(exitCode)
    }
  }



  class CommandTopicPartition(opts: TopicCommandOptions) {
    val name: String = opts.topic.get
    val partitions: Option[Integer] = opts.partitions
    val replicationFactor: Integer = opts.replicationFactor.getOrElse(-1)
    val replicaAssignment: Option[Map[Int, List[Int]]] = opts.replicaAssignment
    val configsToAdd: Properties = parseTopicConfigsToBeAdded(opts)
    val configsToDelete: Seq[String] = parseTopicConfigsToBeDeleted(opts)
    val rackAwareMode: RackAwareMode = opts.rackAwareMode

    def hasReplicaAssignment: Boolean = replicaAssignment.isDefined
    def hasPartitions: Boolean = partitions.isDefined
    def ifTopicDoesntExist(): Boolean = opts.ifNotExists
  }

  case class PartitionDescription(
                                   topic: String,
                                   partition: Int,
                                   leader: Option[Int],
                                   assignedReplicas: Seq[Int],
                                   isr: Seq[Int],
                                   markedForDeletion: Boolean,
                                   describeConfigs: Boolean)

  class DescribeOptions(opts: TopicCommandOptions, liveBrokers: Set[Int]) {
    val describeConfigs: Boolean = !opts.reportUnavailablePartitions && !opts.reportUnderReplicatedPartitions
    val describePartitions: Boolean = !opts.reportOverriddenConfigs
    private def hasUnderreplicatedPartitions(partitionDescription: PartitionDescription) = {
      partitionDescription.isr.size < partitionDescription.assignedReplicas.size
    }
    private def shouldPrintUnderReplicatedPartitions(partitionDescription: PartitionDescription) = {
      opts.reportUnderReplicatedPartitions && hasUnderreplicatedPartitions(partitionDescription)
    }
    private def hasUnavailablePartitions(partitionDescription: PartitionDescription) = {
      partitionDescription.leader.isEmpty || !liveBrokers.contains(partitionDescription.leader.get)
    }
    private def shouldPrintUnavailablePartitions(partitionDescription: PartitionDescription) = {
      opts.reportUnavailablePartitions && hasUnavailablePartitions(partitionDescription)
    }

    def shouldPrintTopicPartition(partitionDesc: PartitionDescription): Boolean = {
      describeConfigs ||
        shouldPrintUnderReplicatedPartitions(partitionDesc) ||
        shouldPrintUnavailablePartitions(partitionDesc)
    }
  }

  trait TopicService extends AutoCloseable {
    def createTopic(opts: TopicCommandOptions): Unit = {
      val topic = new CommandTopicPartition(opts)
      if (Topic.hasCollisionChars(topic.name))
        println("WARNING: Due to limitations in metric names, topics with a period ('.') or underscore ('_') could " +
          "collide. To avoid issues it is best to use either, but not both.")
      createTopic(topic)
    }
    def createTopic(topic: CommandTopicPartition)
    def listTopics(opts: TopicCommandOptions)
    def alterTopic(opts: TopicCommandOptions)
    def describeTopic(opts: TopicCommandOptions)
    def deleteTopic(opts: TopicCommandOptions)
    def getTopics(topicWhitelist: Option[String], excludeInternalTopics: Boolean = false): Seq[String]
  }

  object AdminClientTopicService {
    def createAdminClient(commandConfig: Properties, bootstrapServer: Option[String]): JAdminClient = {
      bootstrapServer match {
        case Some(serverList) => commandConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, serverList)
        case None =>
      }
      JAdminClient.create(commandConfig)
    }

    def apply(commandConfig: Properties, bootstrapServer: Option[String]): AdminClientTopicService =
      new AdminClientTopicService(createAdminClient(commandConfig, bootstrapServer))
  }

  case class AdminClientTopicService private (adminClient: JAdminClient) extends TopicService {

    override def createTopic(topic: CommandTopicPartition): Unit = {
      if (topic.replicationFactor > Short.MaxValue)
        throw new IllegalArgumentException(s"The replication factor's maximum value must be smaller or equal to ${Short.MaxValue}")

      if (!adminClient.listTopics().names().get().contains(topic.name)) {
        val newTopic = if (topic.hasReplicaAssignment)
          new NewTopic(topic.name, asJavaReplicaReassignment(topic.replicaAssignment.get))
        else
          new NewTopic(topic.name, topic.partitions.get, topic.replicationFactor.shortValue())
        val configsMap = topic.configsToAdd.stringPropertyNames()
          .asScala
          .map(name => name -> topic.configsToAdd.getProperty(name))
          .toMap.asJava

        newTopic.configs(configsMap)
        val createResult = adminClient.createTopics(Collections.singleton(newTopic))
        createResult.all().get()
      } else {
        throw new IllegalArgumentException(s"Topic ${topic.name} already exists")
      }
    }

    override def listTopics(opts: TopicCommandOptions): Unit = {
      println(getTopics(opts.topic, opts.excludeInternalTopics).mkString("\n"))
    }

    override def alterTopic(opts: TopicCommandOptions): Unit = {
      val topic = new CommandTopicPartition(opts)
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      ensureTopicExists(topics)
      val topicsInfo = adminClient.describeTopics(topics.asJavaCollection).values()
      adminClient.createPartitions(topics.map {topicName =>
        if (topic.hasReplicaAssignment) {
          val startPartitionId = topicsInfo.get(topicName).get().partitions().size()
          val newAssignment = {
            val replicaMap = topic.replicaAssignment.get.drop(startPartitionId)
            new util.ArrayList(replicaMap.map(p => p._2.asJava).asJavaCollection).asInstanceOf[util.List[util.List[Integer]]]
          }
          topicName -> NewPartitions.increaseTo(topic.partitions.get, newAssignment)
        } else {
          topicName -> NewPartitions.increaseTo(topic.partitions.get)
        }}.toMap.asJava).all().get()
    }

    override def describeTopic(opts: TopicCommandOptions): Unit = {
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      val allConfigs = adminClient.describeConfigs(topics.map(new ConfigResource(Type.TOPIC, _)).asJavaCollection).values()
      val liveBrokers = adminClient.describeCluster().nodes().get().asScala.map(_.id())
      val topicDescriptions = adminClient.describeTopics(topics.asJavaCollection).all().get().values().asScala
      val describeOptions = new DescribeOptions(opts, liveBrokers.toSet)

      for (td <- topicDescriptions) {
        val sortedPartitions = td.partitions().asScala.sortBy(_.partition())
        if (describeOptions.describeConfigs) {
          val config = allConfigs.get(new ConfigResource(Type.TOPIC, td.name())).get()
          val hasNonDefault = config.entries().asScala.exists(!_.isDefault)
          if (!opts.reportOverriddenConfigs || hasNonDefault) {
            val numPartitions = td.partitions().size
            val replicationFactor = td.partitions().iterator().next().replicas().size
            val configsAsString = config.entries().asScala.filter(!_.isDefault).map { ce => s"${ce.name()}=${ce.value()}" }.mkString(",")
            println(s"Topic:${td.name()}\tPartitionCount:$numPartitions\tReplicationFactor:$replicationFactor\tConfigs:$configsAsString")
          }
        }
        if (describeOptions.describePartitions) {
          for (partition <- sortedPartitions) {
            val partitionDesc = PartitionDescription(
              topic = td.name(),
              partition.partition(),
              leader = Option(partition.leader()).map(_.id()),
              assignedReplicas = partition.replicas().asScala.map(_.id()),
              isr = partition.isr().asScala.map(_.id()),
              markedForDeletion = false,
              describeOptions.describeConfigs
            )

            if (describeOptions.shouldPrintTopicPartition(partitionDesc)) {
              printPartition(partitionDesc)
            }
          }
        }
      }
    }

    override def deleteTopic(opts: TopicCommandOptions): Unit = {
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      ensureTopicExists(topics)
      adminClient.deleteTopics(topics.asJavaCollection).all().get()
    }

    override def getTopics(topicWhitelist: Option[String], excludeInternalTopics: Boolean = false): Seq[String] = {
      val allTopics = if (excludeInternalTopics) {
        adminClient.listTopics()
      } else {
        adminClient.listTopics(new ListTopicsOptions().listInternal(true))
      }
      doGetTopics(allTopics.names().get().asScala.toSeq.sorted, topicWhitelist, excludeInternalTopics)
    }

    override def close(): Unit = adminClient.close()
  }

  object ZookeeperTopicService {
    def apply(zkConnect: Option[String]): ZookeeperTopicService =
      new ZookeeperTopicService(KafkaZkClient(zkConnect.get, JaasUtils.isZkSecurityEnabled, 30000, 30000,
        Int.MaxValue, Time.SYSTEM))
  }

  case class ZookeeperTopicService(zkClient: KafkaZkClient) extends TopicService {

    override def createTopic(topic: CommandTopicPartition): Unit = {
      val adminZkClient = new AdminZkClient(zkClient)
      try {
        if (topic.hasReplicaAssignment)
          adminZkClient.createTopicWithAssignment(topic.name, topic.configsToAdd, topic.replicaAssignment.get)
        else
          adminZkClient.createTopic(topic.name, topic.partitions.get, topic.replicationFactor, topic.configsToAdd, topic.rackAwareMode)
        println(s"Created topic ${topic.name}.")
      } catch  {
        case e: TopicExistsException => if (!topic.ifTopicDoesntExist()) throw e
      }
    }

    override def listTopics(opts: TopicCommandOptions): Unit = {
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      for(topic <- topics) {
        if (zkClient.isTopicMarkedForDeletion(topic))
          println(s"$topic - marked for deletion")
        else
          println(topic)
      }
    }

    override def alterTopic(opts: TopicCommandOptions): Unit = {
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      val tp = new CommandTopicPartition(opts)
      ensureTopicExists(topics, opts.ifExists)
      val adminZkClient = new AdminZkClient(zkClient)
      topics.foreach { topic =>
        val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic)
        if(opts.topicConfig.isDefined || opts.configsToDelete.isDefined) {
          println("WARNING: Altering topic configuration from this script has been deprecated and may be removed in future releases.")
          println("         Going forward, please use kafka-configs.sh for this functionality")

          // compile the final set of configs
          configs ++= tp.configsToAdd
          tp.configsToDelete.foreach(config => configs.remove(config))
          adminZkClient.changeTopicConfig(topic, configs)
          println(s"Updated config for topic $topic.")
        }

        if(tp.hasPartitions) {
          if (topic == Topic.GROUP_METADATA_TOPIC_NAME) {
            throw new IllegalArgumentException("The number of partitions for the offsets topic cannot be changed.")
          }
          println("WARNING: If partitions are increased for a topic that has a key, the partition " +
            "logic or ordering of the messages will be affected")
          val existingAssignment = zkClient.getReplicaAssignmentForTopics(immutable.Set(topic)).map {
            case (topicPartition, replicas) => topicPartition.partition -> replicas
          }
          if (existingAssignment.isEmpty)
            throw new InvalidTopicException(s"The topic $topic does not exist")
          val newAssignment = tp.replicaAssignment.getOrElse(Map()).drop(existingAssignment.size)
          val allBrokers = adminZkClient.getBrokerMetadatas()
          val partitions: Integer = tp.partitions.getOrElse(1)
          adminZkClient.addPartitions(topic, existingAssignment, allBrokers, partitions, Option(newAssignment).filter(_.nonEmpty))
          println("Adding partitions succeeded!")
        }
      }
    }

    override def describeTopic(opts: TopicCommandOptions): Unit = {
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      val topicOptWithExits = opts.topic.isDefined && opts.ifExists
      ensureTopicExists(topics, topicOptWithExits)
      val liveBrokers = zkClient.getAllBrokersInCluster.map(_.id).toSet
      val describeOptions = new DescribeOptions(opts, liveBrokers)
      val adminZkClient = new AdminZkClient(zkClient)

      for (topic <- topics) {
        zkClient.getPartitionAssignmentForTopics(immutable.Set(topic)).get(topic) match {
          case Some(topicPartitionAssignment) =>
            val markedForDeletion = zkClient.isTopicMarkedForDeletion(topic)
            if (describeOptions.describeConfigs) {
              val configs = adminZkClient.fetchEntityConfig(ConfigType.Topic, topic).asScala
              if (!opts.reportOverriddenConfigs || configs.nonEmpty) {
                val numPartitions = topicPartitionAssignment.size
                val replicationFactor = topicPartitionAssignment.head._2.size
                val configsAsString = configs.map { case (k, v) => s"$k=$v" }.mkString(",")
                val markedForDeletionString = if (markedForDeletion) "\tMarkedForDeletion:true" else ""
                println(s"Topic:$topic\tPartitionCount:$numPartitions\tReplicationFactor:$replicationFactor\tConfigs:$configsAsString$markedForDeletionString")
              }
            }
            if (describeOptions.describePartitions) {
              for ((partitionId, assignedReplicas) <- topicPartitionAssignment.toSeq.sortBy(_._1)) {
                val leaderIsrEpoch = zkClient.getTopicPartitionState(new TopicPartition(topic, partitionId))
                val partitionDesc = PartitionDescription(
                  topic,
                  partitionId,
                  leader = if (leaderIsrEpoch.isEmpty) None else Option(leaderIsrEpoch.get.leaderAndIsr.leader),
                  assignedReplicas,
                  isr = if (leaderIsrEpoch.isEmpty) Seq.empty[Int] else leaderIsrEpoch.get.leaderAndIsr.isr,
                  markedForDeletion,
                  describeOptions.describeConfigs
                )

                if (describeOptions.shouldPrintTopicPartition(partitionDesc)) {
                  printPartition(partitionDesc)
                }
              }
            }
          case None =>
            println("Topic " + topic + " doesn't exist!")
        }
      }
    }

    override def deleteTopic(opts: TopicCommandOptions): Unit = {
      val topics = getTopics(opts.topic, opts.excludeInternalTopics)
      ensureTopicExists(topics, opts.ifExists)
      topics.foreach { topic =>
        try {
          if (Topic.isInternal(topic)) {
            throw new AdminOperationException(s"Topic $topic is a kafka internal topic and is not allowed to be marked for deletion.")
          } else {
            zkClient.createDeleteTopicPath(topic)
            println(s"Topic $topic is marked for deletion.")
            println("Note: This will have no impact if delete.topic.enable is not set to true.")
          }
        } catch {
          case _: NodeExistsException =>
            println(s"Topic $topic is already marked for deletion.")
          case e: AdminOperationException =>
            throw e
          case _: Throwable =>
            throw new AdminOperationException(s"Error while deleting topic $topic")
        }
      }
    }

    override def getTopics(topicWhitelist: Option[String], excludeInternalTopics: Boolean = false): Seq[String] = {
      val allTopics = zkClient.getAllTopicsInCluster.sorted
      doGetTopics(allTopics, topicWhitelist, excludeInternalTopics)
    }

    override def close(): Unit = zkClient.close()
  }

  /**
    * ensures topic existence and throws exception if topic doesn't exist
    *
    * @param opts
    * @param topics
    * @param topicOptWithExists
    */
  private def ensureTopicExists(topics: Seq[String], topicOptWithExists: Boolean = false) = {
    if (topics.isEmpty && !topicOptWithExists) {
      // If given topic doesn't exist then throw exception
      throw new IllegalArgumentException(s"Topics in [${topics.mkString(",")}] does not exist")
    }
  }

  private def printPartition(tp: PartitionDescription): Unit = {
    val markedForDeletionString =
    if (tp.markedForDeletion && !tp.describeConfigs) "\tMarkedForDeletion: true" else ""
    print("\tTopic: " + tp.topic)
    print("\tPartition: " + tp.partition)
    print("\tLeader: " + (if(tp.leader.isDefined) tp.leader.get else "none"))
    print("\tReplicas: " + tp.assignedReplicas.mkString(","))
    print("\tIsr: " + tp.isr.mkString(","))
    print(markedForDeletionString)
    println()
  }

  private def doGetTopics(allTopics: Seq[String], topicWhitelist: Option[String], excludeInternalTopics: Boolean): Seq[String] = {
    if (topicWhitelist.isDefined) {
      val topicsFilter = Whitelist(topicWhitelist.get)
      allTopics.filter(topicsFilter.isTopicAllowed(_, excludeInternalTopics))
    } else
    allTopics.filterNot(Topic.isInternal(_) && excludeInternalTopics)
  }


  def parseTopicConfigsToBeAdded(opts: TopicCommandOptions): Properties = {
    val configsToBeAdded = opts.topicConfig.getOrElse(Collections.emptyList()).asScala.map(_.split("""\s*=\s*"""))
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
    val configsToBeDeleted = opts.configsToDelete.getOrElse(Collections.emptyList()).asScala.map(_.trim())
    val propsToBeDeleted = new Properties
    configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
    LogConfig.validateNames(propsToBeDeleted)
    configsToBeDeleted
  }

  def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.LinkedHashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      val duplicateBrokers = CoreUtils.duplicates(brokerList)
      if (duplicateBrokers.nonEmpty)
        throw new AdminCommandFailedException(s"Partition replica lists may not contain duplicate entries: ${duplicateBrokers.mkString(",")}")
      ret.put(i, brokerList.toList)
      if (ret(i).size != ret(0).size)
        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret
  }

  def asJavaReplicaReassignment(original: Map[Int, List[Int]]): util.Map[Integer, util.List[Integer]] = {
    original.map(f => Integer.valueOf(f._1) -> f._2.map(e => Integer.valueOf(e)).asJava).asJava
  }

  class TopicCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    private val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to. In case of providing this, a direct Zookeeper connection won't be required.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    private val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
      "This is used only with --bootstrap-server option for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])
    private val zkConnectOpt = parser.accepts("zookeeper", "DEPRECATED, The connection string for the zookeeper connection in the form host:port. " +
      "Multiple hosts can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("hosts")
      .ofType(classOf[String])
    private val listOpt = parser.accepts("list", "List all available topics.")
    private val createOpt = parser.accepts("create", "Create a new topic.")
    private val deleteOpt = parser.accepts("delete", "Delete a topic")
    private val alterOpt = parser.accepts("alter", "Alter the number of partitions, replica assignment, and/or configuration for the topic.")
    private val describeOpt = parser.accepts("describe", "List details for the given topics.")
    private val topicOpt = parser.accepts("topic", "The topic to create, alter, describe or delete. It also accepts a regular " +
                                           "expression, except for --create option. Put topic name in double quotes and use the '\\' prefix " +
                                           "to escape regular expression symbols; e.g. \"test\\.topic\".")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    private val nl = System.getProperty("line.separator")
    private val configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered."  +
                                             "The following is a list of valid configurations: " + nl + LogConfig.configNames.map("\t" + _).mkString(nl) + nl +
                                             "See the Kafka documentation for full details on the topic configs." +
                                             "It is supported only in combination with --create if --bootstrap-server option is used.")
                           .withRequiredArg
                           .describedAs("name=value")
                           .ofType(classOf[String])
    private val deleteConfigOpt = parser.accepts("delete-config", "A topic configuration override to be removed for an existing topic (see the list of configurations under the --config option). " +
      "Not supported with the --bootstrap-server option.")
                           .withRequiredArg
                           .describedAs("name")
                           .ofType(classOf[String])
    private val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created or " +
      "altered (WARNING: If partitions are increased for a topic that has a key, the partition logic or ordering of the messages will be affected")
                           .withRequiredArg
                           .describedAs("# of partitions")
                           .ofType(classOf[java.lang.Integer])
    private val replicationFactorOpt = parser.accepts("replication-factor", "The replication factor for each partition in the topic being created.")
                           .withRequiredArg
                           .describedAs("replication factor")
                           .ofType(classOf[java.lang.Integer])
    private val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created or altered.")
                           .withRequiredArg
                           .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                                        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                           .ofType(classOf[String])
    private val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                                                            "if set when describing topics, only show under replicated partitions")
    private val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                                                            "if set when describing topics, only show partitions whose leader is not available")
    private val topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                                                "if set when describing topics, only show topics that have overridden configs")
    private val ifExistsOpt = parser.accepts("if-exists",
                                     "if set when altering or deleting or describing topics, the action will only execute if the topic exists. Not supported with the --bootstrap-server option.")
    private val ifNotExistsOpt = parser.accepts("if-not-exists",
                                        "if set when creating topics, the action will only execute if the topic does not already exist. Not supported with the --bootstrap-server option.")

    private val disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment")

    private val forceOpt = parser.accepts("force", "Suppress console prompts")

    private val excludeInternalTopicOpt = parser.accepts("exclude-internal", "exclude internal topics when running list or describe command. The internal topics will be listed by default")

    options = parser.parse(args : _*)

    private val allTopicLevelOpts: Set[OptionSpec[_]] = Set(alterOpt, createOpt, describeOpt, listOpt, deleteOpt)

    def has(builder: OptionSpec[_]): Boolean = options.has(builder)
    def valueAsOption[A](option: OptionSpec[A], defaultValue: Option[A] = None): Option[A] = if (has(option)) Some(options.valueOf(option)) else defaultValue
    def valuesAsOption[A](option: OptionSpec[A], defaultValue: Option[util.List[A]] = None): Option[util.List[A]] = if (has(option)) Some(options.valuesOf(option)) else defaultValue

    def hasCreateOption: Boolean = has(createOpt)
    def hasAlterOption: Boolean = has(alterOpt)
    def hasListOption: Boolean = has(listOpt)
    def hasDescribeOption: Boolean = has(describeOpt)
    def hasDeleteOption: Boolean = has(deleteOpt)

    def zkConnect: Option[String] = valueAsOption(zkConnectOpt)
    def bootstrapServer: Option[String] = valueAsOption(bootstrapServerOpt)
    def commandConfig: Properties = if (has(commandConfigOpt)) Utils.loadProps(options.valueOf(commandConfigOpt)) else new Properties()
    def topic: Option[String] = valueAsOption(topicOpt)
    def partitions: Option[Integer] = valueAsOption(partitionsOpt)
    def replicationFactor: Option[Integer] = valueAsOption(replicationFactorOpt)
    def replicaAssignment: Option[Map[Int, List[Int]]] =
      if (has(replicaAssignmentOpt) && !Option(options.valueOf(replicaAssignmentOpt)).getOrElse("").isEmpty)
        Some(parseReplicaAssignment(options.valueOf(replicaAssignmentOpt)))
      else
        None
    def rackAwareMode: RackAwareMode = if (has(disableRackAware)) RackAwareMode.Disabled else RackAwareMode.Enforced
    def reportUnderReplicatedPartitions: Boolean = has(reportUnderReplicatedPartitionsOpt)
    def reportUnavailablePartitions: Boolean = has(reportUnavailablePartitionsOpt)
    def reportOverriddenConfigs: Boolean = has(topicsWithOverridesOpt)
    def ifExists: Boolean = has(ifExistsOpt)
    def ifNotExists: Boolean = has(ifNotExistsOpt)
    def excludeInternalTopics: Boolean = has(excludeInternalTopicOpt)
    def topicConfig: Option[util.List[String]] = valuesAsOption(configOpt)
    def configsToDelete: Option[util.List[String]] = valuesAsOption(deleteConfigOpt)

    def checkArgs() {
      if (args.length == 0)
        CommandLineUtils.printUsageAndDie(parser, "Create, delete, describe, or change a topic.")

      CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to create, delete, describe, or change a topic.")

      // should have exactly one action
      val actions = Seq(createOpt, listOpt, alterOpt, describeOpt, deleteOpt).count(options.has)
      if (actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --list, --describe, --create, --alter or --delete")

      // check required args
      if (has(bootstrapServerOpt) == has(zkConnectOpt))
        throw new IllegalArgumentException("Only one of --bootstrap-server or --zookeeper must be specified")

      if (!has(bootstrapServerOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)
      if(has(describeOpt) && has(ifExistsOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)
      if (!has(listOpt) && !has(describeOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, topicOpt)
      if (has(createOpt) && !has(replicaAssignmentOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, partitionsOpt, replicationFactorOpt)
      if (has(bootstrapServerOpt) && has(alterOpt))
        CommandLineUtils.checkRequiredArgs(parser, options, partitionsOpt)

      // check invalid args
      CommandLineUtils.checkInvalidArgs(parser, options, configOpt, allTopicLevelOpts -- Set(alterOpt, createOpt))
      CommandLineUtils.checkInvalidArgsSet(parser, options, Set(bootstrapServerOpt, configOpt), Set(alterOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, deleteConfigOpt, allTopicLevelOpts -- Set(alterOpt) ++ Set(bootstrapServerOpt))
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
      CommandLineUtils.checkInvalidArgs(parser, options, ifExistsOpt, allTopicLevelOpts -- Set(alterOpt, deleteOpt, describeOpt) ++ Set(bootstrapServerOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, ifNotExistsOpt, allTopicLevelOpts -- Set(createOpt) ++ Set(bootstrapServerOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, excludeInternalTopicOpt, allTopicLevelOpts -- Set(listOpt, describeOpt))
    }
  }

  def askToProceed(): Unit = {
    println("Are you sure you want to continue? [y/n]")
    if (!StdIn.readLine().equalsIgnoreCase("y")) {
      println("Ending your session")
      Exit.exit(0)
    }
  }

}

