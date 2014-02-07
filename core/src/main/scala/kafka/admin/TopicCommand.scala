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

import joptsimple._
import java.util.Properties
import kafka.utils._
import org.I0Itec.zkclient.ZkClient
import scala.collection._
import scala.collection.JavaConversions._
import kafka.cluster.Broker
import kafka.log.LogConfig
import kafka.consumer.Whitelist

object TopicCommand {

  def main(args: Array[String]): Unit = {
    
    val opts = new TopicCommandOptions(args)
    
    // should have exactly one action
    val actions = Seq(opts.createOpt, opts.deleteOpt, opts.listOpt, opts.alterOpt, opts.describeOpt).count(opts.options.has _)
    if(actions != 1) {
      System.err.println("Command must include exactly one action: --list, --describe, --create, --delete, or --alter")
      opts.parser.printHelpOn(System.err)
      System.exit(1)
    }
      
    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)
    if (!opts.options.has(opts.listOpt)) CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt)
    
    val zkClient = new ZkClient(opts.options.valueOf(opts.zkConnectOpt), 30000, 30000, ZKStringSerializer)

    try {
      if(opts.options.has(opts.createOpt))
        createTopic(zkClient, opts)
      else if(opts.options.has(opts.alterOpt))
        alterTopic(zkClient, opts)
      else if(opts.options.has(opts.deleteOpt))
        deleteTopic(zkClient, opts)
      else if(opts.options.has(opts.listOpt))
        listTopics(zkClient, opts)
      else if(opts.options.has(opts.describeOpt))
        describeTopic(zkClient, opts)
    } catch {
      case e =>
        println("Error while executing topic command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      zkClient.close()
    }
  }

  private def getTopics(zkClient: ZkClient, opts: TopicCommandOptions): Seq[String] = {
    val topicsSpec = opts.options.valueOf(opts.topicOpt)
    val topicsFilter = new Whitelist(topicsSpec)
    val allTopics = ZkUtils.getAllTopics(zkClient)
    allTopics.filter(topicsFilter.isTopicAllowed).sorted
  }

  def createTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    if (opts.options.has(opts.replicaAssignmentOpt)) {
      val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, assignment, configs)
    } else {
      CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
      val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
      val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
      AdminUtils.createTopic(zkClient, topic, partitions, replicas, configs)
    }
    println("Created topic \"%s\".".format(topic))
  }

  def alterTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    topics.foreach { topic =>
      if(opts.options.has(opts.configOpt) || opts.options.has(opts.deleteConfigOpt)) {
        val configsToBeAdded = parseTopicConfigsToBeAdded(opts)
        val configsToBeDeleted = parseTopicConfigsToBeDeleted(opts)
        // compile the final set of configs
        val configs = AdminUtils.fetchTopicConfig(zkClient, topic)
        configs.putAll(configsToBeAdded)
        configsToBeDeleted.foreach(config => configs.remove(config))
        AdminUtils.changeTopicConfig(zkClient, topic, configs)
        println("Updated config for topic \"%s\".".format(topic))
      }
      if(opts.options.has(opts.partitionsOpt)) {
        println("WARNING: If partitions are increased for a topic that has a key, the partition " +
          "logic or ordering of the messages will be affected")
        val nPartitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val replicaAssignmentStr = opts.options.valueOf(opts.replicaAssignmentOpt)
        AdminUtils.addPartitions(zkClient, topic, nPartitions, replicaAssignmentStr)
        println("adding partitions succeeded!")
      }
      if(opts.options.has(opts.replicationFactorOpt))
        Utils.croak("Changing the replication factor is not supported.")
    }
  }
  
  def deleteTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    topics.foreach { topic =>
      AdminUtils.deleteTopic(zkClient, topic)
      println("Topic \"%s\" queued for deletion.".format(topic))
    }
  }
  
  def listTopics(zkClient: ZkClient, opts: TopicCommandOptions) {
    if(opts.options.has(opts.topicsWithOverridesOpt)) {
      ZkUtils.getAllTopics(zkClient).sorted.foreach { topic =>
        val configs = AdminUtils.fetchTopicConfig(zkClient, topic)
        if(configs.size() != 0) {
          val replicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
          val numPartitions = replicaAssignment.size
          val replicationFactor = replicaAssignment.head._2.size
          println("\nTopic:%s\tPartitionCount:%d\tReplicationFactor:%d\tConfigs:%s".format(topic, numPartitions,
                   replicationFactor, configs.map(kv => kv._1 + "=" + kv._2).mkString(",")))
        }
      }
    } else {
      for(topic <- ZkUtils.getAllTopics(zkClient).sorted)
        println(topic)
    }
  }
  
  def describeTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    val topics = getTopics(zkClient, opts)
    val reportUnderReplicatedPartitions = if (opts.options.has(opts.reportUnderReplicatedPartitionsOpt)) true else false
    val reportUnavailablePartitions = if (opts.options.has(opts.reportUnavailablePartitionsOpt)) true else false
    val liveBrokers = ZkUtils.getAllBrokersInCluster(zkClient).map(_.id).toSet
    for (topic <- topics) {
      ZkUtils.getPartitionAssignmentForTopics(zkClient, List(topic)).get(topic) match {
        case Some(topicPartitionAssignment) =>
          val sortedPartitions = topicPartitionAssignment.toList.sortWith((m1, m2) => m1._1 < m2._1)
          if (!reportUnavailablePartitions && !reportUnderReplicatedPartitions) {
            println(topic)
            val config = AdminUtils.fetchTopicConfig(zkClient, topic)
            println("\tconfigs: " + config.map(kv => kv._1 + " = " + kv._2).mkString(", "))
            println("\tpartitions: " + sortedPartitions.size)
          }
          for ((partitionId, assignedReplicas) <- sortedPartitions) {
            val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionId)
            val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionId)
            if ((!reportUnderReplicatedPartitions && !reportUnavailablePartitions) ||
                (reportUnderReplicatedPartitions && inSyncReplicas.size < assignedReplicas.size) ||
                (reportUnavailablePartitions && (!leader.isDefined || !liveBrokers.contains(leader.get)))) {
              print("\t\ttopic: " + topic)
              print("\tpartition: " + partitionId)
              print("\tleader: " + (if(leader.isDefined) leader.get else "none"))
              print("\treplicas: " + assignedReplicas.mkString(","))
              println("\tisr: " + inSyncReplicas.mkString(","))
            }
          }
        case None =>
          println("topic " + topic + " doesn't exist!")
      }
    }
  }
  
  def formatBroker(broker: Broker) = broker.id + " (" + broker.host + ":" + broker.port + ")"
  
  def parseTopicConfigsToBeAdded(opts: TopicCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.configOpt).map(_.split("""\s*=\s*"""))
    require(configsToBeAdded.forall(config => config.length == 2),
      "Invalid topic config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    LogConfig.validate(props)
    props
  }

  def parseTopicConfigsToBeDeleted(opts: TopicCommandOptions): Seq[String] = {
    val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfigOpt).map(_.split("""\s*=\s*"""))
    if(opts.options.has(opts.createOpt))
      require(configsToBeDeleted.size == 0, "Invalid topic config: all configs on create topic must be in the format \"key=val\".")
    require(configsToBeDeleted.forall(config => config.length == 1),
      "Invalid topic config: all configs to be deleted must be in the format \"key\".")
    val propsToBeDeleted = new Properties
    configsToBeDeleted.foreach(pair => propsToBeDeleted.setProperty(pair(0).trim, ""))
    LogConfig.validateNames(propsToBeDeleted)
    configsToBeDeleted.map(pair => pair(0))
  }

  def parseReplicaAssignment(replicaAssignmentList: String): Map[Int, List[Int]] = {
    val partitionList = replicaAssignmentList.split(",")
    val ret = new mutable.HashMap[Int, List[Int]]()
    for (i <- 0 until partitionList.size) {
      val brokerList = partitionList(i).split(":").map(s => s.trim().toInt)
      ret.put(i, brokerList.toList)
      if (ret(i).size != ret(0).size)
        throw new AdminOperationException("Partition " + i + " has different replication factor: " + brokerList)
    }
    ret.toMap
  }
  
  class TopicCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
                                      "Multiple URLS can be given to allow fail-over.")
                           .withRequiredArg
                           .describedAs("urls")
                           .ofType(classOf[String])
    val listOpt = parser.accepts("list", "List all available topics.")
    val createOpt = parser.accepts("create", "Create a new topic.")
    val alterOpt = parser.accepts("alter", "Alter the configuration for the topic.")
    val deleteOpt = parser.accepts("delete", "Delete the topic.")
    val describeOpt = parser.accepts("describe", "List details for the given topics.")
    val helpOpt = parser.accepts("help", "Print usage information.")
    val topicOpt = parser.accepts("topic", "The topic to be create, alter, delete, or describe. Can also accept a regular " +
                                           "expression except for --create option")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val configOpt = parser.accepts("config", "A topic configuration override for the topic being created or altered.")
                          .withRequiredArg
                          .describedAs("name=value")
                          .ofType(classOf[String])
    val deleteConfigOpt = parser.accepts("deleteConfig", "A topic configuration override to be removed for an existing topic")
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
    val replicaAssignmentOpt = parser.accepts("replica-assignment", "A list of manual partition-to-broker assignments for the topic being created.")
                           .withRequiredArg
                           .describedAs("broker_id_for_part1_replica1 : broker_id_for_part1_replica2 , " +
                                        "broker_id_for_part2_replica1 : broker_id_for_part2_replica2 , ...")
                           .ofType(classOf[String])
    val reportUnderReplicatedPartitionsOpt = parser.accepts("under-replicated-partitions",
                                                            "if set when describing topics, only show under replicated partitions")
    val reportUnavailablePartitionsOpt = parser.accepts("unavailable-partitions",
                                                            "if set when describing topics, only show partitions whose leader is not available")
    val topicsWithOverridesOpt = parser.accepts("topics-with-overrides",
                                                "if set when listing topics, only show topics that have overridden configs")

    val options = parser.parse(args : _*)
  }
  
}
