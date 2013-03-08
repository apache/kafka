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
import kafka.common.Topic
import kafka.cluster.Broker

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
    
    val zkClient = new ZkClient(opts.options.valueOf(opts.zkConnectOpt), 30000, 30000, ZKStringSerializer)
    
    if(opts.options.has(opts.createOpt))
      createTopic(zkClient, opts)
    else if(opts.options.has(opts.alterOpt))
      alterTopic(zkClient, opts)
    else if(opts.options.has(opts.deleteOpt))
      deleteTopic(zkClient, opts)
    else if(opts.options.has(opts.listOpt))
      listTopics(zkClient)
    else if(opts.options.has(opts.describeOpt))
      describeTopic(zkClient, opts)

    zkClient.close()
  }

  def createTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt)
    val topics = opts.options.valuesOf(opts.topicOpt)
    val configs = parseTopicConfigs(opts)
    for (topic <- topics) {
      if (opts.options.has(opts.replicaAssignmentOpt)) {
        val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
        AdminUtils.createTopicWithAssignment(zkClient, topic, assignment, configs)
      } else {
        CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
        val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
        val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
        AdminUtils.createTopic(zkClient, topic, partitions, replicas, configs)
      }
      println("Created topic \"%s\".".format(topic))
    }
  }
  
  def alterTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt)
    val topics = opts.options.valuesOf(opts.topicOpt)
    val configs = parseTopicConfigs(opts)
    if(opts.options.has(opts.partitionsOpt))
      Utils.croak("Changing the number of partitions is not supported.")
    if(opts.options.has(opts.replicationFactorOpt))
      Utils.croak("Changing the replication factor is not supported.")
    for(topic <- topics) {
      AdminUtils.changeTopicConfig(zkClient, topic, configs)
      println("Updated config for topic \"%s\".".format(topic))
    }
  }
  
  def deleteTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt)
    for(topic <- opts.options.valuesOf(opts.topicOpt)) {
      AdminUtils.deleteTopic(zkClient, topic)
      println("Topic \"%s\" deleted.".format(topic))
    }
  }
  
  def listTopics(zkClient: ZkClient) {
    for(topic <- ZkUtils.getAllTopics(zkClient).sorted)
      println(topic)
  }
  
  def describeTopic(zkClient: ZkClient, opts: TopicCommandOptions) {
    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.topicOpt)
    val topics = opts.options.valuesOf(opts.topicOpt)
    val metadata = AdminUtils.fetchTopicMetadataFromZk(topics.toSet, zkClient)
    for(md <- metadata) {
      println(md.topic)
      val config = AdminUtils.fetchTopicConfig(zkClient, md.topic)
      println("\tconfigs: " + config.map(kv => kv._1 + " = " + kv._2).mkString(", "))
      println("\tpartitions: " + md.partitionsMetadata.size)
      for(pd <- md.partitionsMetadata) {
        println("\t\tpartition " + pd.partitionId)
        println("\t\tleader: " + (if(pd.leader.isDefined) formatBroker(pd.leader.get) else "none"))
        println("\t\treplicas: " + pd.replicas.map(formatBroker).mkString(", "))
        println("\t\tisr: " + pd.isr.map(formatBroker).mkString(", "))
      }
    }
  }
  
  def formatBroker(broker: Broker) = broker.id + " (" + broker.host + ":" + broker.port + ")"
  
  def parseTopicConfigs(opts: TopicCommandOptions): Properties = {
    val configs = opts.options.valuesOf(opts.configOpt).map(_.split("\\s*=\\s*"))
    require(configs.forall(_.length == 2), "Invalid topic config: all configs must be in the format \"key=val\".")
    val props = new Properties
    configs.foreach(pair => props.setProperty(pair(0), pair(1)))
    props
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
    val topicOpt = parser.accepts("topic", "The topic to be create, alter, delete, or describe.")
                         .withRequiredArg
                         .describedAs("topic")
                         .ofType(classOf[String])
    val configOpt = parser.accepts("config", "A topic configuration for the topic being created or altered.")
                          .withRequiredArg
                          .describedAs("name=value")
                          .ofType(classOf[String])
    val partitionsOpt = parser.accepts("partitions", "The number of partitions for the topic being created.")
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
    

    val options = parser.parse(args : _*)
  }
  
}
