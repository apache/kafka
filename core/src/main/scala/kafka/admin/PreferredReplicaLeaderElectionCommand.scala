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
import kafka.common.AdminCommandFailedException
import kafka.zk.KafkaZkClient
import kafka.zookeeper.ZooKeeperClient

import collection._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.TopicPartition
import org.apache.zookeeper.KeeperException.NodeExistsException

object PreferredReplicaLeaderElectionCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val jsonFileOpt = parser.accepts("path-to-json-file", "The JSON file with the list of partitions " +
      "for which preferred replica leader election should be done, in the following format - \n" +
       "{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\n" +
      "Defaults to all existing partitions")
      .withRequiredArg
      .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
      
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This tool causes leadership for each partition to be transferred back to the 'preferred replica'," + 
                                                " it can be used to balance leadership among the servers.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)

    val zkConnect = options.valueOf(zkConnectOpt)
    var zooKeeperClient: ZooKeeperClient = null
    var zkClient: KafkaZkClient = null
    try {
      zooKeeperClient = new ZooKeeperClient(zkConnect, 30000, 30000, Int.MaxValue)
      zkClient = new KafkaZkClient(zooKeeperClient, JaasUtils.isZkSecurityEnabled())

      val partitionsForPreferredReplicaElection =
        if (!options.has(jsonFileOpt))
          zkClient.getAllPartitions()
        else
          parsePreferredReplicaElectionData(Utils.readFileAsString(options.valueOf(jsonFileOpt)))
      val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkClient, partitionsForPreferredReplicaElection)

      preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
    } catch {
      case e: Throwable =>
        println("Failed to start preferred replica election")
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def parsePreferredReplicaElectionData(jsonString: String): immutable.Set[TopicPartition] = {
    Json.parseFull(jsonString) match {
      case Some(js) =>
        js.asJsonObject.get("partitions") match {
          case Some(partitionsList) =>
            val partitionsRaw = partitionsList.asJsonArray.iterator.map(_.asJsonObject)
            val partitions = partitionsRaw.map { p =>
              val topic = p("topic").to[String]
              val partition = p("partition").to[Int]
              new TopicPartition(topic, partition)
            }.toBuffer
            val duplicatePartitions = CoreUtils.duplicates(partitions)
            if (duplicatePartitions.nonEmpty)
              throw new AdminOperationException("Preferred replica election data contains duplicate partitions: %s".format(duplicatePartitions.mkString(",")))
            partitions.toSet
          case None => throw new AdminOperationException("Preferred replica election data is empty")
        }
      case None => throw new AdminOperationException("Preferred replica election data is empty")
    }
  }

  def writePreferredReplicaElectionData(zkClient: KafkaZkClient,
                                        partitionsUndergoingPreferredReplicaElection: Set[TopicPartition]) {
    try {
      zkClient.createPreferredReplicaElection(partitionsUndergoingPreferredReplicaElection.toSet)
      println("Created preferred replica election path with %s".format(partitionsUndergoingPreferredReplicaElection.mkString(",")))
    } catch {
      case _: NodeExistsException =>
        throw new AdminOperationException("Preferred replica leader election currently in progress for " +
          "%s. Aborting operation".format(zkClient.getPreferredReplicaElection.mkString(",")))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
}

class PreferredReplicaLeaderElectionCommand(zkClient: KafkaZkClient, partitionsFromUser: scala.collection.Set[TopicPartition]) {
  def moveLeaderToPreferredReplica() = {
    try {
      val topics = partitionsFromUser.map(_.topic).toSet
      val partitionsFromZk = zkClient.getPartitionsForTopics(topics).flatMap{ case (topic, partitions) =>
        partitions.map(new TopicPartition(topic, _))
      }.toSet

      val (validPartitions, invalidPartitions) = partitionsFromUser.partition(partitionsFromZk.contains)
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, validPartitions)

      println("Successfully started preferred replica election for partitions %s".format(validPartitions))
      invalidPartitions.foreach(p => println("Skipping preferred replica leader election for partition %s since it doesn't exist.".format(p)))
    } catch {
      case e: Throwable => throw new AdminCommandFailedException("Admin command failed", e)
    }
  }
}
