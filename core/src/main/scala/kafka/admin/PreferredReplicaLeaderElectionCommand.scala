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
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNodeExistsException}
import kafka.common.{TopicAndPartition, AdminCommandFailedException}

object PreferredReplicaLeaderElectionCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val jsonFileOpt = parser.accepts("path to json file", "The JSON file with the list of partitions " +
      "for which preferred replica leader election should be done, in the following format - \n" +
       "[{\"topic\": \"foo\", \"partition\": \"1\"}, {\"topic\": \"foobar\", \"partition\": \"2\"}]. \n" +
      "Defaults to all existing partitions")
      .withRequiredArg
      .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
      .ofType(classOf[String])
      .defaultsTo("")
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, jsonFileOpt, zkConnectOpt)

    val jsonFile = options.valueOf(jsonFileOpt)
    val zkConnect = options.valueOf(zkConnectOpt)
    val jsonString = Utils.readFileAsString(jsonFile)
    var zkClient: ZkClient = null

    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      val partitionsForPreferredReplicaElection =
        if(jsonFile == "") ZkUtils.getAllPartitions(zkClient) else parsePreferredReplicaJsonData(jsonString)
      val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkClient, partitionsForPreferredReplicaElection)

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
          // delete the admin path so it can be retried
          ZkUtils.deletePathRecursive(zkClient, ZkUtils.PreferredReplicaLeaderElectionPath)
          zkClient.close()
        }
      })

      preferredReplicaElectionCommand.moveLeaderToPreferredReplica()
      println("Successfully started preferred replica election for partitions %s".format(partitionsForPreferredReplicaElection))
    } catch {
      case e =>
        println("Failed to start preferred replica election")
        println(Utils.stackTrace(e))
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  def parsePreferredReplicaJsonData(jsonString: String): Set[TopicAndPartition] = {
    Json.parseFull(jsonString) match {
      case Some(partitionList) =>
        val partitions = (partitionList.asInstanceOf[List[Any]])
        Set.empty[TopicAndPartition] ++ partitions.map { m =>
          val topic = m.asInstanceOf[Map[String, String]].get("topic").get
          val partition = m.asInstanceOf[Map[String, String]].get("partition").get.toInt
          TopicAndPartition(topic, partition)
        }
      case None => throw new AdministrationException("Preferred replica election data is empty")
    }
  }

  def writePreferredReplicaElectionData(zkClient: ZkClient,
                                        partitionsUndergoingPreferredReplicaElection: scala.collection.Set[TopicAndPartition]) {
    val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath
    val jsonData = Utils.arrayToJson(partitionsUndergoingPreferredReplicaElection.map { p =>
      Utils.stringMapToJson(Map(("topic" -> p.topic), ("partition" -> p.partition.toString)))
    }.toArray)
    try {
      ZkUtils.createPersistentPath(zkClient, zkPath, jsonData)
      info("Created preferred replica election path with %s".format(jsonData))
    } catch {
      case nee: ZkNodeExistsException =>
        val partitionsUndergoingPreferredReplicaElection =
          PreferredReplicaLeaderElectionCommand.parsePreferredReplicaJsonData(ZkUtils.readData(zkClient, zkPath)._1)
        throw new AdministrationException("Preferred replica leader election currently in progress for " +
          "%s. Aborting operation".format(partitionsUndergoingPreferredReplicaElection))
      case e2 => throw new AdministrationException(e2.toString)
    }
  }
}

class PreferredReplicaLeaderElectionCommand(zkClient: ZkClient, partitions: scala.collection.Set[TopicAndPartition])
  extends Logging {
  def moveLeaderToPreferredReplica() = {
    try {
      val validPartitions = partitions.filter(p => validatePartition(zkClient, p.topic, p.partition))
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, validPartitions)
    } catch {
      case e => throw new AdminCommandFailedException("Admin command failed", e)
    }
  }

  def validatePartition(zkClient: ZkClient, topic: String, partition: Int): Boolean = {
    // check if partition exists
    val partitionsOpt = ZkUtils.getPartitionsForTopics(zkClient, List(topic)).get(topic)
    partitionsOpt match {
      case Some(partitions) =>
        if(partitions.contains(partition)) {
          true
        } else {
          error("Skipping preferred replica leader election for partition [%s,%d] ".format(topic, partition) +
            "since it doesn't exist")
          false
        }
      case None => error("Skipping preferred replica leader election for partition " +
        "[%s,%d] since topic %s doesn't exist".format(topic, partition, topic))
        false
    }
  }
}
