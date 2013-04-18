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
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.common.{TopicAndPartition, AdminCommandFailedException}
import collection._
import mutable.ListBuffer

object PreferredReplicaLeaderElectionCommand extends Logging {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
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

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt)

    val zkConnect = options.valueOf(zkConnectOpt)
    var zkClient: ZkClient = null

    try {
      zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer)
      val partitionsForPreferredReplicaElection =
        if (!options.has(jsonFileOpt))
          ZkUtils.getAllPartitions(zkClient)
        else
          parsePreferredReplicaJsonData(Utils.readFileAsString(options.valueOf(jsonFileOpt)))
      val preferredReplicaElectionCommand = new PreferredReplicaLeaderElectionCommand(zkClient, partitionsForPreferredReplicaElection)

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

  def parsePreferredReplicaJsonData(jsonString: String): immutable.Set[TopicAndPartition] = {
    Json.parseFull(jsonString) match {
      case Some(m) =>
        m.asInstanceOf[Map[String, Any]].get("partitions") match {
          case Some(partitionsList) =>
            val partitions = partitionsList.asInstanceOf[List[Map[String, Any]]]
            partitions.map { p =>
              val topic = p.get("topic").get.asInstanceOf[String]
              val partition = p.get("partition").get.asInstanceOf[Int]
              TopicAndPartition(topic, partition)
            }.toSet
          case None => throw new AdminOperationException("Preferred replica election data is empty")
        }
      case None => throw new AdminOperationException("Preferred replica election data is empty")
    }
  }

  def writePreferredReplicaElectionData(zkClient: ZkClient,
                                        partitionsUndergoingPreferredReplicaElection: scala.collection.Set[TopicAndPartition]) {
    val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath
    var partitionsData: mutable.ListBuffer[String] = ListBuffer[String]()
    for (p <- partitionsUndergoingPreferredReplicaElection) {
      partitionsData += Utils.mergeJsonFields(Utils.mapToJsonFields(Map("topic" -> p.topic), valueInQuotes = true) ++
                                               Utils.mapToJsonFields(Map("partition" -> p.partition.toString), valueInQuotes = false))
    }
    val jsonPartitionsData = Utils.seqToJson(partitionsData, valueInQuotes = false)
    val jsonData = Utils.mapToJson(Map("version" -> 1.toString, "partitions" -> jsonPartitionsData), valueInQuotes = false)
    try {
      ZkUtils.createPersistentPath(zkClient, zkPath, jsonData)
      info("Created preferred replica election path with %s".format(jsonData))
    } catch {
      case nee: ZkNodeExistsException =>
        val partitionsUndergoingPreferredReplicaElection =
          PreferredReplicaLeaderElectionCommand.parsePreferredReplicaJsonData(ZkUtils.readData(zkClient, zkPath)._1)
        throw new AdminOperationException("Preferred replica leader election currently in progress for " +
          "%s. Aborting operation".format(partitionsUndergoingPreferredReplicaElection))
      case e2 => throw new AdminOperationException(e2.toString)
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
