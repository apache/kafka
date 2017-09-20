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
import java.util.concurrent.{ExecutionException}

import joptsimple.OptionParser
import kafka.utils._

import collection.JavaConverters._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.utils.CommandLineUtils.printUsageAndDie
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.TimeoutException

import collection._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.security.JaasUtils

object PreferredReplicaLeaderElectionCommand extends Logging {

  def main(args: Array[String]): Unit = {
    try {
      run(args)
    } catch {
      case e: Throwable =>
        println("Failed to start preferred replica election")
        println(Utils.stackTrace(e))
        System.exit(1);
    }
  }
  /** Basically the same as main, but throws rather than calling System.exit */
  def run(args: Array[String], timeout: Option[Int] = None): Unit = {
    val parser = new OptionParser(false)
    val jsonFileOpt = parser.accepts("path-to-json-file", "The JSON file with the list of partitions " +
      "for which preferred replica leader election should be done, in the following format - \n" +
       "{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\n" +
      "Defaults to all existing partitions")
      .withRequiredArg
      .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
      .ofType(classOf[String])
    val zkConnectOpt = parser.accepts("zookeeper", "DEPRECATED. The connection string for the zookeeper connection in the " +
      "form host:port. Multiple comma-separated URLS can be given to allow fail-over. REQUIRED unless --bootstrap-server is given.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "A hostname and port for the broker to connect to, " +
      "in the form host:port. Multiple comma-separated URLs can be given. REQUIRED unless --zookeeper is given.")
      .withRequiredArg
      .describedAs("host:port")
      .ofType(classOf[String])
      
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This tool causes leadership for each partition to be transferred back to the 'preferred replica'," + 
                                                " it can be used to balance leadership among the servers." +
                                                " The preferred replica is the first one in the replica assignment (see kafka-reassign-partitions.sh)." +
                                                " Using this command is not necessary when the broker is configured with \"auto.leader.rebalance.enable=true\".")

    val options = parser.parse(args : _*)

    if (options.has(bootstrapServerOpt) && options.has(zkConnectOpt)
      || !options.has(bootstrapServerOpt) && !options.has(zkConnectOpt)) {
      printUsageAndDie(parser, "Exactly one of \"" + bootstrapServerOpt+ "\" or \"" + zkConnectOpt + "\" must be provided")
    }

    val partitionsForPreferredReplicaElection =
      if (!options.has(jsonFileOpt))
        None
      else
        Some(parsePreferredReplicaElectionData(Utils.readFileAsString(options.valueOf(jsonFileOpt))))

    val preferredReplicaElectionCommand = if (options.has(zkConnectOpt)) {
      Console.err.println(zkConnectOpt + " is deprecated and will be removed in a future version of Kafka.")
      Console.err.println("Use " + bootstrapServerOpt + " instead to specify a broker to connect to.")
      new ZkPreferredReplicaLeaderElectionCommand(ZkUtils(options.valueOf(zkConnectOpt),
        timeout.getOrElse(30000),
        timeout.getOrElse(30000),
        JaasUtils.isZkSecurityEnabled()))
    } else {
      new AdminClientPreferredReplicaLeaderElectionCommand(options.valueOf(bootstrapServerOpt), timeout)
    }

    try {
      preferredReplicaElectionCommand.moveLeaderToPreferredReplica(partitionsForPreferredReplicaElection)
    } finally {
      preferredReplicaElectionCommand.close()
    }
  }

  def parsePreferredReplicaElectionData(jsonString: String): immutable.Set[TopicAndPartition] = {
    val partitionsUndergoingPreferredReplicaElection =
      ZkUtils.parsePreferredReplicaElectionDataWithoutDedup(jsonString)
    if (partitionsUndergoingPreferredReplicaElection.isEmpty) {
      throw new AdminOperationException("Preferred replica election data is empty")
    }
    val duplicatePartitions = CoreUtils.duplicates(partitionsUndergoingPreferredReplicaElection)
    if (duplicatePartitions.nonEmpty)
      throw new AdminOperationException("Preferred replica election data contains duplicate partitions: %s".format(duplicatePartitions.mkString(",")))
    return partitionsUndergoingPreferredReplicaElection.toSet
  }

  def writePreferredReplicaElectionData(zkUtils: ZkUtils,
                                        partitionsUndergoingPreferredReplicaElection: scala.collection.Set[TopicAndPartition]) {
    val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath
    val jsonData = ZkUtils.preferredReplicaLeaderElectionZkData(partitionsUndergoingPreferredReplicaElection)
    try {
      zkUtils.createPersistentPath(zkPath, jsonData)
      println("Created preferred replica election path with %s".format(jsonData))
    } catch {
      case _: ZkNodeExistsException =>
        val partitionsUndergoingPreferredReplicaElection =
          PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(zkUtils.readData(zkPath)._1)
        throw new AdminOperationException("Preferred replica leader election currently in progress for " +
          "%s. Aborting operation".format(partitionsUndergoingPreferredReplicaElection))
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
}

trait PreferredReplicaLeaderElectionCommand {
  /**
    * Elect the preferred leader for the given {@code partitionsFromUser}.
    * If the given {@code partitionsFromUser} are None then elect the preferred leader for all partitions.
    */
  def moveLeaderToPreferredReplica(partitionsFromUser: Option[scala.collection.Set[TopicAndPartition]]) : Unit
  def close() : Unit
}

class AdminClientPreferredReplicaLeaderElectionCommand(bootstrapServers: String, timeout: Option[Int]) extends PreferredReplicaLeaderElectionCommand with Logging {
  val props = new Properties()
  props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  timeout match {
    case Some(timeout) =>
      props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout.toString)
    case None =>
  }

  val adminClient: org.apache.kafka.clients.admin.AdminClient = org.apache.kafka.clients.admin.AdminClient.create(props)

  /**
    * Wait until the given future has completed, then return whether it completed exceptionally.
    * Because KafkaFuture.isCompletedExceptionally doesn't wait for a result
    */
  private def completedExceptionally[T](future: KafkaFuture[T]): Boolean = {
    try {
      future.get()
      false
    } catch {
      case (_: Throwable) =>
        true
    }
  }

  override def moveLeaderToPreferredReplica(partitionsFromUser: Option[Set[TopicAndPartition]]): Unit = {
    val partitions = partitionsFromUser match {
      case Some(partitionsFromUser) => partitionsFromUser.map(tp => tp.asTopicPartition).toSet.asJava
      case None => null
    }
    debug(s"Calling AdminClient.electPreferredLeaders($partitions)")
    val result = adminClient.electPreferredLeaders(partitions)
    // wait for all results
    val attemptedPartitions = try {
      result.partitions().get.asScala
    } catch {
      case e: ExecutionException =>
        val cause = e.getCause
        if (cause.isInstanceOf[TimeoutException]) {
          // We timed out, or don't even know the attempted partitions
          println("Timeout waiting for election results")
          return
        }
        throw cause
      case e: Throwable =>
        // We don't even know the attempted partitions
        println("Error while making request")
        e.printStackTrace()
        return
    }
    val (exceptional, ok) = attemptedPartitions.map(tp => tp -> result.partitionResult(tp)).
      partition{case (_, partitionResult) => completedExceptionally(partitionResult)}
    if (!ok.isEmpty) {
      println("Successfully completed preferred replica election for partitions %s".format(ok.map(_._1).mkString(", ")))
    }
    if (!exceptional.isEmpty) {
      val adminException = new AdminCommandFailedException(s"${exceptional.size} preferred replica(s) could not be elected")
      for ((partition, void) <- exceptional) {
        val exception = try {
          void.get()
          new AdminCommandFailedException("Exceptional future with no exception")
        } catch {
          case e: ExecutionException => e.getCause
        }
        println(s"Error completing preferred replica election for partition $partition: $exception")
        adminException.addSuppressed(exception)
      }
      throw adminException
    }
  }

  override def close(): Unit = {
    debug("Closing AdminClient")
    adminClient.close()
  }
}

class ZkPreferredReplicaLeaderElectionCommand(zkUtils: ZkUtils) extends PreferredReplicaLeaderElectionCommand {

  override def moveLeaderToPreferredReplica(partitionsFromUser: Option[scala.collection.Set[TopicAndPartition]]) = {
    try {
      val partitions = partitionsFromUser match {
        case Some(partitionsFromUser) => partitionsFromUser
        case None => zkUtils.getAllPartitions()
      }
      val topics = partitions.map(_.topic).toSet
      val partitionsFromZk = zkUtils.getPartitionsForTopics(topics.toSeq).flatMap{ case (topic, partitions) =>
        partitions.map(TopicAndPartition(topic, _))
      }.toSet

      val (validPartitions, invalidPartitions) = partitions.partition(partitionsFromZk.contains)
      PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkUtils, validPartitions)

      println("Successfully started preferred replica election for partitions %s".format(validPartitions))
      invalidPartitions.foreach(p => println("Skipping preferred replica leader election for partition %s since it doesn't exist.".format(p)))
    } catch {
      case e: Throwable => throw new AdminCommandFailedException("Admin command failed", e)
    }
  }

  override def close(): Unit = {
    zkUtils.close()
  }
}
