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
      
    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "This tool causes leadership for each partition to be transferred back to the 'preferred replica'," + 
                                                " it can be used to balance leadership among the servers." +
                                                " The preferred replica is the first one in the replica assignment (see kafka-reassign-partitions.sh)." +
                                                " Using this command is not necessary when the broker is configured with \"auto.leader.rebalance.enable=true\".")

    val options = parser.parse(args : _*)

    if (options.has(bootstrapServerOpt) == options.has(zkConnectOpt)) {
      printUsageAndDie(parser, s"Exactly one of '${bootstrapServerOpt}' or '${zkConnectOpt}' must be provided")
    }

    val partitionsForPreferredReplicaElection =
      if (options.has(jsonFileOpt))
        Some(parsePreferredReplicaElectionData(Utils.readFileAsString(options.valueOf(jsonFileOpt))))
      else
        None

    val preferredReplicaElectionCommand = if (options.has(zkConnectOpt)) {
      Console.err.println(s"$zkConnectOpt is deprecated and will be removed in a future version of Kafka.")
      Console.err.println(s"Use $bootstrapServerOpt instead to specify a broker to connect to.")
      new ZkPreferredReplicaLeaderElectionCommand(ZkUtils(options.valueOf(zkConnectOpt),
        timeout.getOrElse(30000),
        timeout.getOrElse(30000),
        JaasUtils.isZkSecurityEnabled()))
    } else {
      new AdminClientPreferredReplicaLeaderElectionCommand(options.valueOf(bootstrapServerOpt), timeout)
    }

    try {
      preferredReplicaElectionCommand.electPreferredLeaders(partitionsForPreferredReplicaElection)
    } finally {
      preferredReplicaElectionCommand.close()
    }
  }

  def parsePreferredReplicaElectionData(jsonString: String): Set[TopicAndPartition] = {
    val partitionsForElection =
      ZkUtils.parsePreferredReplicaElectionDataWithoutDedup(jsonString)
    if (partitionsForElection.isEmpty) {
      throw new AdminOperationException("Preferred replica election data is empty")
    }
    val duplicatePartitions = CoreUtils.duplicates(partitionsForElection)
    if (duplicatePartitions.nonEmpty)
      throw new AdminOperationException(
        s"Preferred replica election data contains duplicate partitions: ${duplicatePartitions.mkString(",")}")
    return partitionsForElection.toSet
  }

  def writePreferredReplicaElectionData(zkUtils: ZkUtils,
                                        partitionsForElection: Set[TopicAndPartition]) {
    val zkPath = ZkUtils.PreferredReplicaLeaderElectionPath
    val jsonData = ZkUtils.preferredReplicaLeaderElectionZkData(partitionsForElection)
    try {
      zkUtils.createPersistentPath(zkPath, jsonData)
      println(s"Created preferred replica election path with $jsonData")
    } catch {
      case _: ZkNodeExistsException =>
        val partitionsUndergoingPreferredReplicaElection =
          PreferredReplicaLeaderElectionCommand.parsePreferredReplicaElectionData(zkUtils.readData(zkPath)._1)
        throw new AdminOperationException("Preferred replica leader election currently in progress for " +
          s"$partitionsUndergoingPreferredReplicaElection. Aborting operation")
      case e2: Throwable => throw new AdminOperationException(e2.toString)
    }
  }
}

/** Abstraction over different ways to perform a leader election */
trait PreferredReplicaLeaderElectionCommand {
  /** Elect the preferred leader for the given {@code partitionsForElection}.
    * If the given {@code partitionsForElection} are None then elect the preferred leader for all partitions.
    */
  def electPreferredLeaders(partitionsForElection: Option[Set[TopicAndPartition]]) : Unit
  def close() : Unit
}

/** Election via AdminClient.electPreferredLeaders() */
class AdminClientPreferredReplicaLeaderElectionCommand(bootstrapServers: String, timeout: Option[Int])
  extends PreferredReplicaLeaderElectionCommand with Logging {
  val props = new Properties()
  props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  timeout match {
    case Some(timeout) =>
      props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout.toString)
    case None =>
  }

  val adminClient = org.apache.kafka.clients.admin.AdminClient.create(props)

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

  override def electPreferredLeaders(partitionsFromUser: Option[Set[TopicAndPartition]]): Unit = {
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
      println(s"Successfully completed preferred replica election for partitions ${ok.map(_._1).mkString(", ")}")
    }
    if (!exceptional.isEmpty) {
      val adminException = new AdminCommandFailedException(
        s"${exceptional.size} preferred replica(s) could not be elected")
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

/** Election via ZooKeeper
  * @deprecated since Kafka 1.1.0. Use [[kafka.admin.AdminClientPreferredReplicaLeaderElectionCommand]] instead.
  */
@Deprecated
class ZkPreferredReplicaLeaderElectionCommand(zkUtils: ZkUtils) extends PreferredReplicaLeaderElectionCommand {

  override def electPreferredLeaders(partitionsFromUser: Option[scala.collection.Set[TopicAndPartition]]) = {
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

      println(s"Successfully started preferred replica election for partitions $validPartitions")
      invalidPartitions.foreach(p => println(
        s"Skipping preferred replica leader election for partition $p since it doesn't exist."))
    } catch {
      case e: Throwable => throw new AdminCommandFailedException("Admin command failed", e)
    }
  }

  override def close(): Unit = {
    zkUtils.close()
  }
}
