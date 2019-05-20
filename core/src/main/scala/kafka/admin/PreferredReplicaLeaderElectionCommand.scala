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
import java.util.concurrent.ExecutionException

import joptsimple.OptionSpecBuilder
import kafka.common.AdminCommandFailedException
import kafka.utils._
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.errors.TimeoutException

import collection.JavaConverters._
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.{KafkaFuture, TopicPartition}
import org.apache.zookeeper.KeeperException.NodeExistsException

import collection._

object PreferredReplicaLeaderElectionCommand extends Logging {

  def main(args: Array[String]): Unit = {

    val timeout = 30000
    run(args, timeout)
  }
  def run(args: Array[String], timeout: Int = 30000): Unit = {
    val commandOpts = new PreferredReplicaLeaderElectionCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(commandOpts, "This tool helps to causes leadership for each partition to be transferred back to the 'preferred replica'," +
      " it can be used to balance leadership among the servers.")

    CommandLineUtils.checkRequiredArgs(commandOpts.parser, commandOpts.options)

    if (commandOpts.options.has(commandOpts.bootstrapServerOpt) == commandOpts.options.has(commandOpts.zkConnectOpt)) {
      CommandLineUtils.printUsageAndDie(commandOpts.parser, s"Exactly one of '${commandOpts.bootstrapServerOpt}' or '${commandOpts.zkConnectOpt}' must be provided")
    }

    val partitionsForPreferredReplicaElection =
    if (commandOpts.options.has(commandOpts.jsonFileOpt))
      Some(parsePreferredReplicaElectionData(Utils.readFileAsString(commandOpts.options.valueOf(commandOpts.jsonFileOpt))))
    else
      None

    val preferredReplicaElectionCommand = if (commandOpts.options.has(commandOpts.zkConnectOpt)) {
      println(s"Warning: --zookeeper is deprecated and will be removed in a future version of Kafka.")
      println(s"Use --bootstrap-server instead to specify a broker to connect to.")
      new ZkCommand(commandOpts.options.valueOf(commandOpts.zkConnectOpt),
              JaasUtils.isZkSecurityEnabled,
              timeout)
    } else {
        val adminProps = if (commandOpts.options.has(commandOpts.adminClientConfigOpt))
          Utils.loadProps(commandOpts.options.valueOf(commandOpts.adminClientConfigOpt))
        else
          new Properties()
        adminProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, commandOpts.options.valueOf(commandOpts.bootstrapServerOpt))
        adminProps.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout.toString)
        new AdminClientCommand(adminProps)
    }

    try {
      preferredReplicaElectionCommand.electPreferredLeaders(partitionsForPreferredReplicaElection)
    } finally {
      preferredReplicaElectionCommand.close()
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

  class PreferredReplicaLeaderElectionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val jsonFileOpt = parser.accepts("path-to-json-file", "The JSON file with the list of partitions " +
      "for which preferred replica leader election should be done, in the following format - \n" +
      "{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\n" +
      "Defaults to all existing partitions")
      .withRequiredArg
      .describedAs("list of partitions for which preferred replica leader election needs to be triggered")
      .ofType(classOf[String])

    private val zookeeperOptBuilder: OptionSpecBuilder = parser.accepts("zookeeper",
      "DEPRECATED. The connection string for the zookeeper connection in the " +
      "form host:port. Multiple URLS can be given to allow fail-over. " +
      "Replaced by --bootstrap-server, REQUIRED unless --bootstrap-server is given.")
    private val bootstrapOptBuilder: OptionSpecBuilder = parser.accepts("bootstrap-server",
      "A hostname and port for the broker to connect to, " +
      "in the form host:port. Multiple comma-separated URLs can be given. REQUIRED unless --zookeeper is given.")
    parser.mutuallyExclusive(zookeeperOptBuilder, bootstrapOptBuilder)
    val bootstrapServerOpt = bootstrapOptBuilder
      .withRequiredArg
      .describedAs("host:port")
      .ofType(classOf[String])
    val zkConnectOpt = zookeeperOptBuilder
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val adminClientConfigOpt = parser.accepts("admin.config",
      "Admin client config properties file to pass to the admin client when --bootstrap-server is given.")
      .availableIf(bootstrapServerOpt)
      .withRequiredArg
      .describedAs("config file")
      .ofType(classOf[String])

    options = parser.parse(args: _*)
  }

  /** Abstraction over different ways to perform a leader election */
  trait Command {
    /** Elect the preferred leader for the given {@code partitionsForElection}.
      * If the given {@code partitionsForElection} are None then elect the preferred leader for all partitions.
      */
    def electPreferredLeaders(partitionsForElection: Option[Set[TopicPartition]]) : Unit
    def close() : Unit
  }

  class ZkCommand(zkConnect: String, isSecure: Boolean, timeout: Int)
    extends Command {
    var zkClient: KafkaZkClient = null

    val time = Time.SYSTEM
    zkClient = KafkaZkClient(zkConnect, isSecure, timeout, timeout, Int.MaxValue, time)

    override def electPreferredLeaders(partitionsFromUser: Option[Set[TopicPartition]]) {
      try {
        val topics =
          partitionsFromUser match {
            case Some(partitions) =>
              partitions.map(_.topic).toSet
            case None =>
              zkClient.getAllPartitions().map(_.topic)
          }

        val partitionsFromZk = zkClient.getPartitionsForTopics(topics).flatMap{ case (topic, partitions) =>
          partitions.map(new TopicPartition(topic, _))
        }.toSet

        val (validPartitions, invalidPartitions) =
          partitionsFromUser match {
            case Some(partitions) =>
              partitions.partition(partitionsFromZk.contains)
            case None =>
              (zkClient.getAllPartitions(), Set.empty)
          }
          PreferredReplicaLeaderElectionCommand.writePreferredReplicaElectionData(zkClient, validPartitions)

        println("Successfully started preferred replica election for partitions %s".format(validPartitions))
        invalidPartitions.foreach(p => println("Skipping preferred replica leader election for partition %s since it doesn't exist.".format(p)))
      } catch {
        case e: Throwable => throw new AdminCommandFailedException("Admin command failed", e)
      }
    }

    override def close(): Unit = {
      if (zkClient != null)
        zkClient.close()
    }
  }

  /** Election via AdminClient.electPreferredLeaders() */
  class AdminClientCommand(adminClientProps: Properties)
    extends Command with Logging {

    val adminClient = org.apache.kafka.clients.admin.AdminClient.create(adminClientProps)

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

    override def electPreferredLeaders(partitionsFromUser: Option[Set[TopicPartition]]): Unit = {
      val partitions = partitionsFromUser match {
        case Some(partitionsFromUser) => partitionsFromUser.asJava
        case None => null
      }
      debug(s"Calling AdminClient.electPreferredLeaders($partitions)")
      val result = adminClient.electPreferredLeaders(partitions)
      // wait for all results

      val attemptedPartitions = partitionsFromUser match {
        case Some(partitionsFromUser) => partitions.asScala
        case None => try {
          result.partitions().get.asScala
        } catch {
          case e: ExecutionException =>
            val cause = e.getCause
            if (cause.isInstanceOf[TimeoutException]) {
              // We timed out, or don't even know the attempted partitions
              println("Timeout waiting for election results")
            }
            throw new AdminCommandFailedException(null, cause)
          case e: Throwable =>
            // We don't even know the attempted partitions
            println("Error while making request")
            e.printStackTrace()
            return
        }
      }

      val (exceptional, ok) = attemptedPartitions.map(tp => tp -> result.partitionResult(tp)).
        partition { case (_, partitionResult) => completedExceptionally(partitionResult) }

      if (!ok.isEmpty) {
        println(s"Successfully completed preferred replica election for partitions ${ok.map{ case (tp, future) => tp }.mkString(", ")}")
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
}

class PreferredReplicaLeaderElectionCommand(zkClient: KafkaZkClient, partitionsFromUser: scala.collection.Set[TopicPartition]) {
  def moveLeaderToPreferredReplica() = {
    try {
      val topics = partitionsFromUser.map(_.topic).toSet
      val partitionsFromZk = zkClient.getPartitionsForTopics(topics).flatMap { case (topic, partitions) =>
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
