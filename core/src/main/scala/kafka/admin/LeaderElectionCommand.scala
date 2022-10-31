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
import joptsimple.util.EnumConverter
import kafka.common.AdminCommandFailedException
import kafka.utils.CommandDefaultOptions
import kafka.utils.CommandLineUtils
import kafka.utils.CoreUtils
import kafka.utils.Implicits._
import kafka.utils.Json
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.ElectionNotNeededException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.Utils
import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.concurrent.duration._

object LeaderElectionCommand extends Logging {
  def main(args: Array[String]): Unit = {
    run(args, 30.second)
  }

  def run(args: Array[String], timeout: Duration): Unit = {
    val commandOptions = new LeaderElectionCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(
      commandOptions,
      "This tool attempts to elect a new leader for a set of topic partitions. The type of elections supported are preferred replicas and unclean replicas."
    )

    validate(commandOptions)

    val electionType = commandOptions.options.valueOf(commandOptions.electionType)

    val jsonFileTopicPartitions = Option(commandOptions.options.valueOf(commandOptions.pathToJsonFile)).map { path  =>
      parseReplicaElectionData(Utils.readFileAsString(path))
    }

    val singleTopicPartition = (
      Option(commandOptions.options.valueOf(commandOptions.topic)),
      Option(commandOptions.options.valueOf(commandOptions.partition))
    ) match {
      case (Some(topic), Some(partition)) => Some(Set(new TopicPartition(topic, partition)))
      case _ => None
    }

    /* Note: No need to look at --all-topic-partitions as we want this to be None if it is use.
     * The validate function should be checking that this option is required if the --topic and --path-to-json-file
     * are not specified.
     */
    val topicPartitions = jsonFileTopicPartitions.orElse(singleTopicPartition)

    val adminClient = {
      val props = Option(commandOptions.options.valueOf(commandOptions.adminClientConfig)).map { config =>
        Utils.loadProps(config)
      }.getOrElse(new Properties())

      props.setProperty(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        commandOptions.options.valueOf(commandOptions.bootstrapServer)
      )
      props.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeout.toMillis.toString)
      props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (timeout.toMillis / 2).toString)

      Admin.create(props)
    }

    try {
      electLeaders(adminClient, electionType, topicPartitions)
    } finally {
      adminClient.close()
    }
  }

  private[this] def parseReplicaElectionData(jsonString: String): Set[TopicPartition] = {
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
            if (duplicatePartitions.nonEmpty) {
              throw new AdminOperationException(
                s"Replica election data contains duplicate partitions: ${duplicatePartitions.mkString(",")}"
              )
            }
            partitions.toSet
          case None => throw new AdminOperationException("Replica election data is missing \"partitions\" field")
        }
      case None => throw new AdminOperationException("Replica election data is empty")
    }
  }

  private[this] def electLeaders(
    client: Admin,
    electionType: ElectionType,
    topicPartitions: Option[Set[TopicPartition]]
  ): Unit = {
    val electionResults = try {
      val partitions = topicPartitions.map(_.asJava).orNull
      debug(s"Calling AdminClient.electLeaders($electionType, $partitions)")
      client.electLeaders(electionType, partitions).partitions.get.asScala
    } catch {
      case e: ExecutionException =>
        e.getCause match {
          case cause: TimeoutException =>
            val message = "Timeout waiting for election results"
            println(message)
            throw new AdminCommandFailedException(message, cause)
          case cause: ClusterAuthorizationException =>
            val message = "Not authorized to perform leader election"
            println(message)
            throw new AdminCommandFailedException(message, cause)
          case _ =>
            throw e
        }
      case e: Throwable =>
        println("Error while making request")
        throw e
    }

    val succeeded = mutable.Set.empty[TopicPartition]
    val noop = mutable.Set.empty[TopicPartition]
    val failed = mutable.Map.empty[TopicPartition, Throwable]

    electionResults.foreach[Unit] { case (topicPartition, error) =>
      if (error.isPresent) {
        error.get match {
          case _: ElectionNotNeededException => noop += topicPartition
          case _ => failed += topicPartition -> error.get
        }
      } else {
        succeeded += topicPartition
      }
    }

    if (succeeded.nonEmpty) {
      val partitions = succeeded.mkString(", ")
      println(s"Successfully completed leader election ($electionType) for partitions $partitions")
    }

    if (noop.nonEmpty) {
      val partitions = noop.mkString(", ")
      println(s"Valid replica already elected for partitions $partitions")
    }

    if (failed.nonEmpty) {
      val rootException = new AdminCommandFailedException(s"${failed.size} replica(s) could not be elected")
      failed.forKeyValue { (topicPartition, exception) =>
        println(s"Error completing leader election ($electionType) for partition: $topicPartition: $exception")
        rootException.addSuppressed(exception)
      }
      throw rootException
    }
  }

  private[this] def validate(commandOptions: LeaderElectionCommandOptions): Unit = {
    // required options: --bootstrap-server and --election-type
    var missingOptions = List.empty[String]
    if (!commandOptions.options.has(commandOptions.bootstrapServer)) {
      missingOptions = commandOptions.bootstrapServer.options().get(0) :: missingOptions
    }

    if (!commandOptions.options.has(commandOptions.electionType)) {
      missingOptions = commandOptions.electionType.options().get(0) :: missingOptions
    }

    if (missingOptions.nonEmpty) {
      throw new AdminCommandFailedException(s"Missing required option(s): ${missingOptions.mkString(", ")}")
    }

    // One and only one is required: --topic, --all-topic-partitions or --path-to-json-file
    val mutuallyExclusiveOptions = Seq(
      commandOptions.topic,
      commandOptions.allTopicPartitions,
      commandOptions.pathToJsonFile
    )

    mutuallyExclusiveOptions.count(commandOptions.options.has) match {
      case 1 => // This is the only correct configuration, don't throw an exception
      case _ =>
        throw new AdminCommandFailedException(
          "One and only one of the following options is required: " +
          s"${mutuallyExclusiveOptions.map(_.options.get(0)).mkString(", ")}"
        )
    }

    // --partition if and only if --topic is used
    (
      commandOptions.options.has(commandOptions.topic),
      commandOptions.options.has(commandOptions.partition)
    ) match {
      case (true, false) =>
        throw new AdminCommandFailedException(
          s"Missing required option(s): ${commandOptions.partition.options.get(0)}"
        )
      case (false, true) =>
        throw new AdminCommandFailedException(
          s"Option ${commandOptions.partition.options.get(0)} is only allowed if " +
          s"${commandOptions.topic.options.get(0)} is used"
        )
      case _ => // Ignore; we have a valid configuration
    }
  }
}

private final class LeaderElectionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  val bootstrapServer = parser
    .accepts(
      "bootstrap-server",
      "A hostname and port for the broker to connect to, in the form host:port. Multiple comma separated URLs can be given. REQUIRED.")
    .withRequiredArg
    .describedAs("host:port")
    .ofType(classOf[String])
  val adminClientConfig = parser
    .accepts(
      "admin.config",
      "Configuration properties files to pass to the admin client")
    .withRequiredArg
    .describedAs("config file")
    .ofType(classOf[String])

  val pathToJsonFile = parser
    .accepts(
      "path-to-json-file",
      "The JSON file with the list  of partition for which leader elections should be performed. This is an example format. \n{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1},\n\t {\"topic\": \"foobar\", \"partition\": 2}]\n}\nNot allowed if --all-topic-partitions or --topic flags are specified.")
    .withRequiredArg
    .describedAs("Path to JSON file")
    .ofType(classOf[String])

  val topic = parser
    .accepts(
      "topic",
      "Name of topic for which to perform an election. Not allowed if --path-to-json-file or --all-topic-partitions is specified.")
    .withRequiredArg
    .describedAs("topic name")
    .ofType(classOf[String])

  val partition = parser
    .accepts(
      "partition",
      "Partition id for which to perform an election. REQUIRED if --topic is specified.")
    .withRequiredArg
    .describedAs("partition id")
    .ofType(classOf[Integer])

  val allTopicPartitions = parser
    .accepts(
      "all-topic-partitions",
      "Perform election on all of the eligible topic partitions based on the type of election (see the --election-type flag). Not allowed if --topic or --path-to-json-file is specified.")

  val electionType = parser
    .accepts(
      "election-type",
      "Type of election to attempt. Possible values are \"preferred\" for preferred leader election or \"unclean\" for unclean leader election. If preferred election is selection, the election is only performed if the current leader is not the preferred leader for the topic partition. If unclean election is selected, the election is only performed if there are no leader for the topic partition. REQUIRED.")
    .withRequiredArg
    .describedAs("election type")
    .withValuesConvertedBy(ElectionTypeConverter)

  options = parser.parse(args: _*)
}

final object ElectionTypeConverter extends EnumConverter[ElectionType](classOf[ElectionType]) { }
