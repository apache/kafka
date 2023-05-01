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

import java.util.{Optional, Properties}
import java.util.concurrent.ExecutionException
import kafka.common.AdminCommandFailedException
import kafka.utils.CommandDefaultOptions
import kafka.utils.CommandLineUtils
import kafka.utils.CoreUtils
import kafka.utils.Implicits._
import kafka.utils.Json
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ClusterAuthorizationException
import org.apache.kafka.common.errors.ElectionNotNeededException
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._
import scala.collection.mutable
import scala.concurrent.duration._

object RecommendedLeaderElectionCommand extends Logging {
  val PREFERRED_BROKER_ENV = "PREFERRED_BROKERS"

  var preferredBrokers: Set[Int] = _


  def main(args: Array[String]): Unit = {
    run(args, 30.second)
  }

  def run(args: Array[String], timeout: Duration): Unit = {
    val commandOptions = new RecommendedLeaderElectionCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(
      commandOptions,
      "This tool attempts to elect a new (recommended) leader for a set of topic partitions. To perform a preferred or unclean leader election, please use LeaderElectionCommand instead."
    )

    validate(commandOptions)

    // This (comma-separated) list of broker IDs defines the healthy leaders from which to choose. It is required.
    val preferredBrokersOpt = 
      if (commandOptions.options.has(commandOptions.preferredBrokers))
        Option(commandOptions.options.valueOf(commandOptions.preferredBrokers))
      else
        sys.env.get(PREFERRED_BROKER_ENV)  // fall back to environment variable if flag is not defined

    preferredBrokers = preferredBrokersOpt
      .map(_.split(",").toList.map(_.trim.toInt))
      .map(_.toSet)
      .getOrElse(throw new IllegalArgumentException(
        if (commandOptions.options.has(commandOptions.preferredBrokers))
          s"Cannot parse option ${commandOptions.preferredBrokers.options().get(0)}"
        else
          s"Option ${commandOptions.preferredBrokers.options().get(0)} was not specified, and cannot parse env ${PREFERRED_BROKER_ENV}"
      ))

    val backoffDuration =
      if (commandOptions.options.has(commandOptions.backoffSeconds))
        Duration(commandOptions.options.valueOf(commandOptions.backoffSeconds), SECONDS)
      else
        Duration(60, SECONDS)

    val jsonFileTopicPartitions: Option[Map[TopicPartition, Integer]] =
      Option(commandOptions.options.valueOf(commandOptions.pathToJsonFile)).map { path  =>
        parseReplicaElectionData(Utils.readFileAsString(path))
      }

    val singleTopicPartition = (
      Option(commandOptions.options.valueOf(commandOptions.topic)),
      Option(commandOptions.options.valueOf(commandOptions.partition)),
      Option(commandOptions.options.valueOf(commandOptions.newLeader)),
    ) match {
      case (Some(topic), Some(partition), Some(newLeader)) => Some(Map(new TopicPartition(topic, partition) -> newLeader))
      case _ => None
    }

    val topicPartitionsOpt = jsonFileTopicPartitions.orElse(singleTopicPartition)

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
      topicPartitionsOpt match {
        case Some(topicPartitions) =>
          val total = topicPartitions.size
          var processed = 0
          topicPartitions.grouped(100).foreach(partitionSubset => {
            println(s"Processing ${partitionSubset} ...")
            electRecommendedLeaders(adminClient, partitionSubset)
            processed += partitionSubset.size
            if (processed == total) {
              println(s"Finished processing $processed partitions.")
            } else {
              print(s"Processed $processed/$total partitions. Sleeping ${backoffDuration.toSeconds} secs to avoid flooding...")
              Thread.sleep(backoffDuration.toMillis)
              println(" done")
            }
          })
        case None =>
          val message = "Topic-partitions must be explicitly specified but none were provided: nothing to do!"
          println(message)
          throw new AdminCommandFailedException(message)
      }
    } finally {
      adminClient.close()
    }
  }

  // Parse the provided JSON string and return the topic-partitions and their corresponding new leaders.
  private[this] def parseReplicaElectionData(jsonString: String): Map[TopicPartition, Integer] = {
    Json.parseFull(jsonString) match {
      case Some(js) =>
        //js.asJsonObject.get("KafkaPartitionState").get.asJsonObject.get("urp") match {
        js.asJsonObject.get("partitions") match {
          case Some(partitionsList) =>
            val partitionsRaw = partitionsList.asJsonArray.iterator.map(_.asJsonObject)
            val partitionsWithNewLeader =
              partitionsRaw
                .map { p =>
                  // The logic here is predicated on the assumption that the JSON's description of current leadership
                  // and current ISR matches the controller's view, even if the controller's view is nominally "wrong"
                  // (as was true in the "stuck AlterIsr" case of April 2023). Regardless of what the ISR _should_
                  // be, the controller is the one that "approves" the recommended leadership election, so "fixing"
                  // the ISR in the JSON view doesn't help: the controller won't switch leadership to a replica
                  // that's not in the controller's understanding of the ISR.
                  val topic = p("topic").to[String]
                  val partition = p("partition").to[Int]
                  val leader = p("leader").to[Int]
                  val inSync = p("in-sync").asJsonArray.iterator.map(_.to[Int]).toSet
                  if (inSync.size < 2) {
                    println(s"partition $topic-$partition has no other leader to choose. Skipping.")
                    None
                  } else if (inSync.intersect(preferredBrokers).isEmpty) {
                    println(s"partition $topic-$partition has no healthy leader to choose. Skipping.")
                    None
                  } else if (preferredBrokers.contains(leader)) {
                    println(s"Skipping $topic-$partition as the current leader ${leader} is in the preferred/healthy set.")
                    None
                  } else{
                    val newLeader = new Integer((inSync - leader).intersect(preferredBrokers).head)
                    Some((new TopicPartition(topic, partition), newLeader))
                  }
                }
                // .collect {case Some((tp: TopicPartition, newLeader: Integer)) => (tp, newLeader)}
                // somehow triggered a weird spotbugs failure:
                // ```
                // "Bug type UMAC_UNCALLABLE_METHOD_OF_ANONYMOUS_CLASS"
                // In method kafka.admin.RecommendedLeaderElectionCommand$$anonfun$1.applyOrElse(Option, Function1)
                // superclass is scala.runtime.AbstractPartialFunction
                // Did you intend to override scala.runtime.AbstractPartialFunction.applyOrElse(Object, Function1)
                // ```
                // => ended up using an equivalent but non-idiomatic or-else-null-then-filter-out pattern:
                .map(_.orNull).filter(_ != null)
                .toMap
            val duplicatePartitions = CoreUtils.duplicates(partitionsWithNewLeader)
            if (duplicatePartitions.nonEmpty) {
              throw new AdminOperationException(
                s"Replica election data contains duplicate partitions: ${duplicatePartitions.mkString(",")}"
              )
            }
            partitionsWithNewLeader
          case None => throw new AdminOperationException("Replica election data is missing \"partitions\" field")
        }
      case None => throw new AdminOperationException("Replica election data is empty")
    }
  }

  private[this] def electRecommendedLeaders(
    client: Admin,
    topicPartitionsWithNewLeaders: Map[TopicPartition, Integer]
  ): Unit = {
    val electionResults: mutable.Map[TopicPartition, Optional[Throwable]] = try {
      if (topicPartitionsWithNewLeaders != null) {
        debug(s"Calling AdminClient.electRecommendedLeaders($topicPartitionsWithNewLeaders)")
        client.electRecommendedLeaders(topicPartitionsWithNewLeaders.asJava).partitions.get.asScala
      } else {
        debug(s"electRecommendedLeaders() called with no partitions: nothing to do!")
        mutable.Map.empty[TopicPartition, Optional[Throwable]]
      }
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
      println(s"Successfully completed recommended leader election for partitions $partitions")
    }

    if (noop.nonEmpty) {
      val partitions = noop.mkString(", ")
      println(s"Valid replica already elected for partitions $partitions")
    }

    if (failed.nonEmpty) {
      val rootException = new AdminCommandFailedException(s"${failed.size} replica(s) could not be elected")
      failed.forKeyValue { (topicPartition, exception) =>
        println(s"Error completing recommended leader election for partition: $topicPartition: $exception")
        rootException.addSuppressed(exception)
      }
      throw rootException
    }
  }

  private[this] def validate(commandOptions: RecommendedLeaderElectionCommandOptions): Unit = {
    // required options: --bootstrap-server
    var missingOptions = List.empty[String]
    if (!commandOptions.options.has(commandOptions.bootstrapServer)) {
      missingOptions = commandOptions.bootstrapServer.options().get(0) :: missingOptions
    }

    if (missingOptions.nonEmpty) {
      throw new AdminCommandFailedException(s"Missing required option(s): ${missingOptions.mkString(", ")}")
    }

    // Two configurations are allowed:  (--topic AND --partition AND --leader) OR --path-to-json-file
    (
      commandOptions.options.has(commandOptions.topic),
      commandOptions.options.has(commandOptions.partition),
      commandOptions.options.has(commandOptions.newLeader),
      commandOptions.options.has(commandOptions.pathToJsonFile)
    ) match {
      case (true, true, true, false) =>    // valid, don't throw
      case (false, false, false, true) =>  // valid, don't throw
      case _ =>
        throw new AdminCommandFailedException(
          "Exactly one of the following combinations is required: (1) " +
          s"${commandOptions.pathToJsonFile.options().get(0)} OR (2) " +
          s"${commandOptions.topic.options().get(0)}, ${commandOptions.partition.options().get(0)}, and " +
          s"${commandOptions.newLeader.options().get(0)}. Mixtures of the two are not allowed."
        )
    }
  }
}

private final class RecommendedLeaderElectionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  val bootstrapServer = parser
    .accepts(
      "bootstrap-server",
      "A hostname and port for the broker to connect to, in the form host:port. Multiple comma-separated URLs can be given. REQUIRED.")
    .withRequiredArg
    .describedAs("host:port")
    .ofType(classOf[String])

  val adminClientConfig = parser
    .accepts(
      "admin.config",
      "Configuration properties file to pass to the admin client.")
    .withRequiredArg
    .describedAs("config file")
    .ofType(classOf[String])

  val pathToJsonFile = parser
    .accepts(
      "path-to-json-file",
      "The JSON file with the list  of partition for which leader elections should be performed. This is an example format. \n{\"partitions\":\n\t[{\"topic\": \"foo\", \"partition\": 1, \"leader\": 8001, \"in-sync\": [6221, 1309, 8001]},\n\t {\"topic\": \"foobar\", \"partition\": 2, \"leader\": 7911, \"in-sync\": [5577, 7911, 7174]}]\n}\nNot allowed if --topic/--partition/--leader are specified, but REQUIRED if they aren't.")
    .withRequiredArg
    .describedAs("Path to JSON file specifying topic-partitions and their corresponding _current_ in-sync replica sets and leaders (i.e., controller's and CC's view). New leaders for each partition will be selected from one of its healthy (listed in the PREFERRED_BROKERS env var), in-sync followers.")
    .ofType(classOf[String])

  val topic = parser
    .accepts(
      "topic",
      "Name of the topic for which to perform an election. REQUIRED if --partition and --leader are specified. Not allowed if --path-to-json-file is specified.")
    .withRequiredArg
    .describedAs("topic name")
    .ofType(classOf[String])

  val partition = parser
    .accepts(
      "partition",
      "Partition id for which to perform an election. REQUIRED if --topic and --leader are specified. Not allowed if --path-to-json-file is specified.")
    .withRequiredArg
    .describedAs("partition id")
    .ofType(classOf[Integer])

  val newLeader = parser
    .accepts(
      "leader",
      "The new leader for the recommended election. REQUIRED if --topic and --partition are specified. Not allowed if --path-to-json-file is specified. May be ignored if the controller doesn't believe it's in the partition's current ISR.")
    .withRequiredArg
    .describedAs("the new leader")
    .ofType(classOf[Integer])

  // a.k.a. healthyBrokers:
  val preferredBrokers = parser
    .accepts(
      "preferred-brokers",
      s"Comma-separated list of brokers considered healthy enough to be targets for leadership elections. REQUIRED if the ${RecommendedLeaderElectionCommand.PREFERRED_BROKER_ENV} environment variable is not set.")
    .withRequiredArg
    .describedAs("list of healthy broker IDs")
    .ofType(classOf[String])

  val backoffSeconds = parser
    .accepts(
      "backoff-seconds",
      "Number of seconds to pause between each batch of 100 topic-partition leadership elections. Default: 60 sec.")
    .withRequiredArg
    .describedAs("seconds to pause")
    .ofType(classOf[Long])

  options = parser.parse(args: _*)
}
