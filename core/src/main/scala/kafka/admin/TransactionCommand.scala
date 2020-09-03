/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import kafka.utils.CommandLineUtils.printErrorMessageAndDie
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Logging}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, DescribeProducersOptions}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._

object TransactionCommand extends Logging {

  private def createAdmin(commandOptions: TransactionCommandOptions): Admin = {
    val props = Option(commandOptions.options.valueOf(commandOptions.adminClientConfig)).map { config =>
      Utils.loadProps(config)
    }.getOrElse(new Properties())


    val bootstrapServersOpt = Option(commandOptions.options.valueOf(commandOptions.bootstrapServer))
    if (bootstrapServersOpt.isEmpty) {
      printErrorMessageAndDie("Missing required argument --bootstrap-server")
    }

    props.setProperty(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      bootstrapServersOpt.get
    )

    Admin.create(props)
  }

  private def prettyPrintTable(
    headers: Array[String],
    rows: Iterable[Array[String]]
  ): Unit = {
    val columnLengths = headers.map(_.length)
    for (row <- rows) {
      row.indices.foreach { i =>
        columnLengths(i) = math.max(columnLengths(i), row(i).length)
      }
    }

    def printColumn(str: String, len: Int): Unit = {
      val padLength = len - str.length
      print(str + (" " * padLength))
    }

    def printRow(row: Array[String]): Unit = {
      columnLengths.indices.foreach { i =>
        val columnLength = columnLengths(i)
        val columnValue = row(i)
        printColumn(columnValue, columnLength)
        print('\t')
      }
      println()
    }

    printRow(headers)
    rows.foreach(printRow)
  }

  private def describeTransactions(
    admin: Admin,
    transactionalId: String
  ): Unit = {
    val result = try {
      admin.describeTransactions(Seq(transactionalId).asJava)
        .transactionalIdResult(transactionalId)
        .get()
    } catch {
      case e: ExecutionException =>
        val cause = e.getCause
        debug(s"Failed to describe transaction state of transactional-id `$transactionalId`", cause)
        printErrorMessageAndDie(s"Failed to describe transaction state of transactional-id `$transactionalId`: " +
          s"${cause.getMessage}. Enable debug logging for additional detail.")
    }

    // TODO: Do we want a way to return coordinator ID?
    val headers = Array(
      "ProducerId",
      "ProducerEpoch",
      "TransactionState",
      "TransactionTimeoutMs",
      "CurrentTransactionStartTimeMs",
      "TopicPartitions"
    )

    val transactionStartTimeMsColumnValue = if (result.transactionStartTimeMs.isPresent) {
      result.transactionStartTimeMs.getAsLong.toString
    } else {
      "None"
    }

    val rows = Array(
      result.producerId.toString,
      result.producerEpoch.toString,
      result.state,
      result.transactionTimeoutMs.toString,
      transactionStartTimeMsColumnValue,
      Utils.join(result.topicPartitions, ",")
    )

    prettyPrintTable(headers, Seq(rows))
  }

  private def describeProducers(
    admin: Admin,
    brokerId: Option[Int],
    topicPartition: TopicPartition
  ): Unit = {
    val options = new DescribeProducersOptions()
    brokerId.foreach(options.setBrokerId)

    val result = try {
      admin.describeProducers(Seq(topicPartition).asJava, options)
        .partitionResult(topicPartition)
        .get()
    } catch {
      case e: ExecutionException =>
        val cause = e.getCause
        val brokerClause = brokerId.map(id => s"broker $id").getOrElse("leader")
        debug(s"Failed to describe producers for partition $topicPartition on $brokerClause", cause)
        printErrorMessageAndDie(s"Failed to describe producers for partition $topicPartition on $brokerClause: " +
          s"${cause.getMessage}. Enable debug logging for additional detail.")
    }

    // TODO: Add coordinator epoch
    val headers = Array(
      "ProducerId",
      "ProducerEpoch",
      "LastSequence",
      "LastTimestamp",
      "CurrentTransactionStartOffset"
    )

    val rows = result.activeProducers().asScala.map { activeProducer =>
      val currentTransactionStartOffsetColumnValue = if (activeProducer.currentTransactionStartOffset.isPresent) {
        activeProducer.currentTransactionStartOffset.toString
      } else {
        "None"
      }

      Array(activeProducer.producerId.toString,
        activeProducer.producerEpoch.toString,
        activeProducer.lastSequence.toString,
        activeProducer.lastTimestamp.toString,
        currentTransactionStartOffsetColumnValue
      )
    }

    prettyPrintTable(headers, rows)
  }

  def main(args: Array[String]): Unit = {
    val commandOptions = new TransactionCommandOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(commandOptions, "This tool is " +
      "used to analyze transaction state and recover from hanging transactions")

    val admin = createAdmin(commandOptions)
    val brokerIdOpt = Option(commandOptions.options.valueOf(commandOptions.brokerId)).map(Int.unbox)
    val topicPartitionOpt = (
      Option(commandOptions.options.valueOf(commandOptions.topic)),
      Option(commandOptions.options.valueOf(commandOptions.partition))
    ) match {
      case (Some(topic), Some(partition)) => Some(new TopicPartition(topic, partition))
      case _ => None
    }

    if (commandOptions.options.has(commandOptions.describeProducersOption)) {
      topicPartitionOpt match {
        case Some(topicPartition) =>
          describeProducers(admin, brokerIdOpt, topicPartition)
        case None =>
          printErrorMessageAndDie("The --describe-producers action requires both " +
            "the --topic and --partition arguments")
      }
    } else if (commandOptions.options.has(commandOptions.describeOption)) {
      Option(commandOptions.options.valueOf(commandOptions.transactionalId)) match {
        case Some(transactionalId) =>
          describeTransactions(admin, transactionalId)
        case None =>
          printErrorMessageAndDie("The --describe action requires the " +
            "--transactional-id argument")
      }
    }
  }
}

private final class TransactionCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  val bootstrapServer = parser
    .accepts("bootstrap-server",
      "(REQUIRED) A hostname and port for the broker to connect to, in the form host:port. " +
        "Multiple comma separated URLs can be given.")
    .withRequiredArg
    .describedAs("host:port")
    .ofType(classOf[String])
  val adminClientConfig = parser
    .accepts("admin.config",
      "Configuration properties files to pass to the admin client")
    .withRequiredArg
    .describedAs("config file")
    .ofType(classOf[String])

  val topic = parser
    .accepts("topic",
      "Name of topic for which to perform an election.")
    .withRequiredArg
    .describedAs("topic name")
    .ofType(classOf[String])

  val partition = parser
    .accepts("partition",
      "Partition id. REQUIRED if --topic is specified.")
    .withRequiredArg
    .describedAs("partition id")
    .ofType(classOf[Integer])

  val brokerId = parser
    .accepts("broker-id",
      "Used with --topic and --partition to indicate a specific broker to verify")
    .withRequiredArg
    .describedAs("broker id")
    .ofType(classOf[Integer])

  val transactionalId = parser
    .accepts("transactional-id")
    .withRequiredArg
    .describedAs("transactional id")
    .ofType(classOf[String])

  val describeOption = parser
    .accepts("describe",
    "Used to describe the transaction state of a specific transactional id " +
      "(requires --transactional-id)")

  val describeProducersOption = parser
    .accepts("describe-producers",
      "Used to describe active transactional/idempotent producers " +
        "writing to a specific topic partition (requires --topic and --partition)")

  options = parser.parse(args: _*)

  if (Seq(describeOption, describeProducersOption).count(options.has) != 1) {
    CommandLineUtils.printUsageAndDie(parser,
      "Command must include exactly one action: --describe-producers")
  }
}
