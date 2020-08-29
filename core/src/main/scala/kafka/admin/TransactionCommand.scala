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

    props.setProperty(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
      commandOptions.options.valueOf(commandOptions.bootstrapServer)
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

  private def describeProducers(
    admin: Admin,
    brokerId: Option[Int],
    topicPartition: TopicPartition
  ): Unit = {
    val options = new DescribeProducersOptions()
    brokerId.foreach(options.setBrokerId)

    val result = admin.describeProducers(Seq(topicPartition).asJava, options)
      .partitionResult(topicPartition).get()

    val headers = Array(
      "ProducerId", "ProducerEpoch", "LastSequence", "LastTimestamp", "CurrentTransactionStartOffset"
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
    CommandLineUtils.printHelpAndExitIfNeeded(
      commandOptions,
      "This tool attempts to elect a new leader for a set of topic partitions. The type of elections supported are preferred replicas and unclean replicas."
    )

    CommandLineUtils.printHelpAndExitIfNeeded(commandOptions, "This tool is used to analyze transaction " +
      "state and recover from hanging transactions")

    val admin = createAdmin(commandOptions)
    val brokerId = Option(commandOptions.options.valueOf(commandOptions.brokerId)).map(Int.unbox)
    val topicPartitionOpt = (
      Option(commandOptions.options.valueOf(commandOptions.topic)),
      Option(commandOptions.options.valueOf(commandOptions.partition))
    ) match {
      case (Some(topic), Some(partition)) => Some(new TopicPartition(topic, partition))
      case _ => None
    }

    topicPartitionOpt.foreach { topicPartition =>
      describeProducers(admin, brokerId, topicPartition)
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

  val describeProducersOptions = parser
    .accepts("describe-producers",
      "Used to describe active transactional/idempotent producers " +
        "writing to a specific topic partition (you must specify --topic and --partition)")

  options = parser.parse(args: _*)

  if (Seq(describeProducersOptions).count(options.has) != 1) {
    CommandLineUtils.printUsageAndDie(parser,
      "Command must include exactly one action: --describe-producers")
  }
}
