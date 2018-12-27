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

import java.io.PrintStream
import java.util.Properties

import kafka.common.AdminCommandFailedException
import kafka.utils.json.JsonValue
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, CoreUtils, Json}
import org.apache.kafka.clients.admin.RecordsToDelete
import org.apache.kafka.clients.{CommonClientConfigs, admin}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConverters._

/**
 * A command for delete records of the given partitions down to the specified offset.
 */
object DeleteRecordsCommand {

  private[admin] val EarliestVersion = 1

  def main(args: Array[String]): Unit = {
    execute(args, System.out)
  }

  def parseOffsetJsonStringWithoutDedup(jsonData: String): Seq[(TopicPartition, Long)] = {
    Json.parseFull(jsonData) match {
      case Some(js) =>
        val version = js.asJsonObject.get("version") match {
          case Some(jsonValue) => jsonValue.to[Int]
          case None => EarliestVersion
        }
        parseJsonData(version, js)
      case None => throw new AdminOperationException("The input string is not a valid JSON")
    }
  }

  def parseJsonData(version: Int, js: JsonValue): Seq[(TopicPartition, Long)] = {
    version match {
      case 1 =>
        js.asJsonObject.get("partitions") match {
          case Some(partitions) =>
            partitions.asJsonArray.iterator.map(_.asJsonObject).map { partitionJs =>
              val topic = partitionJs("topic").to[String]
              val partition = partitionJs("partition").to[Int]
              val offset = partitionJs("offset").to[Long]
              new TopicPartition(topic, partition) -> offset
            }.toBuffer
          case _ => throw new AdminOperationException("Missing partitions field");
        }
      case _ => throw new AdminOperationException(s"Not supported version field value $version")
    }
  }

  def execute(args: Array[String], out: PrintStream): Unit = {
    val opts = new DeleteRecordsCommandOptions(args)
    val adminClient = createAdminClient(opts)
    val offsetJsonFile =  opts.options.valueOf(opts.offsetJsonFileOpt)
    val offsetJsonString = Utils.readFileAsString(offsetJsonFile)
    val offsetSeq = parseOffsetJsonStringWithoutDedup(offsetJsonString)

    val duplicatePartitions = CoreUtils.duplicates(offsetSeq.map { case (tp, _) => tp })
    if (duplicatePartitions.nonEmpty)
      throw new AdminCommandFailedException("Offset json file contains duplicate topic partitions: %s".format(duplicatePartitions.mkString(",")))

    val recordsToDelete = offsetSeq.map { case (topicPartition, offset) =>
      (topicPartition, RecordsToDelete.beforeOffset(offset))
    }.toMap.asJava

    out.println("Executing records delete operation")
    val deleteRecordsResult = adminClient.deleteRecords(recordsToDelete)
    out.println("Records delete operation completed:")

    deleteRecordsResult.lowWatermarks.asScala.foreach { case (tp, partitionResult) => {
      try out.println(s"partition: $tp\tlow_watermark: ${partitionResult.get.lowWatermark}")
      catch {
        case e: Exception => out.println(s"partition: $tp\terror: ${e.getMessage}")
      }
    }}

    adminClient.close()
  }

  private def createAdminClient(opts: DeleteRecordsCommandOptions): admin.AdminClient = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    admin.AdminClient.create(props)
  }

  class DeleteRecordsCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val BootstrapServerDoc = "REQUIRED: The server to connect to."
    val offsetJsonFileDoc = "REQUIRED: The JSON file with offset per partition. The format to use is:\n" +
                                 "{\"partitions\":\n  [{\"topic\": \"foo\", \"partition\": 1, \"offset\": 1}],\n \"version\":1\n}"
    val CommandConfigDoc = "A property file containing configs to be passed to Admin Client."

    val bootstrapServerOpt = parser.accepts("bootstrap-server", BootstrapServerDoc)
                                   .withRequiredArg
                                   .describedAs("server(s) to use for bootstrapping")
                                   .ofType(classOf[String])
    val offsetJsonFileOpt = parser.accepts("offset-json-file", offsetJsonFileDoc)
                                   .withRequiredArg
                                   .describedAs("Offset json file path")
                                   .ofType(classOf[String])
    val commandConfigOpt = parser.accepts("command-config", CommandConfigDoc)
                                   .withRequiredArg
                                   .describedAs("command config property file path")
                                   .ofType(classOf[String])

    options = parser.parse(args : _*)

    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool helps to delete records of the given partitions down to the specified offset.")

    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, offsetJsonFileOpt)
  }
}
