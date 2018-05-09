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

import kafka.admin.AdminClient.DeleteRecordsResult
import kafka.common.AdminCommandFailedException
import kafka.utils.{CoreUtils, Json, CommandLineUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.clients.CommonClientConfigs
import joptsimple._

/**
 * A command for delete records of the given partitions down to the specified offset.
 */
object DeleteRecordsCommand {

  def main(args: Array[String]): Unit = {
    execute(args, System.out)
  }

  def parseOffsetJsonStringWithoutDedup(jsonData: String): Seq[(TopicPartition, Long)] = {
    Json.parseFull(jsonData).toSeq.flatMap { js =>
      js.asJsonObject.get("partitions").toSeq.flatMap { partitionsJs =>
        partitionsJs.asJsonArray.iterator.map(_.asJsonObject).map { partitionJs =>
          val topic = partitionJs("topic").to[String]
          val partition = partitionJs("partition").to[Int]
          val offset = partitionJs("offset").to[Long]
          new TopicPartition(topic, partition) -> offset
        }.toBuffer
      }
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

    out.println("Executing records delete operation")
    val deleteRecordsResult: Map[TopicPartition, DeleteRecordsResult] = adminClient.deleteRecordsBefore(offsetSeq.toMap).get()
    out.println("Records delete operation completed:")

    deleteRecordsResult.foreach{ case (tp, partitionResult) => {
      if (partitionResult.error == null)
        out.println(s"partition: $tp\tlow_watermark: ${partitionResult.lowWatermark}")
      else
        out.println(s"partition: $tp\terror: ${partitionResult.error.toString}")
    }}
    adminClient.close()
  }

  private def createAdminClient(opts: DeleteRecordsCommandOptions): AdminClient = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    AdminClient.create(props)
  }

  class DeleteRecordsCommandOptions(args: Array[String]) {
    val BootstrapServerDoc = "REQUIRED: The server to connect to."
    val offsetJsonFileDoc = "REQUIRED: The JSON file with offset per partition. The format to use is:\n" +
                                 "{\"partitions\":\n  [{\"topic\": \"foo\", \"partition\": 1, \"offset\": 1}],\n \"version\":1\n}"
    val CommandConfigDoc = "A property file containing configs to be passed to Admin Client."

    val parser = new OptionParser(false)
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

    val options = parser.parse(args : _*)
    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, offsetJsonFileOpt)
  }
}
