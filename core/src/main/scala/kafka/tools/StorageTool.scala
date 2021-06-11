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

package kafka.tools

import java.io.PrintStream
import java.nio.file.{Files, Paths}

import kafka.server.{BrokerMetadataCheckpoint, KafkaConfig, MetaProperties, RawMetaProperties}
import kafka.utils.{Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{store, storeTrue}
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable

object StorageTool extends Logging {
  def main(args: Array[String]): Unit = {
    try {
      val parser = ArgumentParsers.
        newArgumentParser("kafka-storage").
        defaultHelp(true).
        description("The Kafka storage tool.")
      val subparsers = parser.addSubparsers().dest("command")

      val infoParser = subparsers.addParser("info").
        help("Get information about the Kafka log directories on this node.")
      val formatParser = subparsers.addParser("format").
        help("Format the Kafka log directories on this node.")
      subparsers.addParser("random-uuid").help("Print a random UUID.")
      List(infoParser, formatParser).foreach(parser => {
        parser.addArgument("--config", "-c").
          action(store()).
          required(true).
          help("The Kafka configuration file to use.")
      })
      formatParser.addArgument("--cluster-id", "-t").
        action(store()).
        required(true).
        help("The cluster ID to use.")
      formatParser.addArgument("--ignore-formatted", "-g").
        action(storeTrue())

      val namespace = parser.parseArgsOrFail(args)
      val command = namespace.getString("command")
      val config = Option(namespace.getString("config")).flatMap(
        p => Some(new KafkaConfig(Utils.loadProps(p))))

      command match {
        case "info" =>
          val directories = configToLogDirectories(config.get)
          val selfManagedMode = configToSelfManagedMode(config.get)
          Exit.exit(infoCommand(System.out, selfManagedMode, directories))

        case "format" =>
          val directories = configToLogDirectories(config.get)
          val clusterId = namespace.getString("cluster_id")
          val metaProperties = buildMetadataProperties(clusterId, config.get)
          val ignoreFormatted = namespace.getBoolean("ignore_formatted")
          if (!configToSelfManagedMode(config.get)) {
            throw new TerseFailure("The kafka configuration file appears to be for " +
              "a legacy cluster. Formatting is only supported for clusters in KRaft mode.")
          }
          Exit.exit(formatCommand(System.out, directories, metaProperties, ignoreFormatted ))

        case "random-uuid" =>
          System.out.println(Uuid.randomUuid)
          Exit.exit(0)

        case _ =>
          throw new RuntimeException(s"Unknown command $command")
      }
    } catch {
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        System.exit(1)
    }
  }

  def configToLogDirectories(config: KafkaConfig): Seq[String] = {
    val directories = new mutable.TreeSet[String]
    directories ++= config.logDirs
    Option(config.metadataLogDir).foreach(directories.add)
    directories.toSeq
  }

  def configToSelfManagedMode(config: KafkaConfig): Boolean = config.processRoles.nonEmpty

  def infoCommand(stream: PrintStream, selfManagedMode: Boolean, directories: Seq[String]): Int = {
    val problems = new mutable.ArrayBuffer[String]
    val foundDirectories = new mutable.ArrayBuffer[String]
    var prevMetadata: Option[RawMetaProperties] = None
    directories.sorted.foreach(directory => {
      val directoryPath = Paths.get(directory)
      if (!Files.isDirectory(directoryPath)) {
        if (!Files.exists(directoryPath)) {
          problems += s"$directoryPath does not exist"
        } else {
          problems += s"$directoryPath is not a directory"
        }
      } else {
        foundDirectories += directoryPath.toString
        val metaPath = directoryPath.resolve("meta.properties")
        if (!Files.exists(metaPath)) {
          problems += s"$directoryPath is not formatted."
        } else {
          val properties = Utils.loadProps(metaPath.toString)
          val rawMetaProperties = new RawMetaProperties(properties)

          val curMetadata = rawMetaProperties.version match {
            case 0 | 1 => Some(rawMetaProperties)
            case v =>
              problems += s"Unsupported version for $metaPath: $v"
              None
          }

          if (prevMetadata.isEmpty) {
            prevMetadata = curMetadata
          } else {
            if (!prevMetadata.get.equals(curMetadata.get)) {
              problems += s"Metadata for $metaPath was ${curMetadata.get}, " +
                s"but other directories featured ${prevMetadata.get}"
            }
          }
        }
      }
    })

    prevMetadata.foreach { prev =>
      if (selfManagedMode) {
        if (prev.version == 0) {
          problems += "The kafka configuration file appears to be for a cluster in KRaft mode, but " +
            "the directories are formatted for legacy mode."
        }
      } else if (prev.version == 1) {
        problems += "The kafka configuration file appears to be for a legacy cluster, but " +
          "the directories are formatted for a cluster in KRaft mode."
      }
    }

    if (directories.isEmpty) {
      stream.println("No directories specified.")
      0
    } else {
      if (foundDirectories.nonEmpty) {
        if (foundDirectories.size == 1) {
          stream.println("Found log directory:")
        } else {
          stream.println("Found log directories:")
        }
        foundDirectories.foreach(d => stream.println("  %s".format(d)))
        stream.println("")
      }

      prevMetadata.foreach { prev =>
        stream.println(s"Found metadata: ${prev}")
        stream.println("")
      }

      if (problems.nonEmpty) {
        if (problems.size == 1) {
          stream.println("Found problem:")
        } else {
          stream.println("Found problems:")
        }
        problems.foreach(d => stream.println("  %s".format(d)))
        stream.println("")
        1
      } else {
        0
      }
    }
  }

  def buildMetadataProperties(
    clusterIdStr: String,
    config: KafkaConfig
  ): MetaProperties = {
    val effectiveClusterId = try {
      Uuid.fromString(clusterIdStr)
    } catch {
      case e: Throwable => throw new TerseFailure(s"Cluster ID string $clusterIdStr " +
        s"does not appear to be a valid UUID: ${e.getMessage}")
    }
    require(config.nodeId >= 0, s"The node.id must be set to a non-negative integer.")
    new MetaProperties(effectiveClusterId.toString, config.nodeId)
  }

  def formatCommand(stream: PrintStream,
                    directories: Seq[String],
                    metaProperties: MetaProperties,
                    ignoreFormatted: Boolean): Int = {
    if (directories.isEmpty) {
      throw new TerseFailure("No log directories found in the configuration.")
    }
    val unformattedDirectories = directories.filter(directory => {
      if (!Files.isDirectory(Paths.get(directory)) || !Files.exists(Paths.get(directory, "meta.properties"))) {
          true
      } else if (!ignoreFormatted) {
        throw new TerseFailure(s"Log directory ${directory} is already formatted. " +
          "Use --ignore-formatted to ignore this directory and format the others.")
      } else {
        false
      }
    })
    if (unformattedDirectories.isEmpty) {
      stream.println("All of the log directories are already formatted.")
    }
    unformattedDirectories.foreach(directory => {
      try {
        Files.createDirectories(Paths.get(directory))
      } catch {
        case e: Throwable => throw new TerseFailure(s"Unable to create storage " +
          s"directory ${directory}: ${e.getMessage}")
      }
      val metaPropertiesPath = Paths.get(directory, "meta.properties")
      val checkpoint = new BrokerMetadataCheckpoint(metaPropertiesPath.toFile)
      checkpoint.write(metaProperties.toProperties)
      stream.println(s"Formatting ${directory}")
    })
    0
  }
}
