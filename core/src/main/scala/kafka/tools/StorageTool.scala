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
import java.util.UUID

import kafka.server.{BrokerMetadataCheckpoint, KafkaConfig, LegacyMetaProperties, MetaProperties}
import kafka.utils.{Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{store, storeTrue}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
 * An exception thrown to indicate that the command has failed, but we don't want to
 * print a stack trace.
 *
 * @param message     The message to print out before exiting.  A stack trace will not
 *                    be printed.
 */
class TerseFailure(message: String) extends KafkaException(message) {
}

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
        case "info" => {
          val directories = configToLogDirectories(config.get)
          val kip500Mode = configToKip500Mode(config.get)
          Exit.exit(infoCommand(System.out, kip500Mode, directories))
        }
        case "format" => {
          val directories = configToLogDirectories(config.get)
          val clusterId = namespace.getString("cluster_id")
          val ignoreFormatted = namespace.getBoolean("ignore_formatted")
          if (!configToKip500Mode(config.get)) {
            throw new TerseFailure("The kafka configuration file appears to be for " +
              "a legacy cluster. Formatting is only supported for kip-500 clusters.")
          }
          Exit.exit(formatCommand(System.out, directories, clusterId, ignoreFormatted ))
        }
        case "random-uuid" => {
          System.out.println(UUID.randomUUID())
          Exit.exit(0)
        }
        case _ => {
          throw new RuntimeException(s"Unknown command $command")
        }
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
    Option(config.metadataLogDir).foreach(directories.add(_))
    directories.toSeq
  }

  def configToKip500Mode(config: KafkaConfig): Boolean = !config.processRoles.isEmpty

  def infoCommand(stream: PrintStream, kip500Mode: Boolean, directories: Seq[String]): Int = {
    val problems = new mutable.ArrayBuffer[String]
    val foundDirectories = new mutable.ArrayBuffer[String]
    var prevMetadata: Option[Either[LegacyMetaProperties, MetaProperties]] = None
    directories.sorted.foreach(directory => {
      val directoryPath = Paths.get(directory)
      if (!Files.isDirectory(directoryPath)) {
        if (!Files.exists(directoryPath)) {
          problems += s"${directoryPath} does not exist"
        } else {
          problems += s"${directoryPath} is not a directory"
        }
      } else {
        foundDirectories += directoryPath.toString
        val metaPath = directoryPath.resolve("meta.properties")
        if (!Files.exists(metaPath)) {
          problems += s"${directoryPath} is not formatted."
        } else {
          val properties = Utils.loadProps(metaPath.toString)
          Try(MetaProperties.version(properties)) match {
            case Failure(exception) =>
              problems += s"Unable to find version in ${metaPath}: ${exception.toString}"
            case Success(version) => {
              val curMetadata: Option[Either[LegacyMetaProperties, MetaProperties]] = version match {
                case 0 => Some(Left(LegacyMetaProperties(properties)))
                case 1 => Some(Right(MetaProperties(properties)))
                case v => {
                  problems += s"Unsupported version for ${metaPath}: ${v}"
                  None
                }
              }
              if (prevMetadata.isEmpty) {
                prevMetadata = curMetadata
              } else {
                if (!prevMetadata.get.equals(curMetadata.get)) {
                  problems += s"Metadata for ${metaPath} was ${curMetadata.get}, " +
                    s"but other directories featured ${prevMetadata.get}"
                }
              }
            }
          }
        }
      }
    })
    if (prevMetadata.isDefined) {
      if (kip500Mode) {
        if (prevMetadata.get.isLeft) {
          problems += "The kafka configuration file appears to be for a kip-500 cluster, but " +
            "the directories are formatted for legacy mode."
        }
      } else if (prevMetadata.get.isRight) {
        problems += "The kafka configuration file appears to be for a legacy cluster, but " +
          "the directories are formatted for kip-500."
      }
    }
    if (directories.isEmpty) {
      stream.println("No directories specified.")
      0
    } else {
      if (!foundDirectories.isEmpty) {
        stream.println("Found log director%s:".format(
          if (foundDirectories.size == 1) "y" else "ies"))
        foundDirectories.foreach(d => stream.println("  %s".format(d)))
        stream.println("")
      }
      if (prevMetadata.isDefined) {
        stream.println("Found metadata: %s".format(
          prevMetadata.get match {
            case Left(p) => p.toString
            case Right(p) => p.toString
          }))
        stream.println("")
      }
      if (problems.size > 0) {
        stream.println("Found problem%s:".format(
          if (problems.size == 1) "" else "s"))
        problems.foreach(d => stream.println("  %s".format(d)))
        stream.println("")
        1
      } else {
        0
      }
    }
  }

  def formatCommand(stream: PrintStream,
                    directories: Seq[String],
                    clusterId: String,
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
      throw new TerseFailure("All of the log directories are already formatted.")
    }
    val effectiveClusterId = try {
      UUID.fromString(clusterId)
    } catch {
      case e: Throwable => throw new TerseFailure(s"Cluster ID string ${clusterId} " +
        s"does not appear to be a valid UUID: ${e.getMessage}")
    }
    val metaProperties = new MetaProperties(effectiveClusterId)
    unformattedDirectories.foreach(directory => {
      try {
        Files.createDirectories(Paths.get(directory))
      } catch {
        case e: Throwable => throw new TerseFailure(s"Unable to create storage " +
          s"directory ${directory}: ${e.getMessage}")
      }
      val metaPropertiesPath = Paths.get(directory, "meta.properties")
      val checkpoint = new BrokerMetadataCheckpoint(metaPropertiesPath.toFile)
      checkpoint.write(metaProperties.toProperties())
      stream.println(s"Formatting ${directory}")
    })
    0
  }
}
