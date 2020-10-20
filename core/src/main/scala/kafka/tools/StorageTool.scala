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

import kafka.server.KafkaConfig
import kafka.utils.{Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{append, store, storeTrue}
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.jdk.CollectionConverters._

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

      val infoParser = subparsers.addParser("info")
      val formatParser = subparsers.addParser("format")
      List(infoParser, formatParser).foreach(parser => {
        parser.addArgument("--config", "-c").
          action(store()).
          help("The Kafka configuration file to use.")
        parser.addArgument("--directory", "-d").
          action(append()).
          help("The directories to use.")
      })
      formatParser.addArgument("--force", "-f").
        action(storeTrue())

      val namespace = parser.parseArgsOrFail(args)
      val command = namespace.getString("command")
      val config = Option(namespace.getString("config")).flatMap(
        p => Some(new KafkaConfig(Utils.loadProps(p))))

      command match {
        case "info" => {
          val directories = readDirectoryOptions(config, namespace)
          Exit.exit(infoCommand(System.out, directories))
        }
        case "format" => {
          val directories = readDirectoryOptions(config, namespace)
          val force = namespace.getBoolean("force")
          Exit.exit(format(System.out, directories, force))
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

  def readDirectoryOptions(config: Option[KafkaConfig], namespace: Namespace): Seq[String] = {
    val directoryList = Option(namespace.getList[String]("directory"))
    if (directoryList.isDefined) {
      directoryList.get.asScala.toSeq
    } else if (config.isDefined) {
      config.get.logDirs.toSeq
    } else {
      throw new TerseFailure("You must specify either --directory or --config")
    }
  }

  def infoCommand(stream: PrintStream, directories: Seq[String]): Int = {
    val problems = new mutable.ArrayBuffer[String]
    val foundDirectories = new mutable.ArrayBuffer[String]
    var prevKip500: Option[Boolean] = None
    var prevClusterId: Option[String] = None
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
          val props = Utils.loadProps(metaPath.toString)
          val clusterId = props.getProperty("cluster.id")
          if (clusterId == null) {
            problems += s"${metaPath} does not contain cluster.id."
          } else {
            if (prevClusterId.isDefined) {
              if (!prevClusterId.get.equals(clusterId)) {
                problems += s"${metaPath} has a different cluster id than the other directories."
              }
            } else {
              prevClusterId = Some(clusterId)
            }
          }
          val kip500prop = props.getProperty("kip.500")
          val kip500 = kip500prop != null && kip500prop.equals("true")
          if (prevKip500.isDefined) {
            if (prevKip500.get != kip500) {
              problems += s"${metaPath} has a different value for kip.500 than the other directories."
            }
          } else {
            prevKip500 = Some(kip500)
          }
        }
      }
    })
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
      if (prevClusterId.isDefined) {
        stream.println(s"ClusterId: ${prevClusterId.get}")
      }
      if (prevKip500.isDefined) {
        stream.println(s"KIP-500 mode: ${prevKip500.get}")
      }
      stream.println("")
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

  def format(stream: PrintStream, directories: Seq[String], force: Boolean): Int = {
    stream.println(s"force ${directories} ${force}")
    0
  }
}
