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
package kafka.docker

import kafka.tools.StorageTool
import kafka.utils.Exit
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.store
import net.sourceforge.argparse4j.inf.Namespace

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption, StandardOpenOption}

object KafkaDockerWrapper {
  def main(args: Array[String]): Unit = {
    val namespace = parseArguments(args)
    val command = namespace.getString("command")
    command match {
      case "setup" =>
        val defaultConfigsPath = Paths.get(namespace.getString("default_configs_dir"))
        val mountedConfigsPath = Paths.get(namespace.getString("mounted_configs_dir"))
        val finalConfigsPath = Paths.get(namespace.getString("final_configs_dir"))
        try {
          prepareConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath)
        } catch {
          case e: Throwable =>
            val errMsg = s"error while preparing configs: ${e.getMessage}"
            System.err.println(errMsg)
            Exit.exit(1, Some(errMsg))
        }

        val formatCmd = formatStorageCmd(finalConfigsPath, envVars)
        StorageTool.main(formatCmd)
      case _ =>
        throw new RuntimeException(s"Unknown operation $command. " +
          s"Please provide a valid operation: 'setup'.")
    }
  }

  import Constants._

  private def parseArguments(args: Array[String]): Namespace = {
    val parser = ArgumentParsers.
      newArgumentParser("kafka-docker-wrapper", true, "-", "@").
      description("The Kafka docker wrapper.")

    val subparsers = parser.addSubparsers().dest("command")

    val setupParser = subparsers.addParser("setup")

    setupParser.addArgument("--default-configs-dir", "-D").
      action(store()).
      required(true).
      help(
        """Directory which holds default properties. It should contain the three file:-
          |server.properties, log4j.properties and tools-log4j.properties.
          |""".stripMargin)

    setupParser.addArgument("--mounted-configs-dir", "-M").
      action(store()).
      required(true).
      help(
        """Directory which holds user mounted properties. It can contain none to all the three files:-
          |server.properties, log4j.properties and tools-log4j.properties.""".stripMargin)

    setupParser.addArgument("--final-configs-dir", "-F").
      action(store()).
      required(true).
      help(
        """Directory which holds final properties. It holds the final properties that will be used to boot kafka.
          |""".stripMargin)

    parser.parseArgsOrFail(args)
  }

  private def formatStorageCmd(configsPath: Path, env: Map[String, String]): Array[String] = {
    Array("format", "--cluster-id=" + env.get("CLUSTER_ID"), "-c", s"${configsPath.toString}/server.properties")
  }

  private def prepareConfigs(defaultConfigsPath: Path, mountedConfigsPath: Path, finalConfigsPath: Path): Unit = {
    prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)
    prepareLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)
    prepareToolsLog4jConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)
  }

  private[docker] def prepareServerConfigs(defaultConfigsPath: Path,
                                           mountedConfigsPath: Path,
                                           finalConfigsPath: Path,
                                           env: Map[String, String]): Unit = {
    val propsToAdd = addNewlinePadding(getServerConfigsFromEnv(env).mkString(NewlineChar))

    val defaultFilePath = defaultConfigsPath.resolve(s"$ServerPropsFilename")
    val mountedFilePath = mountedConfigsPath.resolve(s"$ServerPropsFilename")
    val finalFilePath = finalConfigsPath.resolve(s"$ServerPropsFilename")

    if (Files.exists(mountedFilePath)) {
      copyFile(mountedFilePath, finalFilePath)
      addToFile(propsToAdd, finalFilePath, StandardOpenOption.APPEND)
    } else {
      addToFile(propsToAdd, finalFilePath, StandardOpenOption.TRUNCATE_EXISTING)
    }

    val source = scala.io.Source.fromFile(finalFilePath.toString)
    val data = try source.mkString finally source.close()
    if (data.trim.isEmpty) {
      copyFile(defaultFilePath, finalFilePath)
    }
  }

  private[docker] def prepareLog4jConfigs(defaultConfigsPath: Path,
                                          mountedConfigsPath: Path,
                                          finalConfigsPath: Path,
                                          env: Map[String, String]): Unit = {
    val propsToAdd = getLog4jConfigsFromEnv(env)

    val defaultFilePath = defaultConfigsPath.resolve(s"$Log4jPropsFilename")
    val mountedFilePath = mountedConfigsPath.resolve(s"$Log4jPropsFilename")
    val finalFilePath = finalConfigsPath.resolve(s"$Log4jPropsFilename")

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    addToFile(propsToAdd, finalFilePath, StandardOpenOption.APPEND)
  }

  private[docker] def prepareToolsLog4jConfigs(defaultConfigsPath: Path,
                                               mountedConfigsPath: Path,
                                               finalConfigsPath: Path,
                                               env: Map[String, String]): Unit = {
    val propToAdd = getToolsLog4jConfigsFromEnv(env)

    val defaultFilePath = defaultConfigsPath.resolve(s"$ToolsLog4jFilename")
    val mountedFilePath = mountedConfigsPath.resolve(s"$ToolsLog4jFilename")
    val finalFilePath = finalConfigsPath.resolve(s"$ToolsLog4jFilename")

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    addToFile(propToAdd, finalFilePath, StandardOpenOption.APPEND)
  }

  private[docker] def getServerConfigsFromEnv(env: Map[String, String]): List[String] = {
    env.map {
        case (key, value) =>
          if (key.startsWith("KAFKA_") && !ExcludeServerPropsEnv.contains(key)) {
            val final_key = key.replace("KAFKA_", "").toLowerCase()
              .replace("_", ".")
              .replace("...", "-")
              .replace("..", "_")
            final_key + "=" + value
          } else {
            ""
          }
      }
      .toList
      .filterNot(_.trim.isEmpty)
  }

  private[docker] def getLog4jConfigsFromEnv(env: Map[String, String]): String = {
    val kafkaLog4jRootLogLevelProp = env.get(KafkaLog4jRootLoglevelEnv)
      .filter(_.nonEmpty)
      .map(kafkaLog4jRootLogLevel => s"log4j.rootLogger=$kafkaLog4jRootLogLevel, stdout")
      .getOrElse("")

    val kafkaLog4jLoggersProp = env.get(KafkaLog4JLoggersEnv)
      .filter(_.nonEmpty)
      .map {
        kafkaLog4JLoggersString =>
          kafkaLog4JLoggersString.split(",")
            .map(kafkaLog4JLogger => s"log4j.logger.$kafkaLog4JLogger")
            .mkString(NewlineChar)
      }.getOrElse("")

    addNewlinePadding(kafkaLog4jRootLogLevelProp) + addNewlinePadding(kafkaLog4jLoggersProp)
  }

  private[docker] def getToolsLog4jConfigsFromEnv(env: Map[String, String]): String = {
    env.get(KafkaToolsLog4jLoglevelEnv)
      .filter(_.nonEmpty)
      .map(kafkaToolsLog4jLogLevel => addNewlinePadding(s"log4j.rootLogger=$kafkaToolsLog4jLogLevel, stderr"))
      .getOrElse("")
  }

  private def addToFile(properties: String, filepath: Path, mode: StandardOpenOption): Unit = {
    val path = filepath
    if (!Files.exists(path)) {
      Files.createFile(path)
    }
    Files.write(filepath, properties.getBytes(StandardCharsets.UTF_8), mode)
  }

  private def copyFile(source: Path, destination: Path) = {
    if (Files.exists(source)) {
      Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def addNewlinePadding(str: String): String = {
    if (str.nonEmpty) {
      NewlineChar + str
    } else {
      ""
    }
  }

  private def envVars: Map[String, String] = sys.env
}

private object Constants {
  val ServerPropsFilename = "server.properties"
  val Log4jPropsFilename = "log4j.properties"
  val ToolsLog4jFilename = "tools-log4j.properties"
  val KafkaLog4JLoggersEnv = "KAFKA_LOG4J_LOGGERS"
  val KafkaLog4jRootLoglevelEnv = "KAFKA_LOG4J_ROOT_LOGLEVEL"
  val KafkaToolsLog4jLoglevelEnv = "KAFKA_TOOLS_LOG4J_LOGLEVEL"
  val ExcludeServerPropsEnv: Set[String] = Set(
    "KAFKA_VERSION",
    "KAFKA_HEAP_OPT",
    "KAFKA_LOG4J_OPTS",
    "KAFKA_OPTS",
    "KAFKA_JMX_OPTS",
    "KAFKA_JVM_PERFORMANCE_OPTS",
    "KAFKA_GC_LOG_OPTS",
    "KAFKA_LOG4J_ROOT_LOGLEVEL",
    "KAFKA_LOG4J_LOGGERS",
    "KAFKA_TOOLS_LOG4J_LOGLEVEL",
    "KAFKA_JMX_HOSTNAME")
  val NewlineChar = "\n"
}
