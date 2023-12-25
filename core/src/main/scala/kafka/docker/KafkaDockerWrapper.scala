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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardCopyOption, StandardOpenOption}

object KafkaDockerWrapper {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      throw new RuntimeException(s"Error: No operation input provided. " +
        s"Please provide a valid operation: 'setup'.")
    }
    val operation = args.head
    val arguments = args.tail

    operation match {
      case "setup" =>
        if (arguments.length != 3) {
          val errMsg = "not enough arguments passed. Usage: " +
            "setup <default-configs-dir> <mounted-configs-dir>, <final-configs-dir>"
          System.err.println(errMsg)
          Exit.exit(1, Some(errMsg))
        }
        val defaultConfigsDir = arguments(0)
        val mountedConfigsDir = arguments(1)
        val finalConfigsDir = arguments(2)
        try {
          prepareConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir)
        } catch {
          case e: Throwable =>
            val errMsg = s"error while preparing configs: ${e.getMessage}"
            System.err.println(errMsg)
            Exit.exit(1, Some(errMsg))
        }

        val formatCmd = formatStorageCmd(finalConfigsDir, envVars)
        StorageTool.main(formatCmd)
      case _ =>
        throw new RuntimeException(s"Unknown operation $operation. " +
          s"Please provide a valid operation: 'setup'.")
    }
  }

  import Constants._

  private def formatStorageCmd(configsDir: String, env: Map[String, String]): Array[String] = {
    Array("format", "--cluster-id=" + env.get("CLUSTER_ID"), "-c", s"$configsDir/server.properties", "--ignore-formatted")
  }

  private def prepareConfigs(defaultConfigsDir: String, mountedConfigsDir: String, finalConfigsDir: String): Unit = {
    prepareServerConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir, envVars)
    prepareLog4jConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir, envVars)
    prepareToolsLog4jConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir, envVars)
  }

  private[docker] def prepareServerConfigs(defaultConfigsDir: String,
                                   mountedConfigsDir: String,
                                   finalConfigsDir: String,
                                   env: Map[String, String]): Unit = {
    val propsToAdd = addNewlinePadding(getServerConfigsFromEnv(env).mkString(NewlineChar))

    val defaultFilePath = s"$defaultConfigsDir/$ServerPropsFilename"
    val mountedFilePath = s"$mountedConfigsDir/$ServerPropsFilename"
    val finalFilePath = s"$finalConfigsDir/$ServerPropsFilename"

    if (Files.exists(Paths.get(mountedFilePath))) {
      copyFile(mountedFilePath, finalFilePath)
      addToFile(propsToAdd, finalFilePath, StandardOpenOption.APPEND)
    } else {
      addToFile(propsToAdd, finalFilePath, StandardOpenOption.TRUNCATE_EXISTING)
    }

    val source = scala.io.Source.fromFile(finalFilePath)
    val data = try source.mkString finally source.close()
    if (data.trim.isEmpty) {
      copyFile(defaultFilePath, finalFilePath)
    }
  }

  private[docker] def prepareLog4jConfigs(defaultConfigsDir: String,
                                  mountedConfigsDir: String,
                                  finalConfigsDir: String,
                                  env: Map[String, String]): Unit = {
    val propsToAdd = getLog4jConfigsFromEnv(env)

    val defaultFilePath = s"$defaultConfigsDir/$Log4jPropsFilename"
    val mountedFilePath = s"$mountedConfigsDir/$Log4jPropsFilename"
    val finalFilePath = s"$finalConfigsDir/$Log4jPropsFilename"

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    addToFile(propsToAdd, finalFilePath, StandardOpenOption.APPEND)
  }

  private[docker] def prepareToolsLog4jConfigs(defaultConfigsDir: String,
                                       mountedConfigsDir: String,
                                       finalConfigsDir: String,
                                       env: Map[String, String]): Unit = {
    val propToAdd = getToolsLog4jConfigsFromEnv(env)

    val defaultFilePath = s"$defaultConfigsDir/$ToolsLog4jFilename"
    val mountedFilePath = s"$mountedConfigsDir/$ToolsLog4jFilename"
    val finalFilePath = s"$finalConfigsDir/$ToolsLog4jFilename"

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
      kafkaLog4JLoggersString => kafkaLog4JLoggersString.split(",")
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

  private def addToFile(properties: String, filepath: String, mode: StandardOpenOption): Unit = {
    val path = Paths.get(filepath)
    if (!Files.exists(path)) {
      Files.createFile(path)
    }
    Files.write(Paths.get(filepath), properties.getBytes(StandardCharsets.UTF_8), mode)
  }

  private def copyFile(source: String, destination: String) = {
    if (Files.exists(Paths.get(source))) {
      Files.copy(Paths.get(source), Paths.get(destination), StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def addNewlinePadding(str: String) : String = {
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
