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
package kafka

import kafka.tools.StorageTool

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
        val defaultConfigsDir = arguments(0)
        val realConfigsDir = arguments(1)
        val mountedDir = arguments(2)
        prepareConfigs(defaultConfigsDir, realConfigsDir, mountedDir)

        val formatCmd = formatStorageCmd(realConfigsDir)
        StorageTool.main(formatCmd)
      case _ =>
        throw new RuntimeException(s"Unknown operation $operation. " +
          s"Please provide a valid operation: 'setup'.")
    }
  }

  import Constants._

  private def formatStorageCmd(configsDir: String): Array[String] = {
    Array("format", "--cluster-id=" + sys.env.get("CLUSTER_ID"), "-c", s"$configsDir/server.properties")
  }

  private def prepareConfigs(defaultConfigsDir: String, realConfigsDir: String, mountedDir: String): Unit = {
    prepareServerConfigs(defaultConfigsDir, realConfigsDir, mountedDir)
    prepareLog4jConfigs(defaultConfigsDir, realConfigsDir, mountedDir)
    prepareToolsLog4jConfigs(defaultConfigsDir, realConfigsDir, mountedDir)
  }

  private def prepareServerConfigs(defaultConfigsDir: String, realConfigsDir: String, mountedDir: String): Unit = {
    val serverPropsList = sys.env.map {
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
    }.toList

    val realFilePath = s"$realConfigsDir/$ServerPropsFilename"
    val defaultFilePath = s"$defaultConfigsDir/$ServerPropsFilename"
    val mountedFilePath = s"$mountedDir/$ServerPropsFilename"

    val propsToAdd = "\n" + serverPropsList.mkString("\n")
    if (Files.exists(Paths.get(mountedFilePath))) {
      copyFile(mountedFilePath, realFilePath)
      addToFile(propsToAdd, realFilePath)
    } else {
        addToFile(propsToAdd, realFilePath, StandardOpenOption.TRUNCATE_EXISTING)
    }

    val source = scala.io.Source.fromFile(realFilePath)
    val data = try source.mkString finally source.close()
    if (data.trim.isEmpty) {
      copyFile(defaultFilePath, realFilePath)
    }
  }

  private def copyFile(source: String, destination: String) = {
    if (Files.exists(Paths.get(source))) {
      Files.copy(Paths.get(source), Paths.get(destination), StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def prepareLog4jConfigs(defaultConfigsDir: String, configsDir: String, mountedDir: String): Unit = {
    val kafkaLog4jRootLogLevelProp = sys.env.get(KafkaLog4jRootLoglevelEnv)
      .map(kafkaLog4jRootLogLevel => s"log4j.rootLogger=$kafkaLog4jRootLogLevel, stdout")
      .getOrElse("")

    val kafkaLog4jLoggersProp = sys.env.get(KafkaLog4JLoggersEnv)
      .map(kafkaLog4JLoggersString => kafkaLog4JLoggersString.split(",").mkString("\n"))
      .getOrElse("")

    val realFilePath = s"$configsDir/$Log4jPropsFilename"
    val mountedFilePath = s"$mountedDir/$Log4jPropsFilename"
    val defaultFilePath = s"$defaultConfigsDir/$Log4jPropsFilename"

    copyFile(defaultFilePath, realFilePath)
    copyFile(mountedFilePath, realFilePath)

    val propsToAdd = "\n" + kafkaLog4jRootLogLevelProp + "\n" + kafkaLog4jLoggersProp
    addToFile(propsToAdd, realFilePath)
  }

  private def prepareToolsLog4jConfigs(defaultConfigsDir: String, configsDir: String, mountedDir: String): Unit = {
    val realFilePath = s"$configsDir/$ToolsLog4jFilename"
    val mountedFilePath = s"$mountedDir/$ToolsLog4jFilename"
    val defaultFilePath = s"$defaultConfigsDir/$ToolsLog4jFilename"

    copyFile(defaultFilePath, realFilePath)
    copyFile(mountedFilePath, realFilePath)

    sys.env.get(KafkaToolsLog4jLoglevelEnv).foreach { kafkaToolsLog4jLogLevel =>
      val propToAdd = "\n" + s"log4j.rootLogger=$kafkaToolsLog4jLogLevel, stderr"
      addToFile(propToAdd, realFilePath)
    }
  }

  private def addToFile(properties: String, filepath: String, option: StandardOpenOption = StandardOpenOption.APPEND): Unit = {
    val path = Paths.get(filepath)
    if (!Files.exists(path)) {
      Files.createFile(path)
    }
    Files.write(Paths.get(filepath), properties.getBytes, option)
  }
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
}
