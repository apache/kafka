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
        val mountedConfigsDir = arguments(1)
        val finalConfigsDir = arguments(2)
        prepareConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir)

        val formatCmd = formatStorageCmd(finalConfigsDir)
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

  private def prepareConfigs(defaultConfigsDir: String, mountedConfigsDir: String, finalConfigsDir: String): Unit = {
    prepareServerConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir)
    prepareLog4jConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir)
    prepareToolsLog4jConfigs(defaultConfigsDir, mountedConfigsDir, finalConfigsDir)
  }

  private def prepareServerConfigs(defaultConfigsDir: String,
                                   mountedConfigsDir: String,
                                   finalConfigsDir: String): Unit = {
    val defaultFilePath = s"$defaultConfigsDir/$ServerPropsFilename"
    val mountedFilePath = s"$mountedConfigsDir/$ServerPropsFilename"
    val finalFilePath = s"$finalConfigsDir/$ServerPropsFilename"

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

    val propsToAdd = "\n" + serverPropsList.mkString("\n")
    if (Files.exists(Paths.get(mountedFilePath))) {
      copyFile(mountedFilePath, finalFilePath)
      addToFile(propsToAdd, finalFilePath)
    } else {
        addToFile(propsToAdd, finalFilePath, StandardOpenOption.TRUNCATE_EXISTING)
    }

    val source = scala.io.Source.fromFile(finalFilePath)
    val data = try source.mkString finally source.close()
    if (data.trim.isEmpty) {
      copyFile(defaultFilePath, finalFilePath)
    }
  }

  private def prepareLog4jConfigs(defaultConfigsDir: String,
                                  mountedConfigsDir: String,
                                  finalConfigsDir: String): Unit = {
    val defaultFilePath = s"$defaultConfigsDir/$Log4jPropsFilename"
    val mountedFilePath = s"$mountedConfigsDir/$Log4jPropsFilename"
    val finalFilePath = s"$finalConfigsDir/$Log4jPropsFilename"

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    val kafkaLog4jRootLogLevelProp = sys.env.get(KafkaLog4jRootLoglevelEnv)
      .map(kafkaLog4jRootLogLevel => s"log4j.rootLogger=$kafkaLog4jRootLogLevel, stdout")
      .getOrElse("")

    val kafkaLog4jLoggersProp = sys.env.get(KafkaLog4JLoggersEnv)
      .map(kafkaLog4JLoggersString => kafkaLog4JLoggersString.split(",").mkString("\n"))
      .getOrElse("")

    val propsToAdd = "\n" + kafkaLog4jRootLogLevelProp + "\n" + kafkaLog4jLoggersProp
    addToFile(propsToAdd, finalFilePath)
  }

  private def prepareToolsLog4jConfigs(defaultConfigsDir: String,
                                       mountedConfigsDir: String,
                                       finalConfigsDir: String): Unit = {
    val defaultFilePath = s"$defaultConfigsDir/$ToolsLog4jFilename"
    val mountedFilePath = s"$mountedConfigsDir/$ToolsLog4jFilename"
    val finalFilePath = s"$finalConfigsDir/$ToolsLog4jFilename"

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    sys.env.get(KafkaToolsLog4jLoglevelEnv).foreach { kafkaToolsLog4jLogLevel =>
      val propToAdd = "\n" + s"log4j.rootLogger=$kafkaToolsLog4jLogLevel, stderr"
      addToFile(propToAdd, finalFilePath)
    }
  }

  private def addToFile(properties: String, filepath: String, option: StandardOpenOption = StandardOpenOption.APPEND): Unit = {
    val path = Paths.get(filepath)
    if (!Files.exists(path)) {
      Files.createFile(path)
    }
    Files.write(Paths.get(filepath), properties.getBytes, option)
  }

  private def copyFile(source: String, destination: String) = {
    if (Files.exists(Paths.get(source))) {
      Files.copy(Paths.get(source), Paths.get(destination), StandardCopyOption.REPLACE_EXISTING)
    }
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
