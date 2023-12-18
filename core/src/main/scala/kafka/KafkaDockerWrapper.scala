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

import java.io.FileWriter
import java.nio.file.{Files, Paths, StandardCopyOption}

object KafkaDockerWrapper {

  private def prepareConfigs(defaultConfigsDir: String, realConfigsDir: String): Unit = {
    prepareServerProperties(defaultConfigsDir, realConfigsDir)
    prepareLog4jProperties(realConfigsDir)
    prepareToolsProperties(realConfigsDir)
  }

  private def prepareToolsProperties(realConfigsDir: String): Unit = {
    var result = ""
    if (sys.env.contains("KAFKA_TOOLS_LOG4J_LOGLEVEL")) {
      result += "\n" + "log4j.rootLogger=" + sys.env.get("KAFKA_TOOLS_LOG4J_LOGLEVEL") + ", stderr"
    }

    val realFile = realConfigsDir + "/tools-log4j.properties"
    val fw = new FileWriter(realFile, true)
    try {
      fw.write(result)
    }
    finally fw.close()
  }

  private def prepareLog4jProperties(realConfigsDir: String): Unit = {
    var result = ""
    if (sys.env.contains("KAFKA_LOG4J_ROOT_LOGLEVEL")) {
      result += "\n" + "log4j.rootLogger=" + sys.env.get("KAFKA_LOG4J_ROOT_LOGLEVEL") + ", stdout"
    }
    if (sys.env.contains("KAFKA_LOG4J_LOGGERS")) {
      val loggers = sys.env.getOrElse("KAFKA_LOG4J_LOGGERS", "")
      if (loggers.nonEmpty) {
        val arrLoggers =  loggers.split(",")
        for (i <- 0 until arrLoggers.length) {
          result += "\n" + arrLoggers(i)
        }
      }
    }
    val realFile = realConfigsDir + "/log4j.properties"
    val fw = new FileWriter(realFile, true)
    try {
      fw.write(result)
    }
    finally fw.close()
  }

  private def prepareServerProperties(defaultConfigsDir: String, realConfigsDir: String): Unit = {
    val exclude = Set("KAFKA_VERSION",
      "KAFKA_HEAP_OPT",
      "KAFKA_LOG4J_OPTS",
      "KAFKA_OPTS",
      "KAFKA_JMX_OPTS",
      "KAFKA_JVM_PERFORMANCE_OPTS",
      "KAFKA_GC_LOG_OPTS",
      "KAFKA_LOG4J_ROOT_LOGLEVEL",
      "KAFKA_LOG4J_LOGGERS",
      "KAFKA_TOOLS_LOG4J_LOGLEVEL")
    var result = ""
    for ((key, value) <- sys.env) {
      if (key.startsWith("KAFKA_") && !exclude.contains(key)) {
        val final_key = key.replace("KAFKA_", "").toLowerCase().replace("_", ".").replace("...", "-").replace("..", "_")
        result += "\n" + final_key + "=" + value
      }
    }
    val realFile = realConfigsDir + "/server.properties"
    val defaultFile = defaultConfigsDir + "/server.properties"
    val path = Paths.get(realFile)
    if (!Files.exists(path)) {
      Files.createFile(path)
    }
    val fw = new FileWriter(realFile, true)
    try {
      fw.write(result)
    }
    finally fw.close()

    val source = scala.io.Source.fromFile(realFile)
    val data = try source.mkString finally source.close()
    if (data.trim().isEmpty) {
      Files.copy(Paths.get(defaultFile), path, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  private def formatStorageCmd(configsDir: String): Array[String] = {
    Array("format", "--cluster-id=" + sys.env.get("CLUSTER_ID"), "-c", "/opt/kafka/config/server.properties")
  }

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
        prepareConfigs(defaultConfigsDir, realConfigsDir)

        val formatCmd = formatStorageCmd(realConfigsDir)
        StorageTool.main(formatCmd)
      case _ =>
        throw new RuntimeException(s"Unknown operation $operation. " +
          s"Please provide a valid operation: 'setup'.")
    }
  }
}
