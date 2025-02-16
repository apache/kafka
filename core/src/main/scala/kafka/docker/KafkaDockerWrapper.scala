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

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature
import kafka.Kafka
import kafka.tools.StorageTool
import kafka.utils.Logging
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.store
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.common.utils.Exit

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption, StandardOpenOption}
import scala.jdk.CollectionConverters._

object KafkaDockerWrapper extends Logging {
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
            Exit.exit(1, errMsg)
        }

        val formatCmd = formatStorageCmd(finalConfigsPath, envVars)
        StorageTool.main(formatCmd)
      case "start" =>
        val configFile = namespace.getString("config")
        info("Starting Kafka server in the native mode.")
        Kafka.main(Array(configFile))
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

    val kafkaStartParser = subparsers.addParser("start").help("Start kafka server.")
    kafkaStartParser.addArgument("--config", "-C")
      .action(store())
      .required(true)
      .help("The kafka server configuration file")

    val setupParser = subparsers.addParser("setup").help("Setup property files and format storage.")

    setupParser.addArgument("--default-configs-dir", "-D").
      action(store()).
      required(true).
      help(
        """Directory which holds default properties. It should contain the three file:-
          |server.properties, log4j2.yaml and tools-log4j2.yaml.
          |""".stripMargin)

    setupParser.addArgument("--mounted-configs-dir", "-M").
      action(store()).
      required(true).
      help(
        """Directory which holds user mounted properties. It can contain none to all the three files:-
          |server.properties, log4j2.yaml and tools-log4j2.yaml.""".stripMargin)

    setupParser.addArgument("--final-configs-dir", "-F").
      action(store()).
      required(true).
      help(
        """Directory which holds final properties. It holds the final properties that will be used to boot kafka.
          |""".stripMargin)

    parser.parseArgsOrFail(args)
  }

  private[docker] def formatStorageCmd(configsPath: Path, env: Map[String, String]): Array[String] = {
    val clusterId = env.get("CLUSTER_ID") match {
      case Some(str) => str
      case None => throw new RuntimeException("CLUSTER_ID environment variable is not set.")
    }
    Array("format", "--cluster-id=" + clusterId, "-c", s"${configsPath.toString}/server.properties")
  }

  private def prepareConfigs(defaultConfigsPath: Path, mountedConfigsPath: Path, finalConfigsPath: Path): Unit = {
    prepareServerConfigs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)
    prepareLog4j2Configs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)
    prepareToolsLog4j2Configs(defaultConfigsPath, mountedConfigsPath, finalConfigsPath, envVars)
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

  private[docker] def prepareLog4j2Configs(defaultConfigsPath: Path,
                                           mountedConfigsPath: Path,
                                           finalConfigsPath: Path,
                                           env: Map[String, String]): Unit = {
    val loggerFromEnv = getLog4j2ConfigsFromEnv(env)
    val rootOption = getLog4j2RootConfigsFromEnv(env)

    val defaultFilePath = defaultConfigsPath.resolve(s"$Log4j2PropsFilename")
    val mountedFilePath = mountedConfigsPath.resolve(s"$Log4j2PropsFilename")
    val finalFilePath = finalConfigsPath.resolve(s"$Log4j2PropsFilename")

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    addToYaml(loggerFromEnv, rootOption, finalFilePath)
  }

  private[docker] def prepareToolsLog4j2Configs(defaultConfigsPath: Path,
                                                mountedConfigsPath: Path,
                                                finalConfigsPath: Path,
                                                env: Map[String, String]): Unit = {
    val defaultFilePath = defaultConfigsPath.resolve(s"$ToolsLog4j2Filename")
    val mountedFilePath = mountedConfigsPath.resolve(s"$ToolsLog4j2Filename")
    val finalFilePath = finalConfigsPath.resolve(s"$ToolsLog4j2Filename")

    copyFile(defaultFilePath, finalFilePath)
    copyFile(mountedFilePath, finalFilePath)

    addToYaml(Array.empty, getToolsLog4j2ConfigsFromEnv(env), finalFilePath)
  }

  private[docker] def getServerConfigsFromEnv(env: Map[String, String]): List[String] = {
    env.map {
        case (key, value) =>
          if (key.startsWith("KAFKA_") && !ExcludeServerPropsEnv.contains(key)) {
            val final_key = key.replaceFirst("KAFKA_", "").toLowerCase()
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

  private[docker] def getLog4j2RootConfigsFromEnv(env: Map[String, String]): Option[Root] = {
     env.get(KafkaLog4jRootLoglevelEnv)
      .filter(_.nonEmpty)
      .map(buildRootLogger).getOrElse(Option.empty)
  }

  private[docker] def getToolsLog4j2ConfigsFromEnv(env: Map[String, String]): Option[Root] = {
    env.get(KafkaToolsLog4jLoglevelEnv)
      .filter(_.nonEmpty)
      .map(buildRootLogger).getOrElse(Option.empty)
  }

  private def buildRootLogger(level: String) = {
    val root = new Root
    root.setLevel(level)
    Option.apply(root)
  }

  private[docker] def getLog4j2ConfigsFromEnv(env: Map[String, String]): Array[Logger] = {
    env.get(KafkaLog4JLoggersEnv)
      .filter(_.nonEmpty)
      .map { loggersString =>
        loggersString.split(",").map { e =>
          val parts = e.split("=")
          val logger = new Logger()
          logger.setName(parts(0).trim)
          logger.setLevel(parts(1).trim)
          logger
        }
      }.getOrElse(Array.empty[Logger])
  }

  private def addToFile(properties: String, filepath: Path, mode: StandardOpenOption): Unit = {
    val path = filepath
    if (!Files.exists(path)) {
      Files.createFile(path)
    }
    Files.write(filepath, properties.getBytes(StandardCharsets.UTF_8), mode)
  }

  private def addToYaml(loggerFromEnv: Array[Logger], rootOption: Option[Root], filepath: Path): Unit = {
    val path = filepath
    if (!Files.exists(path)) {
      Files.createFile(path)
    }

    val mapper = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER))
      .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true)
      .setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
      .findAndRegisterModules();

    val yaml = try {
      mapper.readValue(filepath.toFile, classOf[Log4jConfiguration])
    } catch {
      case _: MismatchedInputException => new Log4jConfiguration
      case e: RuntimeException => throw e
    }
    val config = yaml.getConfiguration

    if (config == null && loggerFromEnv.isEmpty && rootOption.isEmpty) {
      return
    }

    if (config == null) {
      generateDefaultLog4jConfig(loggerFromEnv, rootOption, filepath, mapper)
    } else {
      overrideLog4jConfigByEnv(loggerFromEnv, rootOption, filepath, mapper, yaml, config)
    }
  }

  private def generateDefaultLog4jConfig(loggerFromEnv: Array[Logger], rootOption: Option[Root], filepath: Path, mapper: ObjectMapper): Unit = {
    val log4jYaml = new Log4jConfiguration
    val configuration = new Configuration
    val loggers = new Loggers
    val root = if (rootOption.isEmpty) {
      val root = new Root
      // log4j default root logger level
      root.setLevel("ERROR")
      root
    } else rootOption.get
    log4jYaml.setConfiguration(configuration)
    configuration.setLoggers(loggers)
    loggers.setRoot(root)
    loggers.setLogger(loggerFromEnv.toList.asJava)
    Files.write(filepath, mapper.writeValueAsString(log4jYaml).getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING)
  }

  private def overrideLog4jConfigByEnv(loggerFromEnv: Array[Logger],
                                       rootOption: Option[Root],
                                       filepath: Path,
                                       mapper: ObjectMapper,
                                       yaml: Log4jConfiguration,
                                       config: Configuration): Unit = {
    val nameToLoggers = config.getLoggers.getLogger.asScala.map(logger => (logger.getName, logger)).to(collection.mutable.Map)
    loggerFromEnv.foreach(logger => nameToLoggers.put(logger.getName, logger))
    config.getLoggers.setLogger(nameToLoggers.values.toList.asJava)
    if (rootOption.isDefined) {
      config.getLoggers.setRoot(rootOption.get)
    }
    Files.write(filepath, mapper.writeValueAsString(yaml).getBytes(StandardCharsets.UTF_8), StandardOpenOption.TRUNCATE_EXISTING)
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
  val Log4j2PropsFilename = "log4j2.yaml"
  val ToolsLog4j2Filename = "tools-log4j2.yaml"
  val KafkaLog4JLoggersEnv = "KAFKA_LOG4J_LOGGERS"
  val KafkaLog4jRootLoglevelEnv = "KAFKA_LOG4J_ROOT_LOGLEVEL"
  val KafkaToolsLog4jLoglevelEnv = "KAFKA_TOOLS_LOG4J_LOGLEVEL"
  val ExcludeServerPropsEnv: Set[String] = Set(
    "KAFKA_VERSION",
    "KAFKA_HEAP_OPTS",
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
