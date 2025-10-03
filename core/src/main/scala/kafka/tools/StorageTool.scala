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

import kafka.server.KafkaConfig

import java.io.PrintStream
import java.nio.file.{Files, Paths}
import kafka.utils.{Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{append, store, storeTrue}
import net.sourceforge.argparse4j.inf.{ArgumentParserException, Namespace}
import net.sourceforge.argparse4j.internal.HelpScreenException
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.metadata.storage.{Formatter, FormatterException}
import org.apache.kafka.raft.{DynamicVoters, QuorumConfig}
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.config.ReplicationConfigs

import java.util
import scala.collection.mutable

object StorageTool extends Logging {

  def main(args: Array[String]): Unit = {
    var exitCode: Integer = 0
    var message: Option[String] = None
    try {
      exitCode = execute(args, System.out)
    } catch {
      case e: FormatterException =>
        exitCode = 1
        message = Some(e.getMessage)
      case e: TerseFailure =>
        exitCode = 1
        message = Some(e.getMessage)
    }
    message.foreach(System.err.println)
    Exit.exit(exitCode, message)
  }

  /**
   * Executes the command according to the given arguments and returns the appropriate exit code.
   * @param args The command line arguments
   * @return     The exit code
   */
  def execute(
     args: Array[String],
     printStream: PrintStream
  ): Int = {
    val namespace = try {
      parseArguments(args)
    } catch {
      case _: HelpScreenException =>
        return 0
      case e: ArgumentParserException =>
        e.getParser.handleError(e)
        return 1
    }
    val command = namespace.getString("command")
    val config = Option(namespace.getString("config")).flatMap(
      p => Some(new KafkaConfig(Utils.loadProps(p))))
    command match {
      case "info" =>
        val directories = configToLogDirectories(config.get)
        infoCommand(printStream, config.get.processRoles.nonEmpty, directories)

      case "format" =>
        runFormatCommand(namespace, config.get, printStream)
        0

      case "random-uuid" =>
        printStream.println(Uuid.randomUuid)
        0
      case _ =>
        throw new RuntimeException(s"Unknown command $command")
    }
  }

  /**
   * Validates arguments, configuration, prepares bootstrap metadata and delegates to {{@link formatCommand}}.
   * Visible for testing.
   *
   * @param namespace   Arguments
   * @param config      The server configuration
   * @return            The exit code
   */
  def runFormatCommand(
    namespace: Namespace,
    config: KafkaConfig,
    printStream: PrintStream
  ): Unit = {
    if (config.processRoles.isEmpty) {
      throw new TerseFailure("The kafka configuration file appears to be for " +
        "a legacy cluster. Formatting is only supported for clusters in KRaft mode.")
    }
    val formatter = new Formatter().
      setPrintStream(printStream).
      setNodeId(config.nodeId).
      setClusterId(namespace.getString("cluster_id")).
      setUnstableFeatureVersionsEnabled(config.unstableFeatureVersionsEnabled).
      setIgnoreFormatted(namespace.getBoolean("ignore_formatted")).
      setControllerListenerName(config.controllerListenerNames.head).
      setMetadataLogDirectory(config.metadataLogDir)
    Option(namespace.getString("release_version")) match {
      case Some(releaseVersion) => formatter.setReleaseVersion(MetadataVersion.fromVersionString(releaseVersion))
      case None => Option(config.originals.get(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG)).
        foreach(v => formatter.setReleaseVersion(MetadataVersion.fromVersionString(v.toString)))
    }
    Option(namespace.getList[String]("feature")).foreach(
      featureNamesAndLevels(_).foreach {
        kv => formatter.setFeatureLevel(kv._1, kv._2)
      })
    val initialControllers = namespace.getString("initial_controllers")
    val isStandalone = namespace.getBoolean("standalone")
    val staticVotersEmpty = config.quorumVoters.isEmpty
    formatter.setHasDynamicQuorum(staticVotersEmpty)
    if (!staticVotersEmpty && (Option(initialControllers).isDefined || isStandalone)) {
      throw new TerseFailure("You cannot specify " +
        QuorumConfig.QUORUM_VOTERS_CONFIG + " and format the node " +
        "with --initial-controllers or --standalone. " +
        "If you want to use dynamic quorum, please remove " +
        QuorumConfig.QUORUM_VOTERS_CONFIG + " and specify " +
        QuorumConfig.QUORUM_BOOTSTRAP_SERVERS_CONFIG + " instead.")
    }
    Option(initialControllers).
      foreach(v => formatter.setInitialControllers(DynamicVoters.parse(v)))
    if (isStandalone) {
      formatter.setInitialControllers(createStandaloneDynamicVoters(config))
    }
    if (!namespace.getBoolean("no_initial_controllers") &&
      config.processRoles.contains(ProcessRole.ControllerRole) &&
      staticVotersEmpty &&
      !formatter.initialVoters().isPresent) {
          throw new TerseFailure("Because " + QuorumConfig.QUORUM_VOTERS_CONFIG +
            " is not set on this controller, you must specify one of the following: " +
            "--standalone, --initial-controllers, or --no-initial-controllers.");
    }
    Option(namespace.getList("add_scram")).
      foreach(scramArgs => formatter.setScramArguments(scramArgs.asInstanceOf[util.List[String]]))
    configToLogDirectories(config).foreach(formatter.addDirectory(_))
    formatter.run()
  }

  def createStandaloneDynamicVoters(
    config: KafkaConfig
  ): DynamicVoters = {
    if (!config.processRoles.contains(ProcessRole.ControllerRole)) {
      throw new TerseFailure("You can only use --standalone on a controller.")
    }
    if (config.effectiveAdvertisedControllerListeners.isEmpty) {
      throw new RuntimeException("No controller listeners found.")
    }
    val listener = config.effectiveAdvertisedControllerListeners.head
    val host = if (listener.host == null) {
      "localhost"
    } else {
      listener.host
    }
    DynamicVoters.parse(s"${config.nodeId}@${host}:${listener.port}:${Uuid.randomUuid()}")
  }

  def parseArguments(args: Array[String]): Namespace = {
    val parser = ArgumentParsers.
      newArgumentParser("kafka-storage", /* defaultHelp */ true, /* prefixChars */ "-", /* fromFilePrefix */ "@").
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
    formatParser.addArgument("--add-scram", "-S").
      action(append()).
      help("""A SCRAM_CREDENTIAL to add to the __cluster_metadata log e.g.
              |'SCRAM-SHA-256=[name=alice,password=alice-secret]'
              |'SCRAM-SHA-512=[name=alice,iterations=8192,salt="N3E=",saltedpassword="YCE="]'""".stripMargin)
    formatParser.addArgument("--ignore-formatted", "-g").
      action(storeTrue())
    formatParser.addArgument("--release-version", "-r").
      action(store()).
      help(s"The release version to use for the initial feature settings. The minimum is " +
        s"${MetadataVersion.IBP_3_0_IV0}; the default is ${MetadataVersion.LATEST_PRODUCTION}")
    formatParser.addArgument("--feature", "-f").
      help("The setting to use for a specific feature, in feature=level format. For example: `kraft.version=1`.").
      action(append())
    val reconfigurableQuorumOptions = formatParser.addMutuallyExclusiveGroup()
    reconfigurableQuorumOptions.addArgument("--standalone", "-s")
      .help("Used to initialize a controller as a single-node dynamic quorum. When setting this flag, " +
        "the controller.quorum.voters config must not be set, and controller.quorum.bootstrap.servers is set instead.")
      .action(storeTrue())

    reconfigurableQuorumOptions.addArgument("--no-initial-controllers", "-N")
      .help("Used to initialize a server without specifying a dynamic quorum. When setting this flag, " +
        "the controller.quorum.voters config should not be set, and controller.quorum.bootstrap.servers is set instead.")
      .action(storeTrue())

    reconfigurableQuorumOptions.addArgument("--initial-controllers", "-I")
      .help("Used to initialize a server with the specified dynamic quorum. The argument " +
        "is a comma-separated list of id@hostname:port:directory. The same values must be used to " +
        "format all nodes. For example:\n0@example.com:8082:JEXY6aqzQY-32P5TStzaFg,1@example.com:8083:" +
        "MvDxzVmcRsaTz33bUuRU6A,2@example.com:8084:07R5amHmR32VDA6jHkGbTA\n. When setting this flag, " +
        "the controller.quorum.voters config must not be set, and controller.quorum.bootstrap.servers is set instead.")
      .action(store())
    parser.parseArgs(args)
  }

  def configToLogDirectories(config: KafkaConfig): Seq[String] = {
    val directories = new mutable.TreeSet[String]
    directories ++= config.logDirs
    Option(config.metadataLogDir).foreach(directories.add)
    directories.toSeq
  }

  def infoCommand(stream: PrintStream, kraftMode: Boolean, directories: Seq[String]): Int = {
    val problems = new mutable.ArrayBuffer[String]
    val foundDirectories = new mutable.ArrayBuffer[String]
    var prevMetadata: Option[MetaProperties] = None
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
        val metaPath = directoryPath.resolve(MetaPropertiesEnsemble.META_PROPERTIES_NAME)
        if (!Files.exists(metaPath)) {
          problems += s"$directoryPath is not formatted."
        } else {
          val properties = PropertiesUtils.readPropertiesFile(metaPath.toString)
          try {
            val curMetadata = new MetaProperties.Builder(properties).build()
            if (prevMetadata.isEmpty) {
              prevMetadata = Some(curMetadata)
            } else {
              if (!prevMetadata.get.clusterId().equals(curMetadata.clusterId())) {
                problems += s"Mismatched cluster IDs between storage directories."
              } else if (!prevMetadata.get.nodeId().equals(curMetadata.nodeId())) {
                problems += s"Mismatched node IDs between storage directories."
              }
            }
          } catch {
            case e: Exception =>
              e.printStackTrace(System.out)
              problems += s"Error loading $metaPath: ${e.getMessage}"
          }
        }
      }
    })

    prevMetadata.foreach { prev =>
      if (kraftMode) {
        if (prev.version.equals(MetaPropertiesVersion.V0)) {
          problems += "The kafka configuration file appears to be for a cluster in KRaft mode, but " +
            "the directories are formatted for legacy mode."
        }
      } else if (prev.version.equals(MetaPropertiesVersion.V1)) {
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
        val sortedOutput = new util.TreeMap[String, String]()
        prev.toProperties.entrySet.forEach(e => sortedOutput.put(e.getKey.toString, e.getValue.toString))
        stream.println(s"Found metadata: $sortedOutput")
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

  def parseNameAndLevel(input: String): (String, java.lang.Short) = {
    val equalsIndex = input.indexOf("=")
    if (equalsIndex < 0)
      throw new RuntimeException("Can't parse feature=level string " + input + ": equals sign not found.")
    val name = input.substring(0, equalsIndex).trim
    val levelString = input.substring(equalsIndex + 1).trim
    try {
      (name, levelString.toShort)
    } catch {
      case _: Throwable =>
        throw new RuntimeException("Can't parse feature=level string " + input + ": " + "unable to parse " + levelString + " as a short.")
    }
  }

  def featureNamesAndLevels(features: java.util.List[String]): Map[String, java.lang.Short] = {
    val scalaFeatures = new mutable.ArrayBuffer[String]
    features.forEach(scalaFeatures += _)
    scalaFeatures.map { (feature: String) =>
      // Ensure the feature exists
      val nameAndLevel = parseNameAndLevel(feature)
      (nameAndLevel._1, nameAndLevel._2)
    }.toMap
  }
}
