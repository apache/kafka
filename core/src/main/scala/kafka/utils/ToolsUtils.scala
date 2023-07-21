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
package kafka.utils

import joptsimple.OptionParser
import kafka.server.{BrokerMetadataCheckpoint, MetaProperties}
import kafka.tools.TerseFailure
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{append, store, storeTrue}
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.common.metadata.{FeatureLevelRecord, UserScramCredentialRecord}
import org.apache.kafka.common.security.scram.internals.{ScramFormatter, ScramMechanism}
import org.apache.kafka.common.{Metric, MetricName}
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.util.CommandLineUtils

import java.io.PrintStream
import java.nio.file.{Files, Paths}
import java.util
import java.util.{Base64, Optional}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

object ToolsUtils {

  def validatePortOrDie(parser: OptionParser, hostPort: String): Unit = {
    val hostPorts: Array[String] = if(hostPort.contains(','))
      hostPort.split(",")
    else
      Array(hostPort)
    val validHostPort = hostPorts.filter { hostPortData =>
      org.apache.kafka.common.utils.Utils.getPort(hostPortData) != null
    }
    val isValid = !validHostPort.isEmpty && validHostPort.size == hostPorts.length
    if (!isValid)
      CommandLineUtils.printUsageAndExit(parser, "Please provide valid host:port like host1:9091,host2:9092\n ")
  }

  /**
    * print out the metrics in alphabetical order
    * @param metrics  the metrics to be printed out
    */
  def printMetrics(metrics: mutable.Map[MetricName, _ <: Metric]): Unit = {
    var maxLengthOfDisplayName = 0

    val sortedMap = metrics.toSeq.sortWith( (s,t) =>
      Array(s._1.group(), s._1.name(), s._1.tags()).mkString(":")
        .compareTo(Array(t._1.group(), t._1.name(), t._1.tags()).mkString(":")) < 0
    ).map {
      case (key, value) =>
        val mergedKeyName = Array(key.group(), key.name(), key.tags()).mkString(":")
        if (maxLengthOfDisplayName < mergedKeyName.length) {
          maxLengthOfDisplayName = mergedKeyName.length
        }
        (mergedKeyName, value.metricValue)
    }
    println(s"\n%-${maxLengthOfDisplayName}s   %s".format("Metric Name", "Value"))
    sortedMap.foreach {
      case (metricName, value) =>
        val specifier = value match {
          case _ @ (_: java.lang.Float | _: java.lang.Double) => "%.3f"
          case _ => "%s"
        }
        println(s"%-${maxLengthOfDisplayName}s : $specifier".format(metricName, value))
    }
  }

  /**
   * This is a simple wrapper around `CommandLineUtils.printUsageAndExit`.
   * It is needed for tools migration (KAFKA-14525), as there is no Java equivalent for return type `Nothing`.
   * Can be removed once [[kafka.admin.ConsumerGroupCommand]], [[kafka.tools.ConsoleConsumer]]
   * and [[kafka.tools.ConsoleProducer]] are migrated.
   *
   * @param parser Command line options parser.
   * @param message Error message.
   */
  def printUsageAndExit(parser: OptionParser, message: String): Nothing = {
    CommandLineUtils.printUsageAndExit(parser, message)
    throw new AssertionError("printUsageAndExit should not return, but it did.")
  }

  def buildBootstrapMetadata(metadataVersion: MetadataVersion,
                             metadataOptionalArguments: Option[ArrayBuffer[ApiMessageAndVersion]],
                             source: String): BootstrapMetadata = {

    val metadataRecords = new util.ArrayList[ApiMessageAndVersion]
    metadataRecords.add(new ApiMessageAndVersion(new FeatureLevelRecord().
      setName(MetadataVersion.FEATURE_NAME).
      setFeatureLevel(metadataVersion.featureLevel()), 0.toShort));

    metadataOptionalArguments.foreach { metadataArguments =>
      for (record <- metadataArguments) metadataRecords.add(record)
    }

    BootstrapMetadata.fromRecords(metadataRecords, source)
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
      help(
        """A SCRAM_CREDENTIAL to add to the __cluster_metadata log e.g.
          |'SCRAM-SHA-256=[name=alice,password=alice-secret]'
          |'SCRAM-SHA-512=[name=alice,iterations=8192,salt="N3E=",saltedpassword="YCE="]'""".stripMargin)
    formatParser.addArgument("--ignore-formatted", "-g").
      action(storeTrue())
    formatParser.addArgument("--release-version", "-r").
      action(store()).
      help(s"A KRaft release version to use for the initial metadata version. The minimum is 3.0, the default is ${MetadataVersion.latest().version()}")

    parser.parseArgsOrFail(args)
  }

  def getUserScramCredentialRecords(namespace: Namespace): Option[ArrayBuffer[UserScramCredentialRecord]] = {
    if (namespace.getList("add_scram") != null) {
      val listofAddConfig : List[String] = namespace.getList("add_scram").asScala.toList
      val userScramCredentialRecords: ArrayBuffer[UserScramCredentialRecord] = ArrayBuffer()
      for (singleAddConfig <- listofAddConfig) {
        val singleAddConfigList = singleAddConfig.split("\\s+")

        // The first subarg must be of the form key=value
        val nameValueRecord = singleAddConfigList(0).split("=", 2)
        nameValueRecord(0) match {
          case "SCRAM-SHA-256" =>
            userScramCredentialRecords.append(getUserScramCredentialRecord(nameValueRecord(0), nameValueRecord(1)))
          case "SCRAM-SHA-512" =>
            userScramCredentialRecords.append(getUserScramCredentialRecord(nameValueRecord(0), nameValueRecord(1)))
          case _ => throw new TerseFailure(s"The add-scram mechanism ${nameValueRecord(0)} is not supported.")
        }
      }
      Some(userScramCredentialRecords)
    } else {
      None
    }
  }

  def getUserScramCredentialRecord(
                                    mechanism: String,
                                    config: String
                                  ): UserScramCredentialRecord = {
    /*
     * Remove  '[' amd ']'
     * Split K->V pairs on ',' and no K or V should contain ','
     * Split K and V on '=' but V could contain '=' if inside ""
     * Create Map of K to V and replace all " in V
     */
    val argMap = config.substring(1, config.length - 1)
      .split(",")
      .map(_.split("=(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .map(args => args(0) -> args(1).replaceAll("\"", "")).toMap

    val scramMechanism = ScramMechanism.forMechanismName(mechanism)

    def getName(argMap: Map[String, String]): String = {
      if (!argMap.contains("name")) {
        throw new TerseFailure(s"You must supply 'name' to add-scram")
      }
      argMap("name")
    }

    def getSalt(argMap: Map[String, String], scramMechanism: ScramMechanism): Array[Byte] = {
      if (argMap.contains("salt")) {
        Base64.getDecoder.decode(argMap("salt"))
      } else {
        new ScramFormatter(scramMechanism).secureRandomBytes()
      }
    }

    def getIterations(argMap: Map[String, String], scramMechanism: ScramMechanism): Int = {
      if (argMap.contains("salt")) {
        val iterations = argMap("iterations").toInt
        if (iterations < scramMechanism.minIterations()) {
          throw new TerseFailure(s"The 'iterations' value must be >= ${scramMechanism.minIterations()} for add-scram")
        }
        if (iterations > scramMechanism.maxIterations()) {
          throw new TerseFailure(s"The 'iterations' value must be <= ${scramMechanism.maxIterations()} for add-scram")
        }
        iterations
      } else {
        4096
      }
    }

    def getSaltedPassword(
                           argMap: Map[String, String],
                           scramMechanism: ScramMechanism,
                           salt: Array[Byte],
                           iterations: Int
                         ): Array[Byte] = {
      if (argMap.contains("password")) {
        if (argMap.contains("saltedpassword")) {
          throw new TerseFailure(s"You must only supply one of 'password' or 'saltedpassword' to add-scram")
        }
        new ScramFormatter(scramMechanism).saltedPassword(argMap("password"), salt, iterations)
      } else {
        if (!argMap.contains("saltedpassword")) {
          throw new TerseFailure(s"You must supply one of 'password' or 'saltedpassword' to add-scram")
        }
        if (!argMap.contains("salt")) {
          throw new TerseFailure(s"You must supply 'salt' with 'saltedpassword' to add-scram")
        }
        Base64.getDecoder.decode(argMap("saltedpassword"))
      }
    }

    val name = getName(argMap)
    val salt = getSalt(argMap, scramMechanism)
    val iterations = getIterations(argMap, scramMechanism)
    val saltedPassword = getSaltedPassword(argMap, scramMechanism, salt, iterations)

    val myrecord = try {
      val formatter = new ScramFormatter(scramMechanism);

      new UserScramCredentialRecord()
        .setName(name)
        .setMechanism(scramMechanism.`type`)
        .setSalt(salt)
        .setStoredKey(formatter.storedKey(formatter.clientKey(saltedPassword)))
        .setServerKey(formatter.serverKey(saltedPassword))
        .setIterations(iterations)
    } catch {
      case e: Throwable =>
        throw new TerseFailure(s"Error attempting to create UserScramCredentialRecord: ${e.getMessage}")
    }
    myrecord
  }

  def formatCommand(
                     stream: PrintStream,
                     directories: Seq[String],
                     metaProperties: MetaProperties,
                     metadataVersion: MetadataVersion,
                     ignoreFormatted: Boolean
                   ): Int = {
    val bootstrapMetadata = buildBootstrapMetadata(metadataVersion, None, "format command")
    formatCommand(stream, directories, metaProperties, bootstrapMetadata, metadataVersion, ignoreFormatted)
  }

  def formatCommand(
                     stream: PrintStream,
                     directories: Seq[String],
                     metaProperties: MetaProperties,
                     bootstrapMetadata: BootstrapMetadata,
                     metadataVersion: MetadataVersion,
                     ignoreFormatted: Boolean
                   ): Int = {
    if (directories.isEmpty) {
      throw new TerseFailure("No log directories found in the configuration.")
    }

    val unformattedDirectories = directories.filter(directory => {
      if (!Files.isDirectory(Paths.get(directory)) || !Files.exists(Paths.get(directory, "meta.properties"))) {
        true
      } else if (!ignoreFormatted) {
        throw new TerseFailure(s"Log directory $directory is already formatted. " +
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
          s"directory $directory: ${e.getMessage}")
      }
      val metaPropertiesPath = Paths.get(directory, "meta.properties")
      val checkpoint = new BrokerMetadataCheckpoint(metaPropertiesPath.toFile)
      checkpoint.write(metaProperties.toProperties)

      val bootstrapDirectory = new BootstrapDirectory(directory, Optional.empty())
      bootstrapDirectory.writeBinaryFile(bootstrapMetadata)

      stream.println(s"Formatting ${directory} with metadata.version ${metadataVersion}.")
    })
    0
  }
}
