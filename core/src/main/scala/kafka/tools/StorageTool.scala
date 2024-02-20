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
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.metadata.bootstrap.{BootstrapDirectory, BootstrapMetadata}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.common.metadata.FeatureLevelRecord
import org.apache.kafka.common.metadata.UserScramCredentialRecord
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.scram.internals.ScramFormatter
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}

import java.util
import java.util.Base64
import java.util.Optional
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

object StorageTool extends Logging {
  def main(args: Array[String]): Unit = {
    try {
      val namespace = parseArguments(args)
      val command = namespace.getString("command")
      val config = Option(namespace.getString("config")).flatMap(
        p => Some(new KafkaConfig(Utils.loadProps(p))))
      command match {
        case "info" =>
          val directories = configToLogDirectories(config.get)
          val selfManagedMode = configToSelfManagedMode(config.get)
          Exit.exit(infoCommand(System.out, selfManagedMode, directories))

        case "format" =>
          val directories = configToLogDirectories(config.get)
          val clusterId = namespace.getString("cluster_id")
          val metadataVersion = getMetadataVersion(namespace,
            Option(config.get.originals.get(KafkaConfig.InterBrokerProtocolVersionProp)).map(_.toString))
          if (!metadataVersion.isKRaftSupported) {
            throw new TerseFailure(s"Must specify a valid KRaft metadata version of at least 3.0.")
          }
          if (!metadataVersion.isProduction) {
            if (config.get.unstableMetadataVersionsEnabled) {
              System.out.println(s"WARNING: using pre-production metadata version $metadataVersion.")
            } else {
              throw new TerseFailure(s"Metadata version $metadataVersion is not ready for production use yet.")
            }
          }
          val metaProperties = new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId(clusterId).
            setNodeId(config.get.nodeId).
            build()
          val metadataRecords : ArrayBuffer[ApiMessageAndVersion] = ArrayBuffer()
          getUserScramCredentialRecords(namespace).foreach(userScramCredentialRecords => {
            if (!metadataVersion.isScramSupported) {
              throw new TerseFailure(s"SCRAM is only supported in metadataVersion IBP_3_5_IV2 or later.")
            }
            for (record <- userScramCredentialRecords) {
              metadataRecords.append(new ApiMessageAndVersion(record, 0.toShort))
            }
          })
          val bootstrapMetadata = buildBootstrapMetadata(metadataVersion, Some(metadataRecords), "format command")
          val ignoreFormatted = namespace.getBoolean("ignore_formatted")
          if (!configToSelfManagedMode(config.get)) {
            throw new TerseFailure("The kafka configuration file appears to be for " +
              "a legacy cluster. Formatting is only supported for clusters in KRaft mode.")
          }
          Exit.exit(formatCommand(System.out, directories, metaProperties, bootstrapMetadata,
                                  metadataVersion,ignoreFormatted))

        case "random-uuid" =>
          System.out.println(Uuid.randomUuid)
          Exit.exit(0)

        case _ =>
          throw new RuntimeException(s"Unknown command $command")
      }
    } catch {
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        Exit.exit(1, Some(e.getMessage))
    }
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
      help(s"A KRaft release version to use for the initial metadata version. The minimum is 3.0, the default is ${MetadataVersion.LATEST_PRODUCTION.version()}")

    parser.parseArgsOrFail(args)
  }

  def configToLogDirectories(config: KafkaConfig): Seq[String] = {
    val directories = new mutable.TreeSet[String]
    directories ++= config.logDirs
    Option(config.metadataLogDir).foreach(directories.add)
    directories.toSeq
  }

  private def configToSelfManagedMode(config: KafkaConfig): Boolean = config.processRoles.nonEmpty

  def getMetadataVersion(
    namespace: Namespace,
    defaultVersionString: Option[String]
  ): MetadataVersion = {
    val defaultValue = defaultVersionString match {
      case Some(versionString) => MetadataVersion.fromVersionString(versionString)
      case None => MetadataVersion.LATEST_PRODUCTION
    }

    Option(namespace.getString("release_version"))
      .map(ver => MetadataVersion.fromVersionString(ver))
      .getOrElse(defaultValue)
  }

  private def getUserScramCredentialRecord(
    mechanism: String,
    config: String
  ) : UserScramCredentialRecord = {
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

    def getName(argMap: Map[String,String]) : String = {
      if (!argMap.contains("name")) {
        throw new TerseFailure(s"You must supply 'name' to add-scram")
      }
      argMap("name")
    }

    def getSalt(argMap: Map[String,String], scramMechanism : ScramMechanism) : Array[Byte] = {
      if (argMap.contains("salt")) {
        Base64.getDecoder.decode(argMap("salt"))
      } else {
        new ScramFormatter(scramMechanism).secureRandomBytes()
      }
    }

    def getIterations(argMap: Map[String,String], scramMechanism : ScramMechanism) : Int = {
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
      argMap: Map[String,String],
      scramMechanism : ScramMechanism,
      salt : Array[Byte],
      iterations: Int
    ) : Array[Byte] = {
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
      val formatter = new ScramFormatter(scramMechanism)

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

  def getUserScramCredentialRecords(namespace: Namespace): Option[ArrayBuffer[UserScramCredentialRecord]] = {
    if (namespace.getList("add_scram") != null) {
      val listofAddConfig : List[String] = namespace.getList("add_scram").asScala.toList
      val userScramCredentialRecords : ArrayBuffer[UserScramCredentialRecord] = ArrayBuffer()
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

  def infoCommand(stream: PrintStream, selfManagedMode: Boolean, directories: Seq[String]): Int = {
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
      if (selfManagedMode) {
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

  def buildBootstrapMetadata(metadataVersion: MetadataVersion,
                             metadataOptionalArguments: Option[ArrayBuffer[ApiMessageAndVersion]],
                             source: String): BootstrapMetadata = {

    val metadataRecords = new util.ArrayList[ApiMessageAndVersion]
    metadataRecords.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(metadataVersion.featureLevel()), 0.toShort))

    metadataOptionalArguments.foreach { metadataArguments =>
      for (record <- metadataArguments) metadataRecords.add(record)
    }

    BootstrapMetadata.fromRecords(metadataRecords, source)
  }


  def buildMetadataProperties(
    clusterIdStr: String,
    config: KafkaConfig
  ): MetaProperties = {
    val effectiveClusterId = try {
      Uuid.fromString(clusterIdStr)
    } catch {
      case e: Throwable => throw new TerseFailure(s"Cluster ID string $clusterIdStr " +
        s"does not appear to be a valid UUID: ${e.getMessage}")
    }
    if (config.nodeId < 0) {
      throw new TerseFailure(s"The node.id must be set to a non-negative integer. We saw ${config.nodeId}")
    }
    new MetaProperties.Builder().
      setClusterId(effectiveClusterId.toString).
      setNodeId(config.nodeId).
      build()
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
    val loader = new MetaPropertiesEnsemble.Loader()
    directories.foreach(loader.addLogDir)
    val metaPropertiesEnsemble = loader.load()
    metaPropertiesEnsemble.verify(metaProperties.clusterId(), metaProperties.nodeId(),
      util.EnumSet.noneOf(classOf[VerificationFlag]))

    System.out.println(s"metaPropertiesEnsemble=$metaPropertiesEnsemble")
    val copier = new MetaPropertiesEnsemble.Copier(metaPropertiesEnsemble)
    if (!(ignoreFormatted || copier.logDirProps().isEmpty)) {
      val firstLogDir = copier.logDirProps().keySet().iterator().next()
      throw new TerseFailure(s"Log directory $firstLogDir is already formatted. " +
        "Use --ignore-formatted to ignore this directory and format the others.")
    }
    if (!copier.errorLogDirs().isEmpty) {
      val firstLogDir = copier.errorLogDirs().iterator().next()
      throw new TerseFailure(s"I/O error trying to read log directory $firstLogDir.")
    }
    if (metaPropertiesEnsemble.emptyLogDirs().isEmpty) {
      stream.println("All of the log directories are already formatted.")
    } else {
      metaPropertiesEnsemble.emptyLogDirs().forEach(logDir => {
        copier.setLogDirProps(logDir, new MetaProperties.Builder(metaProperties).
          setDirectoryId(copier.generateValidDirectoryId()).
          build())
        copier.setPreWriteHandler((logDir, _, _) => {
          stream.println(s"Formatting $logDir with metadata.version $metadataVersion.")
          Files.createDirectories(Paths.get(logDir))
          val bootstrapDirectory = new BootstrapDirectory(logDir, Optional.empty())
          bootstrapDirectory.writeBinaryFile(bootstrapMetadata)
        })
        copier.setWriteErrorHandler((logDir, e) => {
          throw new TerseFailure(s"Error while writing meta.properties file $logDir: ${e.getMessage}")
        })
        copier.writeLogDirChanges()
      })
    }
    0
  }
}
