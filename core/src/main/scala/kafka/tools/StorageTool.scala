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
import org.apache.kafka.server.common.{ApiMessageAndVersion, Features, MetadataVersion}
import org.apache.kafka.common.metadata.FeatureLevelRecord
import org.apache.kafka.common.metadata.UserScramCredentialRecord
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.scram.internals.ScramFormatter
import org.apache.kafka.server.config.ReplicationConfigs
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion, PropertiesUtils}
import org.apache.kafka.server.common.FeatureVersion

import java.util
import java.util.{Base64, Collections, Optional}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

object StorageTool extends Logging {

  def main(args: Array[String]): Unit = {
    var exitCode: Integer = 0
    var message: Option[String] = None
    try {
      exitCode = execute(args)
    } catch {
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
  def execute(args: Array[String]): Int = {
    val namespace = parseArguments(args)
    val command = namespace.getString("command")
    val config = Option(namespace.getString("config")).flatMap(
      p => Some(new KafkaConfig(Utils.loadProps(p))))
    command match {
      case "info" =>
        val directories = configToLogDirectories(config.get)
        val selfManagedMode = configToSelfManagedMode(config.get)
        infoCommand(System.out, selfManagedMode, directories)

      case "format" =>
        runFormatCommand(namespace, config.get)

      case "random-uuid" =>
        System.out.println(Uuid.randomUuid)
        0
      case _ =>
        throw new RuntimeException(s"Unknown command $command")
    }
  }

  /**
   * Validates arguments, configuration, prepares bootstrap metadata and delegates to {{@link formatCommand}}.
   * Visible for testing.
   * @param namespace   Arguments
   * @param config      The server configuration
   * @return            The exit code
   */
  def runFormatCommand(namespace: Namespace, config: KafkaConfig) = {
    val directories = configToLogDirectories(config)
    val clusterId = namespace.getString("cluster_id")
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(clusterId).
      setNodeId(config.nodeId).
      build()
    val metadataRecords : ArrayBuffer[ApiMessageAndVersion] = ArrayBuffer()
    val specifiedFeatures: util.List[String] = namespace.getList("feature")
    val releaseVersionFlagSpecified = namespace.getString("release_version") != null
    if (releaseVersionFlagSpecified && specifiedFeatures != null) {
      throw new TerseFailure("Both --release-version and --feature were set. Only one of the two flags can be set.")
    }
    val featureNamesAndLevelsMap = featureNamesAndLevels(Option(specifiedFeatures).getOrElse(Collections.emptyList).asScala.toList)
    val metadataVersion = getMetadataVersion(namespace, featureNamesAndLevelsMap,
      Option(config.originals.get(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG)).map(_.toString))
    validateMetadataVersion(metadataVersion, config)
    // Get all other features, validate, and create records for them
    // Use latest default for features if --release-version is not specified
    generateFeatureRecords(
      metadataRecords,
      metadataVersion,
      featureNamesAndLevelsMap,
      Features.PRODUCTION_FEATURES.asScala.toList,
      config.unstableFeatureVersionsEnabled,
      releaseVersionFlagSpecified
    )
    getUserScramCredentialRecords(namespace).foreach(userScramCredentialRecords => {
      if (!metadataVersion.isScramSupported) {
        throw new TerseFailure(s"SCRAM is only supported in metadata.version ${MetadataVersion.IBP_3_5_IV2} or later.")
      }
      for (record <- userScramCredentialRecords) {
        metadataRecords.append(new ApiMessageAndVersion(record, 0.toShort))
      }
    })
    val bootstrapMetadata = buildBootstrapMetadata(metadataVersion, Some(metadataRecords), "format command")
    val ignoreFormatted = namespace.getBoolean("ignore_formatted")
    if (!configToSelfManagedMode(config)) {
      throw new TerseFailure("The kafka configuration file appears to be for " +
        "a legacy cluster. Formatting is only supported for clusters in KRaft mode.")
    }
    formatCommand(System.out, directories, metaProperties, bootstrapMetadata,
      metadataVersion,ignoreFormatted)
  }

  private def validateMetadataVersion(metadataVersion: MetadataVersion, config: KafkaConfig): Unit = {
    if (!metadataVersion.isKRaftSupported) {
      throw new TerseFailure(s"Must specify a valid KRaft metadata.version of at least ${MetadataVersion.IBP_3_0_IV0}.")
    }
    if (!metadataVersion.isProduction) {
      if (config.unstableFeatureVersionsEnabled) {
        System.out.println(s"WARNING: using pre-production metadata.version $metadataVersion.")
      } else {
        throw new TerseFailure(s"The metadata.version $metadataVersion is not ready for production use yet.")
      }
    }
    try {
      config.validateWithMetadataVersion(metadataVersion)
    } catch {
      case e: IllegalArgumentException => throw new TerseFailure(s"Invalid configuration for metadata version: ${e.getMessage}")
    }
  }

  private[tools] def generateFeatureRecords(metadataRecords: ArrayBuffer[ApiMessageAndVersion],
                                            metadataVersion: MetadataVersion,
                                            specifiedFeatures: Map[String, java.lang.Short],
                                            allFeatures: List[Features],
                                            unstableFeatureVersionsEnabled: Boolean,
                                            releaseVersionSpecified: Boolean): Unit = {
    // If we are using --release-version, the default is based on the metadata version.
    val metadataVersionForDefault = if (releaseVersionSpecified) metadataVersion else MetadataVersion.LATEST_PRODUCTION

    val allNonZeroFeaturesAndLevels: ArrayBuffer[FeatureVersion] = mutable.ArrayBuffer[FeatureVersion]()

    allFeatures.foreach { feature =>
      val level: java.lang.Short = specifiedFeatures.getOrElse(feature.featureName, feature.defaultValue(metadataVersionForDefault))
      // Only set feature records for levels greater than 0. 0 is assumed if there is no record. Throw an error if level < 0.
      if (level != 0) {
       allNonZeroFeaturesAndLevels.append(feature.fromFeatureLevel(level, unstableFeatureVersionsEnabled))
      }
    }
    val featuresMap = Features.featureImplsToMap(allNonZeroFeaturesAndLevels.asJava)
    featuresMap.put(MetadataVersion.FEATURE_NAME, metadataVersion.featureLevel)

    try {
      for (feature <- allNonZeroFeaturesAndLevels) {
        // In order to validate, we need all feature versions set.
        Features.validateVersion(feature, featuresMap)
        metadataRecords.append(new ApiMessageAndVersion(new FeatureLevelRecord().
          setName(feature.featureName).
          setFeatureLevel(feature.featureLevel), 0.toShort))
      }
    } catch {
      case e: Throwable => throw new TerseFailure(e.getMessage)
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
      help(s"A KRaft release version to use for the initial metadata.version. The minimum is ${MetadataVersion.IBP_3_0_IV0}, the default is ${MetadataVersion.LATEST_PRODUCTION}")
    formatParser.addArgument("--feature", "-f").
      help("A feature upgrade we should perform, in feature=level format. For example: `metadata.version=5`.").
      action(append());

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
    featureNamesAndLevelsMap: Map[String, java.lang.Short],
    defaultVersionString: Option[String]
  ): MetadataVersion = {
    val defaultValue = defaultVersionString match {
      case Some(versionString) => MetadataVersion.fromVersionString(versionString)
      case None => MetadataVersion.LATEST_PRODUCTION
    }

    val releaseVersionTag = Option(namespace.getString("release_version"))
    val featureTag = featureNamesAndLevelsMap.get(MetadataVersion.FEATURE_NAME)

    (releaseVersionTag, featureTag) match {
      case (Some(_), Some(_)) => // We should throw an error before we hit this case, but include for completeness
        throw new IllegalArgumentException("Both --release_version and --feature were set. Only one of the two flags can be set.")
      case (Some(version), None) =>
        MetadataVersion.fromVersionString(version)
      case (None, Some(level)) =>
        MetadataVersion.fromFeatureLevel(level)
      case (None, None) =>
        defaultValue
    }
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

    val copier = new MetaPropertiesEnsemble.Copier(metaPropertiesEnsemble)
    if (!(ignoreFormatted || copier.logDirProps().isEmpty)) {
      val firstLogDir = copier.logDirProps().keySet().iterator().next()
      throw new TerseFailure(s"Log directory $firstLogDir is already formatted. " +
        "Use --ignore-formatted to ignore this directory and format the others.")
    }
    if (!copier.errorLogDirs().isEmpty) {
      copier.errorLogDirs().forEach(errorLogDir => {
        stream.println(s"I/O error trying to read log directory $errorLogDir. Ignoring...")
      })
      if (metaPropertiesEnsemble.emptyLogDirs().isEmpty && copier.logDirProps().isEmpty) {
        throw new TerseFailure("No available log directories to format.")
      }
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
      })
      copier.writeLogDirChanges()
    }
    0
  }

  private def parseNameAndLevel(input: String): (String, java.lang.Short) = {
    val equalsIndex = input.indexOf("=")
    if (equalsIndex < 0)
      throw new RuntimeException("Can't parse feature=level string " + input + ": equals sign not found.")
    val name = input.substring(0, equalsIndex).trim
    val levelString = input.substring(equalsIndex + 1).trim
    try {
      levelString.toShort
    } catch {
      case _: Throwable =>
        throw new RuntimeException("Can't parse feature=level string " + input + ": " + "unable to parse " + levelString + " as a short.")
    }
    (name, levelString.toShort)
  }

  def featureNamesAndLevels(features: List[String]): Map[String, java.lang.Short] = {
    features.map { (feature: String) =>
      // Ensure the feature exists
      val nameAndLevel = parseNameAndLevel(feature)
      (nameAndLevel._1, nameAndLevel._2)
    }.toMap
  }
}
