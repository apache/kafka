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

package kafka.admin

import kafka.server.BrokerFeatures
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, FeatureUpdate, UpdateFeaturesOptions, UpdateFeaturesResult}
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.utils.Utils

import java.util.Properties
import scala.collection.Seq
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import joptsimple.OptionSpec
import kafka.tools.TerseFailure
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{append, fileType, storeTrue}
import net.sourceforge.argparse4j.inf.{Namespace, Subparser, Subparsers}
import org.apache.kafka.clients.admin.FeatureUpdate.DowngradeType

import java.io.File
import scala.concurrent.ExecutionException

object FeatureCommand {

  def main(args: Array[String]): Unit = {
    val parser = ArgumentParsers.newArgumentParser("kafka-features")
      .defaultHelp(true)
      .description("This tool manages feature flags in Kafka.")

    val subparsers = parser.addSubparsers().dest("command")
    addDescribeParser(subparsers)
    addUpgradeParser(subparsers)
    addDowngradeParser(subparsers)
    addDisableParser(subparsers)

    try {
      val namespace = parser.parseArgsOrFail(args)
      val command = namespace.getString("command")

      val commandConfig = namespace.get[File]("command_config")
      val props = if (commandConfig != null) {
        if (!commandConfig.exists()) {
          throw new TerseFailure(s"Properties file ${commandConfig.getPath} does not exists!")
        }
        Utils.loadProps(commandConfig.getPath)
      } else {
        new Properties()
      }
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, namespace.getString("bootstrap_server"))
      val admin = Admin.create(props)

      command match {
        case "describe" => handleDescribe(namespace, admin)
        case "upgrade" => handleUpgrade(namespace, admin)
        case "downgrade" => handleDowngrade(namespace, admin)
        case "disable" => handleDisable(namespace, admin)
      }
      Exit.exit(0)
    } catch {
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        System.exit(1)
    }
  }

  def addTopLevelArgs(parser: Subparser): Unit = {
    parser.addArgument("--bootstrap-server")
      .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.")
      .required(true)

    parser.addArgument("--command-config")
      .`type`(fileType())
      .help("Property file containing configs to be passed to Admin Client.")
  }

  def addDescribeParser(subparsers: Subparsers): Unit = {
    val describeParser = subparsers.addParser("describe")
      .help("Describe one or more feature flags.")
    addTopLevelArgs(describeParser)

    val featureArgs = describeParser.addArgumentGroup("Specific Features")
    featureArgs.addArgument("--feature")
      .action(append())
      .help("A specific feature to describe. This option may be repeated for describing multiple feature flags.")

    val releaseArgs = describeParser.addArgumentGroup("All Features for release")
    releaseArgs.addArgument("--release")
  }

  def addUpgradeParser(subparsers: Subparsers): Unit = {
    val upgradeParser = subparsers.addParser("upgrade")
      .help("Upgrade one or more feature flags.")
    addTopLevelArgs(upgradeParser)

    val featureArgs = upgradeParser.addArgumentGroup("Upgrade specific features")
    featureArgs.addArgument("--feature")
      .action(append())
      .help("A feature flag to upgrade. This option may be repeated for upgrading multiple feature flags.")
    featureArgs.addArgument("--version")
      .`type`(classOf[Short])
      .help("The version to upgrade to.")
      .action(append())

    val releaseArgs = upgradeParser.addArgumentGroup("Upgrade to feature level defined for a given release")
    releaseArgs.addArgument("--release")

    upgradeParser.addArgument("--dry-run")
      .help("Perform a dry-run of this upgrade operation.")
      .action(storeTrue())
  }

  def addDowngradeParser(subparsers: Subparsers): Unit = {
    val downgradeParser = subparsers.addParser("downgrade")
      .help("Upgrade one or more feature flags.")
    addTopLevelArgs(downgradeParser)

    downgradeParser.addArgument("--feature")
      .help("A feature flag to downgrade. This option may be repeated for downgrade multiple feature flags.")
      .required(true)
      .action(append())
    downgradeParser.addArgument("--version")
      .`type`(classOf[Short])
      .help("The version to downgrade to.")
      .required(true)
      .action(append())
    downgradeParser.addArgument("--unsafe")
      .help("Perform this downgrade even if it considered unsafe. Refer to specific feature flag documentation for details.")
      .action(storeTrue())
    downgradeParser.addArgument("--dry-run")
      .help("Perform a dry-run of this downgrade operation.")
      .action(storeTrue())
  }

  def addDisableParser(subparsers: Subparsers): Unit = {
    val disableParser = subparsers.addParser("disable")
      .help("Disable one or more feature flags. This is the same as downgrading the version to zero.")
    addTopLevelArgs(disableParser)

    disableParser.addArgument("--feature")
      .help("A feature flag to disable. This option may be repeated for disable multiple feature flags.")
      .required(true)
      .action(append())
    disableParser.addArgument("--unsafe")
      .help("Disable the feature flag(s) even if it considered unsafe. Refer to specific feature flag documentation for details.")
      .action(storeTrue())
    disableParser.addArgument("--dry-run")
      .help("Perform a dry-run of this disable operation.")
      .action(storeTrue())
  }

  def handleDescribe(namespace: Namespace, admin: Admin): Unit = {
    val featureFilter = parseFeaturesOrRelease(namespace) match {
      case Neither() => (_: String) => true
      case Features(featureNames) => (feature: String) => featureNames.contains(feature)
      case Release(release) =>
        // Special case, print the versions associated with the given release
        printReleaseFeatures(release)
        return
      case Both() => throw new TerseFailure("Only one of --release or --feature may be specified with describe sub-command.")
    }

    admin.describeFeatures().featureMetadata().get().finalizedFeatures().asScala.foreach {
      case (feature, range) =>
        if (featureFilter.apply(feature)) {
          System.out.println(s"Feature: $feature\tVersion: ${range.maxVersionLevel()}")
        }
    }
  }

  def printReleaseFeatures(release: String): Unit = {
    System.out.println(s"Default feature versions for release $release:")
  }

  def handleUpgrade(namespace: Namespace, admin: Admin): Unit = {
    val featuresToUpgrade = parseFeaturesOrRelease(namespace) match {
      case Features(featureNames) => parseVersions(featureNames, namespace)
      case Release(release) => featuresForRelease(release)
      case Neither() => throw new TerseFailure("Must specify either --release or at least one --feature and --version with upgrade sub-command.")
      case Both() => throw new TerseFailure("Cannot specify both --release and --feature with upgrade sub-command.")
    }

    val dryRun = namespace.getBoolean("dry_run")
    val updateResult = admin.updateFeatures(featuresToUpgrade.map { case (feature, version) =>
      feature -> new FeatureUpdate(version, DowngradeType.NONE)
    }.asJava, new UpdateFeaturesOptions().dryRun(dryRun))
    handleUpdateFeaturesResponse(updateResult, featuresToUpgrade, dryRun, "upgrade")
  }

  def handleDowngrade(namespace: Namespace, admin: Admin): Unit = {
    val featuresToDowngrade = parseFeaturesOrRelease(namespace) match {
      case Features(featureNames) => parseVersions(featureNames, namespace)
      case Neither() => throw new TerseFailure("Must specify at least one --feature and --version with downgrade sub-command.")
      case _ => throw new IllegalStateException()
    }

    val dryRun = namespace.getBoolean("dry_run")
    val unsafe = namespace.getBoolean("unsafe")
    val updateResult = admin.updateFeatures(featuresToDowngrade.map { case (feature, version) =>
      if (unsafe) {
        feature -> new FeatureUpdate(version, DowngradeType.UNSAFE)
      } else {
        feature -> new FeatureUpdate(version, DowngradeType.SAFE)
      }
    }.asJava, new UpdateFeaturesOptions().dryRun(dryRun))

    handleUpdateFeaturesResponse(updateResult, featuresToDowngrade, dryRun, "downgrade")
  }

  def handleDisable(namespace: Namespace, admin: Admin): Unit = {
    val featuresToDisable = parseFeaturesOrRelease(namespace) match {
      case Features(featureNames) => featureNames
      case Neither() => throw new TerseFailure("Must specify at least one --feature and --version with downgrade sub-command.")
      case _ => throw new IllegalStateException()
    }

    val dryRun = namespace.getBoolean("dry_run")
    val unsafe = namespace.getBoolean("unsafe")
    val updateResult = admin.updateFeatures(featuresToDisable.map { feature =>
      if (unsafe) {
        feature -> new FeatureUpdate(0.toShort, DowngradeType.UNSAFE)
      } else {
        feature -> new FeatureUpdate(0.toShort, DowngradeType.SAFE)
      }
    }.toMap.asJava, new UpdateFeaturesOptions().dryRun(dryRun))

    handleUpdateFeaturesResponse(updateResult, featuresToDisable.map {
      feature => feature -> 0.toShort
    }.toMap, dryRun, "disable")
  }

  def handleUpdateFeaturesResponse(updateResult: UpdateFeaturesResult,
                                   updatedFeatures: Map[String, Short],
                                   dryRun: Boolean,
                                   op: String): Unit = {
    val errors = updateResult.values().asScala.map { case (feature, future) =>
      try {
        future.get()
        feature -> None
      } catch {
        case e: ExecutionException => feature -> Some(e.getCause)
        case t: Throwable => feature -> Some(t)
      }
    }

    errors.foreach { case (feature, maybeThrowable) =>
      if (maybeThrowable.isDefined) {
        if (dryRun) {
          System.out.println(s"Can not $op feature '$feature' to ${updatedFeatures(feature)}. ${maybeThrowable.get.getMessage}")
        } else {
          System.out.println(s"Could not $op feature '$feature' to ${updatedFeatures(feature)}. ${maybeThrowable.get.getMessage}")
        }
      } else {
        if (dryRun) {
          System.out.println(s"Feature '$feature' can be ${op}ed to ${updatedFeatures(feature)}.")
        } else {
          System.out.println(s"Feature '$feature' was ${op}ed to ${updatedFeatures(feature)}.")
        }
      }
    }
  }

  sealed trait ReleaseOrFeatures { }
  case class Neither() extends ReleaseOrFeatures
  case class Release(release: String) extends ReleaseOrFeatures
  case class Features(featureNames: Seq[String]) extends ReleaseOrFeatures
  case class Both() extends ReleaseOrFeatures

  def parseFeaturesOrRelease(namespace: Namespace): ReleaseOrFeatures = {
    val release = namespace.getString("release")
    val features = namespace.getList[String]("feature").asScala

    if (release != null && features != null) {
      Both()
    } else if (release == null && features == null) {
      Neither()
    } else if (release != null) {
      Release(release)
    } else {
      Features(features)
    }
  }

  def parseVersions(features: Seq[String], namespace: Namespace): Map[String, Short] = {
    val versions = namespace.getList[Short]("version").asScala
    if (versions == null) {
      throw new TerseFailure("Must specify --version when using --feature argument(s).")
    }
    if (versions.size != features.size) {
      if (versions.size > features.size) {
        throw new TerseFailure("Too many --version arguments given. For each --feature argument there should be one --version argument.")
      } else {
        throw new TerseFailure("Too many --feature arguments given. For each --feature argument there should be one --version argument.")
      }
    }
    features.zip(versions).map { case (feature, version) =>
      feature -> version
    }.toMap
  }

  def defaultFeatures(): Map[String, Short] = {
    Map.empty
  }

  def featuresForRelease(release: String): Map[String, Short] = {
    Map.empty
  }
}

class UpdateFeaturesException(message: String) extends RuntimeException(message)

/**
 * A class that provides necessary APIs to bridge feature APIs provided by the Admin client with
 * the requirements of the CLI tool.
 *
 * @param opts the CLI options
 */
class FeatureApis(private var opts: FeatureCommandOptions) {
  private var supportedFeatures = BrokerFeatures.createDefault().supportedFeatures
  private var adminClient = FeatureApis.createAdminClient(opts)

  private def pad(op: String): String = {
    f"$op%11s"
  }

  private val addOp = pad("[Add]")
  private val upgradeOp = pad("[Upgrade]")
  private val deleteOp = pad("[Delete]")
  private val downgradeOp = pad("[Downgrade]")

  // For testing only.
  private[admin] def setSupportedFeatures(newFeatures: Features[SupportedVersionRange]): Unit = {
    supportedFeatures = newFeatures
  }

  // For testing only.
  private[admin] def setOptions(newOpts: FeatureCommandOptions): Unit = {
    adminClient.close()
    adminClient = FeatureApis.createAdminClient(newOpts)
    opts = newOpts
  }

  /**
   * Describes the supported and finalized features. The request is issued to any of the provided
   * bootstrap servers.
   */
  def describeFeatures(): Unit = {
    val result = adminClient.describeFeatures.featureMetadata.get
    val features = result.supportedFeatures.asScala.keys.toSet ++ result.finalizedFeatures.asScala.keys.toSet

    features.toList.sorted.foreach {
      feature =>
        val output = new StringBuilder()
        output.append(s"Feature: $feature")

        val (supportedMinVersion, supportedMaxVersion) = {
          val supportedVersionRange = result.supportedFeatures.get(feature)
          if (supportedVersionRange == null) {
            ("-", "-")
          } else {
            (supportedVersionRange.minVersion, supportedVersionRange.maxVersion)
          }
        }
        output.append(s"\tSupportedMinVersion: $supportedMinVersion")
        output.append(s"\tSupportedMaxVersion: $supportedMaxVersion")

        val (finalizedMinVersionLevel, finalizedMaxVersionLevel) = {
          val finalizedVersionRange = result.finalizedFeatures.get(feature)
          if (finalizedVersionRange == null) {
            ("-", "-")
          } else {
            (finalizedVersionRange.minVersionLevel, finalizedVersionRange.maxVersionLevel)
          }
        }
        output.append(s"\tFinalizedMinVersionLevel: $finalizedMinVersionLevel")
        output.append(s"\tFinalizedMaxVersionLevel: $finalizedMaxVersionLevel")

        val epoch = {
          if (result.finalizedFeaturesEpoch.isPresent) {
            result.finalizedFeaturesEpoch.get.toString
          } else {
            "-"
          }
        }
        output.append(s"\tEpoch: $epoch")

        println(output)
    }
  }

  /**
   * Upgrades all features known to this tool to their highest max version levels. The method may
   * add new finalized features if they were not finalized previously, but it does not delete
   * any existing finalized feature. The results of the feature updates are written to STDOUT.
   *
   * NOTE: if the --dry-run CLI option is provided, this method only prints the expected feature
   * updates to STDOUT, without applying them.
   *
   * @throws UpdateFeaturesException if at least one of the feature updates failed
   */
  def upgradeAllFeatures(): Unit = {
    val metadata = adminClient.describeFeatures.featureMetadata.get
    val existingFinalizedFeatures = metadata.finalizedFeatures
    val updates = supportedFeatures.features.asScala.map {
      case (feature, targetVersionRange) =>
        val existingVersionRange = existingFinalizedFeatures.get(feature)
        if (existingVersionRange == null) {
          val updateStr =
            addOp +
            s"\tFeature: $feature" +
            s"\tExistingFinalizedMaxVersion: -" +
            s"\tNewFinalizedMaxVersion: ${targetVersionRange.max}"
          (feature, Some((updateStr, new FeatureUpdate(targetVersionRange.max, false))))
        } else {
          if (targetVersionRange.max > existingVersionRange.maxVersionLevel) {
            val updateStr =
              upgradeOp +
              s"\tFeature: $feature" +
              s"\tExistingFinalizedMaxVersion: ${existingVersionRange.maxVersionLevel}" +
              s"\tNewFinalizedMaxVersion: ${targetVersionRange.max}"
            (feature, Some((updateStr, new FeatureUpdate(targetVersionRange.max, false))))
          } else {
            (feature, Option.empty)
          }
        }
    }.filter {
      case(_, updateInfo) => updateInfo.isDefined
    }.map {
      case(feature, updateInfo) => (feature, updateInfo.get)
    }.toMap

    if (updates.nonEmpty) {
      maybeApplyFeatureUpdates(updates)
    }
  }

  /**
   * Downgrades existing finalized features to the highest max version levels known to this tool.
   * The method may delete existing finalized features if they are no longer seen to be supported,
   * but it does not add a feature that was not finalized previously. The results of the feature
   * updates are written to STDOUT.
   *
   * NOTE: if the --dry-run CLI option is provided, this method only prints the expected feature
   * updates to STDOUT, without applying them.
   *
   * @throws UpdateFeaturesException if at least one of the feature updates failed
   */
  def downgradeAllFeatures(): Unit = {
    val metadata = adminClient.describeFeatures.featureMetadata.get
    val existingFinalizedFeatures = metadata.finalizedFeatures
    val supportedFeaturesMap = supportedFeatures.features
    val updates = existingFinalizedFeatures.asScala.map {
      case (feature, existingVersionRange) =>
        val targetVersionRange = supportedFeaturesMap.get(feature)
        if (targetVersionRange == null) {
          val updateStr =
            deleteOp +
            s"\tFeature: $feature" +
            s"\tExistingFinalizedMaxVersion: ${existingVersionRange.maxVersionLevel}" +
            s"\tNewFinalizedMaxVersion: -"
          (feature, Some(updateStr, new FeatureUpdate(0.toShort, true)))
        } else {
          if (targetVersionRange.max < existingVersionRange.maxVersionLevel) {
            val updateStr =
              downgradeOp +
              s"\tFeature: $feature" +
              s"\tExistingFinalizedMaxVersion: ${existingVersionRange.maxVersionLevel}" +
              s"\tNewFinalizedMaxVersion: ${targetVersionRange.max}"
            (feature, Some(updateStr, new FeatureUpdate(targetVersionRange.max, true)))
          } else {
            (feature, Option.empty)
          }
        }
    }.filter {
      case(_, updateInfo) => updateInfo.isDefined
    }.map {
      case(feature, updateInfo) => (feature, updateInfo.get)
    }.toMap

    if (updates.nonEmpty) {
      maybeApplyFeatureUpdates(updates)
    }
  }

  /**
   * Applies the provided feature updates. If the --dry-run CLI option is provided, the method
   * only prints the expected feature updates to STDOUT without applying them.
   *
   * @param updates the feature updates to be applied via the admin client
   *
   * @throws UpdateFeaturesException if at least one of the feature updates failed
   */
  private def maybeApplyFeatureUpdates(updates: Map[String, (String, FeatureUpdate)]): Unit = {
    if (opts.hasDryRunOption) {
      println("Expected feature updates:" + ListMap(
        updates
          .toSeq
          .sortBy { case(feature, _) => feature} :_*)
          .map { case(_, (updateStr, _)) => updateStr}
          .mkString("\n"))
    } else {
      val result = adminClient.updateFeatures(
        updates
          .map { case(feature, (_, update)) => (feature, update)}
          .asJava,
        new UpdateFeaturesOptions())
      val resultSortedByFeature = ListMap(
        result
          .values
          .asScala
          .toSeq
          .sortBy { case(feature, _) => feature} :_*)
      val failures = resultSortedByFeature.map {
        case (feature, updateFuture) =>
          val (updateStr, _) = updates(feature)
          try {
            updateFuture.get
            println(updateStr + "\tResult: OK")
            0
          } catch {
            case e: ExecutionException =>
              val cause = if (e.getCause == null) e else e.getCause
              println(updateStr + "\tResult: FAILED due to " + cause)
              1
            case e: Throwable =>
              println(updateStr + "\tResult: FAILED due to " + e)
              1
          }
      }.sum
      if (failures > 0) {
        throw new UpdateFeaturesException(s"$failures feature updates failed!")
      }
    }
  }

  def execute(): Unit = {
    if (opts.hasDescribeOption) {
      describeFeatures()
    } else if (opts.hasUpgradeAllOption) {
      upgradeAllFeatures()
    } else if (opts.hasDowngradeAllOption) {
      downgradeAllFeatures()
    } else {
      throw new IllegalStateException("Unexpected state: no CLI command could be executed.")
    }
  }

  def close(): Unit = {
    adminClient.close()
  }
}

class FeatureCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
  private val bootstrapServerOpt = parser.accepts(
      "bootstrap-server",
      "REQUIRED: A comma-separated list of host:port pairs to use for establishing the connection" +
      " to the Kafka cluster.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
  private val commandConfigOpt = parser.accepts(
    "command-config",
    "Property file containing configs to be passed to Admin Client." +
    " This is used with --bootstrap-server option when required.")
    .withOptionalArg
    .describedAs("command config property file")
    .ofType(classOf[String])
  private val describeOpt = parser.accepts(
    "describe",
    "Describe supported and finalized features from a random broker.")
  private val upgradeAllOpt = parser.accepts(
    "upgrade-all",
    "Upgrades all finalized features to the maximum version levels known to the tool." +
    " This command finalizes new features known to the tool that were never finalized" +
    " previously in the cluster, but it is guaranteed to not delete any existing feature.")
  private val downgradeAllOpt = parser.accepts(
    "downgrade-all",
    "Downgrades all finalized features to the maximum version levels known to the tool." +
    " This command deletes unknown features from the list of finalized features in the" +
    " cluster, but it is guaranteed to not add a new feature.")
  private val dryRunOpt = parser.accepts(
    "dry-run",
    "Performs a dry-run of upgrade/downgrade mutations to finalized feature without applying them.")

  options = parser.parse(args : _*)

  checkArgs()

  def has(builder: OptionSpec[_]): Boolean = options.has(builder)

  def hasDescribeOption: Boolean = has(describeOpt)

  def hasDryRunOption: Boolean = has(dryRunOpt)

  def hasUpgradeAllOption: Boolean = has(upgradeAllOpt)

  def hasDowngradeAllOption: Boolean = has(downgradeAllOpt)

  def commandConfig: Properties = {
    if (has(commandConfigOpt))
      Utils.loadProps(options.valueOf(commandConfigOpt))
    else
      new Properties()
  }

  def bootstrapServers: String = options.valueOf(bootstrapServerOpt)

  def checkArgs(): Unit = {
    CommandLineUtils.printHelpAndExitIfNeeded(this, "This tool describes and updates finalized features.")
    val numActions = Seq(describeOpt, upgradeAllOpt, downgradeAllOpt).count(has)
    if (numActions != 1) {
      CommandLineUtils.printUsageAndDie(
        parser,
        "Command must include exactly one action: --describe, --upgrade-all, --downgrade-all.")
    }
    CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt)
    if (hasDryRunOption && !hasUpgradeAllOption && !hasDowngradeAllOption) {
      CommandLineUtils.printUsageAndDie(
        parser,
        "Command can contain --dry-run option only when either --upgrade-all or --downgrade-all actions are provided.")
    }
  }
}

object FeatureApis {
  def createAdminClient(opts: FeatureCommandOptions): Admin = {
    val props = new Properties()
    props.putAll(opts.commandConfig)
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.bootstrapServers)
    Admin.create(props)
  }
}
