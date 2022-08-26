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

import kafka.tools.TerseFailure
import kafka.utils.Exit
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{append, fileType, storeTrue}
import net.sourceforge.argparse4j.inf.{Namespace, Subparsers}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.clients.admin.{Admin, FeatureUpdate, UpdateFeaturesOptions, UpdateFeaturesResult}
import org.apache.kafka.common.utils.Utils

import java.io.File
import java.util.Properties
import scala.collection.Seq
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._

object FeatureCommand {

  def main(args: Array[String]): Unit = {
    val res = mainNoExit(args)
    Exit.exit(res)
  }

  // This is used for integration tests in order to avoid killing the test with Exit.exit
  def mainNoExit(args: Array[String]): Int = {
    val parser = ArgumentParsers.newArgumentParser("kafka-features")
      .defaultHelp(true)
      .description("This tool manages feature flags in Kafka.")
    parser.addArgument("--bootstrap-server")
      .help("A comma-separated list of host:port pairs to use for establishing the connection to the Kafka cluster.")
      .required(true)

    parser.addArgument("--command-config")
      .`type`(fileType())
      .help("Property file containing configs to be passed to Admin Client.")
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
      admin.close()
      0
    } catch {
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        1
    }
  }

  def addDescribeParser(subparsers: Subparsers): Unit = {
    val describeParser = subparsers.addParser("describe")
      .help("Describe one or more feature flags.")

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

    val featureMetadata = admin.describeFeatures().featureMetadata().get()
    val featureEpoch = featureMetadata.finalizedFeaturesEpoch()
    val epochString = if (featureEpoch.isPresent) {
      s"Epoch: ${featureEpoch.get}"
    } else {
      "Epoch: -"
    }
    val finalized = featureMetadata.finalizedFeatures().asScala
    featureMetadata.supportedFeatures().asScala.foreach {
      case (feature, range) =>
        if (featureFilter.apply(feature)) {
          if (finalized.contains(feature)) {
            println(s"Feature: $feature\tSupportedMinVersion: ${range.minVersion()}\t" +
              s"SupportedMaxVersion: ${range.maxVersion()}\tFinalizedVersionLevel: ${finalized(feature).maxVersionLevel()}\t$epochString")
          } else {
            println(s"Feature: $feature\tSupportedMinVersion: ${range.minVersion()}\t" +
              s"SupportedMaxVersion: ${range.maxVersion()}\tFinalizedVersionLevel: -\t$epochString")
          }
        }
    }
  }

  def printReleaseFeatures(release: String): Unit = {
    println(s"Default feature versions for release $release:")
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
      feature -> new FeatureUpdate(version, UpgradeType.UPGRADE)
    }.asJava, new UpdateFeaturesOptions().validateOnly(dryRun))
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
        feature -> new FeatureUpdate(version, UpgradeType.UNSAFE_DOWNGRADE)
      } else {
        feature -> new FeatureUpdate(version, UpgradeType.SAFE_DOWNGRADE)
      }
    }.asJava, new UpdateFeaturesOptions().validateOnly(dryRun))

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
        feature -> new FeatureUpdate(0.toShort, UpgradeType.UNSAFE_DOWNGRADE)
      } else {
        feature -> new FeatureUpdate(0.toShort, UpgradeType.SAFE_DOWNGRADE)
      }
    }.toMap.asJava, new UpdateFeaturesOptions().validateOnly(dryRun))

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
          System.out.println(s"Feature '$feature' can be ${op}d to ${updatedFeatures(feature)}.")
        } else {
          System.out.println(s"Feature '$feature' was ${op}d to ${updatedFeatures(feature)}.")
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
