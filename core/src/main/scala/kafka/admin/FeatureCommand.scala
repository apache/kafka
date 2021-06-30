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
import org.apache.kafka.clients.admin.{Admin, FeatureUpdate, UpdateFeaturesOptions}
import org.apache.kafka.common.feature.{Features, SupportedVersionRange}
import org.apache.kafka.common.utils.Utils
import java.util.Properties

import scala.collection.Seq
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import joptsimple.OptionSpec

import scala.concurrent.ExecutionException

object FeatureCommand {

  def main(args: Array[String]): Unit = {
    val opts = new FeatureCommandOptions(args)
    val featureApis = new FeatureApis(opts)
    var exitCode = 0
    try {
      featureApis.execute()
    } catch {
      case e: IllegalArgumentException =>
        printException(e)
        opts.parser.printHelpOn(System.err)
        exitCode = 1
      case _: UpdateFeaturesException =>
        exitCode = 1
      case e: ExecutionException =>
        val cause = if (e.getCause == null) e else e.getCause
        printException(cause)
        exitCode = 1
      case e: Throwable =>
        printException(e)
        exitCode = 1
    } finally {
      featureApis.close()
      Exit.exit(exitCode)
    }
  }

  private def printException(exception: Throwable): Unit = {
    System.err.println("\nError encountered when executing command: " + Utils.stackTrace(exception))
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
          (feature, Some(updateStr, new FeatureUpdate(0, true)))
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
  private def createAdminClient(opts: FeatureCommandOptions): Admin = {
    val props = new Properties()
    props.putAll(opts.commandConfig)
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.bootstrapServers)
    Admin.create(props)
  }
}
