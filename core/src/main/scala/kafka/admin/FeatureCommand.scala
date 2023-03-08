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
import net.sourceforge.argparse4j.impl.Arguments.{append, fileType, store, storeTrue}
import net.sourceforge.argparse4j.inf.{ArgumentParserException, Namespace, Subparsers}
import net.sourceforge.argparse4j.internal.HelpScreenException
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType
import org.apache.kafka.clients.admin.{Admin, FeatureUpdate, UpdateFeaturesOptions}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.common.MetadataVersion

import java.io.{File, PrintStream}
import java.util.Properties
import scala.concurrent.ExecutionException
import scala.jdk.CollectionConverters._
import scala.compat.java8.OptionConverters._

object FeatureCommand {
  def main(args: Array[String]): Unit = {
    val res = mainNoExit(args, System.out)
    Exit.exit(res)
  }

  // This is used for integration tests in order to avoid killing the test with Exit.exit,
  // and in order to capture the command output.
  def mainNoExit(
    args: Array[String],
    out: PrintStream
  ): Int = {
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
      val namespace = parser.parseArgs(args)
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
      try {
        command match {
          case "describe" => handleDescribe(out, admin)
          case "upgrade" => handleUpgrade(out, namespace, admin)
          case "downgrade" => handleDowngrade(out, namespace, admin)
          case "disable" => handleDisable(out, namespace, admin)
        }
      } finally {
        admin.close()
      }
      0
    } catch {
      case _: HelpScreenException =>
        0
      case e: ArgumentParserException =>
        System.err.println(s"Command line error: ${e.getMessage}. Type --help for help.")
        1
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        1
    }
  }

  def addDescribeParser(subparsers: Subparsers): Unit = {
    subparsers.addParser("describe")
      .help("Describes the current active feature flags.")
  }

  def addUpgradeParser(subparsers: Subparsers): Unit = {
    val upgradeParser = subparsers.addParser("upgrade")
      .help("Upgrade one or more feature flags.")
    upgradeParser.addArgument("--metadata")
      .help("The level to which we should upgrade the metadata. For example, 3.3-IV3.")
      .action(store())
    upgradeParser.addArgument("--feature")
      .help("A feature upgrade we should perform, in feature=level format. For example: `metadata.version=5`.")
      .action(append())
    upgradeParser.addArgument("--dry-run")
      .help("Validate this upgrade, but do not perform it.")
      .action(storeTrue())
  }

  def addDowngradeParser(subparsers: Subparsers): Unit = {
    val downgradeParser = subparsers.addParser("downgrade")
      .help("Downgrade one or more feature flags.")
    downgradeParser.addArgument("--metadata")
      .help("The level to which we should downgrade the metadata. For example, 3.3-IV0.")
      .action(store())
    downgradeParser.addArgument("--feature")
      .help("A feature downgrade we should perform, in feature=level format. For example: `metadata.version=5`.")
      .action(append())
    downgradeParser.addArgument("--unsafe")
      .help("Perform this downgrade even if it may irreversibly destroy metadata.")
      .action(storeTrue())
    downgradeParser.addArgument("--dry-run")
      .help("Validate this downgrade, but do not perform it.")
      .action(storeTrue())
  }

  def addDisableParser(subparsers: Subparsers): Unit = {
    val disableParser = subparsers.addParser("disable")
      .help("Disable one or more feature flags. This is the same as downgrading the version to zero.")
    disableParser.addArgument("--feature")
      .help("A feature flag to disable.")
      .action(append())
    disableParser.addArgument("--unsafe")
      .help("Disable this feature flag even if it may irreversibly destroy metadata.")
      .action(storeTrue())
    disableParser.addArgument("--dry-run")
      .help("Perform a dry-run of this disable operation.")
      .action(storeTrue())
  }

  def levelToString(
    feature: String,
    level: Short
  ): String = {
    if (feature.equals(MetadataVersion.FEATURE_NAME))
      try MetadataVersion.fromFeatureLevel(level).version
      catch { case _: Throwable => s"UNKNOWN [$level]"}
    else
      level.toString
  }

  def handleDescribe(
    out: PrintStream,
    admin: Admin
  ): Unit = {
    val featureMetadata = admin.describeFeatures().featureMetadata().get()
    val featureList = new java.util.TreeSet[String](featureMetadata.supportedFeatures().keySet())
      featureList.forEach { feature =>
        val finalizedLevel = featureMetadata.finalizedFeatures().asScala.get(feature) match {
          case None => 0.toShort
          case Some(v) => v.maxVersionLevel()
        }
        val range = featureMetadata.supportedFeatures().get(feature)
        out.printf("Feature: %s\tSupportedMinVersion: %s\tSupportedMaxVersion: %s\tFinalizedVersionLevel: %s\tEpoch: %s%n",
          feature,
          levelToString(feature, range.minVersion()),
          levelToString(feature, range.maxVersion()),
          levelToString(feature, finalizedLevel),
          featureMetadata.finalizedFeaturesEpoch().asScala.flatMap(e => Some(e.toString)).getOrElse("-"))
    }
  }

  def metadataVersionsToString(first: MetadataVersion, last: MetadataVersion): String = {
    MetadataVersion.VERSIONS.toList.asJava.
      subList(first.ordinal(), last.ordinal() + 1).
      asScala.mkString(", ")
  }

  def handleUpgrade(out: PrintStream, namespace: Namespace, admin: Admin): Unit = {
    handleUpgradeOrDowngrade("upgrade", out, namespace, admin, UpgradeType.UPGRADE)
  }

  def downgradeType(namespace: Namespace): UpgradeType = {
    val unsafe = namespace.getBoolean("unsafe")
    if (unsafe == null || !unsafe) {
      UpgradeType.SAFE_DOWNGRADE
    } else {
      UpgradeType.UNSAFE_DOWNGRADE
    }
  }

  def handleDowngrade(out: PrintStream, namespace: Namespace, admin: Admin): Unit = {
    handleUpgradeOrDowngrade("downgrade", out, namespace, admin, downgradeType(namespace))
  }

  def parseNameAndLevel(input: String): (String, Short) = {
    val equalsIndex = input.indexOf("=")
    if (equalsIndex < 0) {
      throw new TerseFailure(s"Can't parse feature=level string ${input}: equals sign not found.")
    }
    val name = input.substring(0, equalsIndex).trim
    val levelString = input.substring(equalsIndex + 1).trim
    val level =
      try levelString.toShort
      catch {
        case _: Throwable => throw new TerseFailure(s"Can't parse feature=level string ${input}: " +
          s"unable to parse ${levelString} as a short.")
      }
    (name, level)
  }

  def handleUpgradeOrDowngrade(
    op: String,
    out: PrintStream,
    namespace: Namespace,
    admin: Admin,
    upgradeType: UpgradeType
  ): Unit = {
    val updates = new java.util.HashMap[String, FeatureUpdate]()
    Option(namespace.getString("metadata")).foreach(metadata => {
      val version = try {
        MetadataVersion.fromVersionString(metadata)
      } catch {
        case _: Throwable => throw new TerseFailure("Unsupported metadata version " + metadata +
          ". Supported metadata versions are " + metadataVersionsToString(
          MetadataVersion.MINIMUM_BOOTSTRAP_VERSION, MetadataVersion.latest()))
      }
      updates.put(MetadataVersion.FEATURE_NAME, new FeatureUpdate(version.featureLevel(), upgradeType))
    })
    Option(namespace.getList[String]("feature")).foreach(features => {
      features.forEach(feature => {
        val (name, level) = parseNameAndLevel(feature)
        if (updates.put(name, new FeatureUpdate(level, upgradeType)) != null) {
          throw new TerseFailure(s"Feature ${name} was specified more than once.")
        }
      })
    })
    update(op, out, admin, updates, namespace.getBoolean("dry-run"))
  }

  def handleDisable(out: PrintStream, namespace: Namespace, admin: Admin): Unit = {
    val upgradeType = downgradeType(namespace)
    val updates = new java.util.HashMap[String, FeatureUpdate]()
    Option(namespace.getList[String]("feature")).foreach(features => {
      features.forEach(name =>
        if (updates.put(name, new FeatureUpdate(0.toShort, upgradeType)) != null) {
          throw new TerseFailure(s"Feature ${name} was specified more than once.")
        })
      }
    )
    update("disable", out, admin, updates, namespace.getBoolean("dry-run"))
  }

  def update(
    op: String,
    out: PrintStream,
    admin: Admin,
    updates: java.util.HashMap[String, FeatureUpdate],
    dryRun: Boolean
  ): Unit = {
    if (updates.isEmpty) {
      throw new TerseFailure(s"You must specify at least one feature to ${op}")
    }
    val result =  admin.updateFeatures(updates, new UpdateFeaturesOptions().validateOnly(dryRun))
    val errors = result.values().asScala.map { case (feature, future) =>
      try {
        future.get()
        feature -> None
      } catch {
        case e: ExecutionException => feature -> Some(e.getCause)
        case t: Throwable => feature -> Some(t)
      }
    }
    var numFailures = 0
    errors.keySet.toList.sorted.foreach { feature =>
      val maybeThrowable = errors(feature)
      val level = updates.get(feature).maxVersionLevel()
      if (maybeThrowable.isDefined) {
        val helper = if (dryRun) {
          "Can not"
        } else {
          "Could not"
        }
        val suffix = if (op.equals("disable")) {
          s"disable ${feature}"
        } else {
          s"${op} ${feature} to ${level}"
        }
        out.println(s"${helper} ${suffix}. ${maybeThrowable.get.getMessage}")
        numFailures = numFailures + 1
      } else {
        val verb = if (dryRun) {
          "can be"
        } else {
          "was"
        }
        val obj = if (op.equals("disable")) {
          "disabled."
        } else {
          s"${op}d to ${level}."
        }
        out.println(s"${feature} ${verb} ${obj}")
      }
    }
    if (numFailures > 0) {
      throw new TerseFailure(s"${numFailures} out of ${updates.size} operation(s) failed.")
    }
  }
}
