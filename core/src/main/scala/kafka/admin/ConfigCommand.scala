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

import java.util.Properties

import joptsimple._
import kafka.admin.TopicCommand._
import kafka.log.{Defaults, LogConfig}
import kafka.server.{ClientConfigOverride, ConfigType}
import kafka.utils.{CommandLineUtils, ZkUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Utils

import scala.collection.JavaConversions._
import scala.collection._


/**
 * This script can be used to change configs for topics/clients dynamically
 */
object ConfigCommand {

  def main(args: Array[String]): Unit = {

    val opts = new ConfigCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Add/Remove entity (topics/clients) configs")

    opts.checkArgs()

    val zkUtils = ZkUtils(opts.options.valueOf(opts.zkConnectOpt),
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled())

    try {
      if (opts.options.has(opts.alterOpt))
        alterConfig(zkUtils, opts)
      else if (opts.options.has(opts.describeOpt))
        describeConfig(zkUtils, opts)
    } catch {
      case e: Throwable =>
        println("Error while executing topic command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      zkUtils.close()
    }
  }

  private def alterConfig(zkUtils: ZkUtils, opts: ConfigCommandOptions) {
    val configsToBeAdded = parseConfigsToBeAdded(opts)
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)
    val entityType = opts.options.valueOf(opts.entityType)
    val entityName = opts.options.valueOf(opts.entityName)
    warnOnMaxMessagesChange(configsToBeAdded)

    // compile the final set of configs
    val configs = AdminUtils.fetchEntityConfig(zkUtils, entityType, entityName)
    configs.putAll(configsToBeAdded)
    configsToBeDeleted.foreach(config => configs.remove(config))

    if (entityType.equals(ConfigType.Topic)) {
      AdminUtils.changeTopicConfig(zkUtils, entityName, configs)
      println("Updated config for topic: \"%s\".".format(entityName))
    } else {
      AdminUtils.changeClientIdConfig(zkUtils, entityName, configs)
      println("Updated config for clientId: \"%s\".".format(entityName))
    }
  }

  def warnOnMaxMessagesChange(configs: Properties): Unit = {
    val maxMessageBytes = configs.get(LogConfig.MaxMessageBytesProp) match {
      case n: String => n.toInt
      case _ => -1
    }
    if (maxMessageBytes > Defaults.MaxMessageSize){
      error(TopicCommand.longMessageSizeWarning(maxMessageBytes))
      TopicCommand.askToProceed
    }
  }

  private def describeConfig(zkUtils: ZkUtils, opts: ConfigCommandOptions) {
    val entityType = opts.options.valueOf(opts.entityType)
    val entityNames: Seq[String] =
      if (opts.options.has(opts.entityName))
        Seq(opts.options.valueOf(opts.entityName))
      else
        zkUtils.getAllEntitiesWithConfig(entityType)

    for (entityName <- entityNames) {
      val configs = AdminUtils.fetchEntityConfig(zkUtils, entityType, entityName)
      println("Configs for %s:%s are %s"
                      .format(entityType, entityName, configs.map(kv => kv._1 + "=" + kv._2).mkString(",")))
    }
  }

  private[admin] def parseConfigsToBeAdded(opts: ConfigCommandOptions): Properties = {
    val configsToBeAdded = opts.options.valuesOf(opts.addConfig).map(_.split("""\s*=\s*"""))
    require(configsToBeAdded.forall(config => config.length == 2),
            "Invalid entity config: all configs to be added must be in the format \"key=val\".")
    val props = new Properties
    configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).trim))
    if (props.containsKey(LogConfig.MessageFormatVersionProp)) {
      println(s"WARNING: The configuration ${LogConfig.MessageFormatVersionProp}=${props.getProperty(LogConfig.MessageFormatVersionProp)} is specified. " +
        s"This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker.")
    }
    props
  }

  private[admin] def parseConfigsToBeDeleted(opts: ConfigCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfig)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfig).map(_.trim())
      val propsToBeDeleted = new Properties
      configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  class ConfigCommandOptions(args: Array[String]) {
    val parser = new OptionParser
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over.")
            .withRequiredArg
            .describedAs("urls")
            .ofType(classOf[String])
    val alterOpt = parser.accepts("alter", "Alter the configuration for the entity.")
    val describeOpt = parser.accepts("describe", "List configs for the given entity.")
    val entityType = parser.accepts("entity-type", "Type of entity (topics/clients)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityName = parser.accepts("entity-name", "Name of entity (topic name/client id)")
            .withRequiredArg
            .ofType(classOf[String])

    val nl = System.getProperty("line.separator")
    val addConfig = parser.accepts("add-config", "Key Value pairs configs to add 'k1=v1,k2=v2'. The following is a list of valid configurations: " +
            "For entity_type '" + ConfigType.Topic + "': " + nl + LogConfig.configNames.map("\t" + _).mkString(nl) + nl +
            "For entity_type '" + ConfigType.Client + "': " + nl + "\t" + ClientConfigOverride.ProducerOverride
                                                            + nl + "\t" + ClientConfigOverride.ConsumerOverride)
            .withRequiredArg
            .ofType(classOf[String])
            .withValuesSeparatedBy(',')
    val deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
            .withRequiredArg
            .ofType(classOf[String])
            .withValuesSeparatedBy(',')
    val helpOpt = parser.accepts("help", "Print usage information.")
    val options = parser.parse(args : _*)

    val allOpts: Set[OptionSpec[_]] = Set(alterOpt, describeOpt, entityType, entityName, addConfig, deleteConfig, helpOpt)

    def checkArgs() {
      // should have exactly one action
      val actions = Seq(alterOpt, describeOpt).count(options.has _)
      if(actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --describe, --alter")

      // check required args
      CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, entityType)
      CommandLineUtils.checkInvalidArgs(parser, options, alterOpt, Set(describeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, Set(alterOpt, addConfig, deleteConfig))
      if(options.has(alterOpt)) {
        if(! options.has(entityName))
          throw new IllegalArgumentException("--entity-name must be specified with --alter")

        val isAddConfigPresent: Boolean = options.has(addConfig)
        val isDeleteConfigPresent: Boolean = options.has(deleteConfig)
        if(! isAddConfigPresent && ! isDeleteConfigPresent)
          throw new IllegalArgumentException("At least one of --add-config or --delete-config must be specified with --alter")
      }
      val entityTypeVal = options.valueOf(entityType)
      if(! entityTypeVal.equals(ConfigType.Topic) && ! entityTypeVal.equals(ConfigType.Client)) {
        throw new IllegalArgumentException("--entity-type must be '%s' or '%s'".format(ConfigType.Topic, ConfigType.Client))
      }
    }
  }

}
