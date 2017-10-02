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
import kafka.common.Config
import kafka.common.InvalidConfigException
import kafka.log.LogConfig
import kafka.server.{ConfigEntityName, ConfigType, DynamicConfig}
import kafka.utils.{CommandLineUtils, ZkUtils}
import kafka.utils.Implicits._
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.scram._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.metrics.Sanitizer
import scala.collection._
import scala.collection.JavaConverters._


/**
 * This script can be used to change configs for topics/clients/brokers dynamically
 * This script can be used to change configs for topics/clients/users/brokers dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: --entity-type topics --entity-name <topic>
 *     <li> client: --entity-type clients --entity-name <client-id>
 *     <li> user: --entity-type users --entity-name <user-principal>
 *     <li> <user, client>: --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>
 *     <li> broker: --entity-type brokers --entity-name <broker>
 * </ul>
 * --entity-default may be used instead of --entity-name when describing or altering default configuration for users and clients.
 *
 */
object ConfigCommand extends Config {

  val DefaultScramIterations = 4096

  def main(args: Array[String]): Unit = {

    val opts = new ConfigCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "Add/Remove entity config for a topic, client, user or broker")

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
        println("Error while executing config command " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally {
      zkUtils.close()
    }
  }

  private[admin] def alterConfig(zkUtils: ZkUtils, opts: ConfigCommandOptions, utils: AdminUtilities = AdminUtils) {
    val configsToBeAdded = parseConfigsToBeAdded(opts)
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)
    val entity = parseEntity(opts)
    val entityType = entity.root.entityType
    val entityName = entity.fullSanitizedName

    if (entityType == ConfigType.User)
      preProcessScramCredentials(configsToBeAdded)

    // compile the final set of configs
    val configs = utils.fetchEntityConfig(zkUtils, entityType, entityName)

    // fail the command if any of the configs to be deleted does not exist
    val invalidConfigs = configsToBeDeleted.filterNot(configs.containsKey(_))
    if (invalidConfigs.nonEmpty)
      throw new InvalidConfigException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

    configs ++= configsToBeAdded
    configsToBeDeleted.foreach(configs.remove(_))

    utils.changeConfigs(zkUtils, entityType, entityName, configs)

    println(s"Completed Updating config for entity: $entity.")
  }

  private def preProcessScramCredentials(configsToBeAdded: Properties) {
    def scramCredential(mechanism: ScramMechanism, credentialStr: String): String = {
      val pattern = "(?:iterations=([0-9]*),)?password=(.*)".r
      val (iterations, password) = credentialStr match {
          case pattern(iterations, password) => (if (iterations != null) iterations.toInt else DefaultScramIterations, password)
          case _ => throw new IllegalArgumentException(s"Invalid credential property $mechanism=$credentialStr")
        }
      if (iterations < mechanism.minIterations())
        throw new IllegalArgumentException(s"Iterations $iterations is less than the minimum ${mechanism.minIterations()} required for $mechanism")
      val credential = new ScramFormatter(mechanism).generateCredential(password, iterations)
      ScramCredentialUtils.credentialToString(credential)
    }
    for (mechanism <- ScramMechanism.values) {
      configsToBeAdded.getProperty(mechanism.mechanismName) match {
        case null =>
        case value =>
          configsToBeAdded.setProperty(mechanism.mechanismName, scramCredential(mechanism, value))
      }
    }
  }

  private def describeConfig(zkUtils: ZkUtils, opts: ConfigCommandOptions) {
    val configEntity = parseEntity(opts)
    val describeAllUsers = configEntity.root.entityType == ConfigType.User && !configEntity.root.sanitizedName.isDefined && !configEntity.child.isDefined
    val entities = configEntity.getAllEntities(zkUtils)
    for (entity <- entities) {
      val configs = AdminUtils.fetchEntityConfig(zkUtils, entity.root.entityType, entity.fullSanitizedName)
      // When describing all users, don't include empty user nodes with only <user, client> quota overrides.
      if (!configs.isEmpty || !describeAllUsers) {
        println("Configs for %s are %s"
          .format(entity, configs.asScala.map(kv => kv._1 + "=" + kv._2).mkString(",")))
      }
    }
  }

  private[admin] def parseConfigsToBeAdded(opts: ConfigCommandOptions): Properties = {
    val props = new Properties
    if (opts.options.has(opts.addConfig)) {
      //split by commas, but avoid those in [], then into KV pairs
      val pattern = "(?=[^\\]]*(?:\\[|$))"
      val configsToBeAdded = opts.options.valueOf(opts.addConfig)
        .split("," + pattern)
        .map(_.split("""\s*=\s*""" + pattern))
      require(configsToBeAdded.forall(config => config.length == 2), "Invalid entity config: all configs to be added must be in the format \"key=val\".")
      //Create properties, parsing square brackets from values if necessary
      configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).replaceAll("\\[?\\]?", "").trim))
      if (props.containsKey(LogConfig.MessageFormatVersionProp)) {
        println(s"WARNING: The configuration ${LogConfig.MessageFormatVersionProp}=${props.getProperty(LogConfig.MessageFormatVersionProp)} is specified. " +
          s"This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker.")
      }
    }
    props
  }

  private[admin] def parseConfigsToBeDeleted(opts: ConfigCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfig)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfig).asScala.map(_.trim())
      val propsToBeDeleted = new Properties
      configsToBeDeleted.foreach(propsToBeDeleted.setProperty(_, ""))
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  case class Entity(entityType: String, sanitizedName: Option[String]) {
    val entityPath = sanitizedName match {
      case Some(n) => entityType + "/" + n
      case None => entityType
    }
    override def toString: String = {
      val typeName = entityType match {
        case ConfigType.User => "user-principal"
        case ConfigType.Client => "client-id"
        case ConfigType.Topic => "topic"
        case t => t
      }
      sanitizedName match {
        case Some(ConfigEntityName.Default) => "default " + typeName
        case Some(n) =>
          val desanitized = if (entityType == ConfigType.User || entityType == ConfigType.Client) Sanitizer.desanitize(n) else n
          s"$typeName '$desanitized'"
        case None => entityType
      }
    }
  }

  case class ConfigEntity(root: Entity, child: Option[Entity]) {
    val fullSanitizedName = root.sanitizedName.getOrElse("") + child.map(s => "/" + s.entityPath).getOrElse("")

    def getAllEntities(zkUtils: ZkUtils) : Seq[ConfigEntity] = {
      // Describe option examples:
      //   Describe entity with specified name:
      //     --entity-type topics --entity-name topic1 (topic1)
      //   Describe all entities of a type (topics/brokers/users/clients):
      //     --entity-type topics (all topics)
      //   Describe <user, client> quotas:
      //     --entity-type users --entity-name user1 --entity-type clients --entity-name client2 (<user1, client2>)
      //     --entity-type users --entity-name userA --entity-type clients (all clients of userA)
      //     --entity-type users --entity-type clients (all <user, client>s))
      //   Describe default quotas:
      //     --entity-type users --entity-default (Default user)
      //     --entity-type users --entity-default --entity-type clients --entity-default (Default <user, client>)
      (root.sanitizedName, child) match {
        case (None, _) =>
          val rootEntities = zkUtils.getAllEntitiesWithConfig(root.entityType)
                                   .map(name => ConfigEntity(Entity(root.entityType, Some(name)), child))
          child match {
            case Some(s) =>
                rootEntities.flatMap(rootEntity =>
                  ConfigEntity(rootEntity.root, Some(Entity(s.entityType, None))).getAllEntities(zkUtils))
            case None => rootEntities
          }
        case (_, Some(childEntity)) =>
          childEntity.sanitizedName match {
            case Some(_) => Seq(this)
            case None =>
                zkUtils.getAllEntitiesWithConfig(root.entityPath + "/" + childEntity.entityType)
                       .map(name => ConfigEntity(root, Some(Entity(childEntity.entityType, Some(name)))))

          }
        case (_, None) =>
          Seq(this)
      }
    }

    override def toString: String = {
      root.toString + child.map(s => ", " + s.toString).getOrElse("")
    }
  }

  private[admin] def parseEntity(opts: ConfigCommandOptions): ConfigEntity = {
    val entityTypes = opts.options.valuesOf(opts.entityType).asScala
    if (entityTypes.head == ConfigType.User || entityTypes.head == ConfigType.Client)
      parseQuotaEntity(opts)
    else {
      // Exactly one entity type and at-most one entity name expected for other entities
      val name = if (opts.options.has(opts.entityName)) Some(opts.options.valueOf(opts.entityName)) else None
      ConfigEntity(Entity(entityTypes.head, name), None)
    }
  }

  private def parseQuotaEntity(opts: ConfigCommandOptions): ConfigEntity = {
    val types = opts.options.valuesOf(opts.entityType).asScala
    val namesIterator = opts.options.valuesOf(opts.entityName).iterator
    val names = opts.options.specs.asScala
                    .filter(spec => spec.options.contains("entity-name") || spec.options.contains("entity-default"))
                    .map(spec => if (spec.options.contains("entity-name")) namesIterator.next else "")

    if (opts.options.has(opts.alterOpt) && names.size != types.size)
      throw new IllegalArgumentException("--entity-name or --entity-default must be specified with each --entity-type for --alter")

    val reverse = types.size == 2 && types(0) == ConfigType.Client
    val entityTypes = if (reverse) types.reverse else types
    val sortedNames = (if (reverse && names.length == 2) names.reverse else names).iterator

    def sanitizeName(entityType: String, name: String) = {
      if (name.isEmpty)
        ConfigEntityName.Default
      else {
        entityType match {
          case ConfigType.User | ConfigType.Client => Sanitizer.sanitize(name)
          case _ => throw new IllegalArgumentException("Invalid entity type " + entityType)
        }
      }
    }

    val entities = entityTypes.map(t => Entity(t, if (sortedNames.hasNext) Some(sanitizeName(t, sortedNames.next)) else None))
    ConfigEntity(entities.head, if (entities.size > 1) Some(entities(1)) else None)
  }

  class ConfigCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)
    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over.")
            .withRequiredArg
            .describedAs("urls")
            .ofType(classOf[String])
    val alterOpt = parser.accepts("alter", "Alter the configuration for the entity.")
    val describeOpt = parser.accepts("describe", "List configs for the given entity.")
    val entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityDefault = parser.accepts("entity-default", "Default entity name for clients/users (applies to corresponding entity type in command line)")

    val nl = System.getProperty("line.separator")
    val addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
            "For entity_type '" + ConfigType.Topic + "': " + LogConfig.configNames.map("\t" + _).mkString(nl, nl, nl) +
            "For entity_type '" + ConfigType.Broker + "': " + DynamicConfig.Broker.names.asScala.map("\t" + _).mkString(nl, nl, nl) +
            "For entity_type '" + ConfigType.User + "': " + DynamicConfig.User.names.asScala.map("\t" + _).mkString(nl, nl, nl) +
            "For entity_type '" + ConfigType.Client + "': " + DynamicConfig.Client.names.asScala.map("\t" + _).mkString(nl, nl, nl) +
            s"Entity types '${ConfigType.User}' and '${ConfigType.Client}' may be specified together to update config for clients of a specific user.")
            .withRequiredArg
            .ofType(classOf[String])
    val deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
            .withRequiredArg
            .ofType(classOf[String])
            .withValuesSeparatedBy(',')
    val helpOpt = parser.accepts("help", "Print usage information.")
    val forceOpt = parser.accepts("force", "Suppress console prompts")
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
      val entityTypeVals = options.valuesOf(entityType).asScala
      if(options.has(alterOpt)) {
        if (entityTypeVals.contains(ConfigType.User) || entityTypeVals.contains(ConfigType.Client)) {
          if (!options.has(entityName) && !options.has(entityDefault))
            throw new IllegalArgumentException("--entity-name or --entity-default must be specified with --alter of users/clients")
        } else if (!options.has(entityName))
            throw new IllegalArgumentException(s"--entity-name must be specified with --alter of ${entityTypeVals}")

        val isAddConfigPresent: Boolean = options.has(addConfig)
        val isDeleteConfigPresent: Boolean = options.has(deleteConfig)
        if(! isAddConfigPresent && ! isDeleteConfigPresent)
          throw new IllegalArgumentException("At least one of --add-config or --delete-config must be specified with --alter")
      }
      entityTypeVals.foreach(entityTypeVal =>
        if (!ConfigType.all.contains(entityTypeVal))
          throw new IllegalArgumentException(s"Invalid entity-type ${entityTypeVal}, --entity-type must be one of ${ConfigType.all}")
      )
      if (entityTypeVals.isEmpty)
        throw new IllegalArgumentException("At least one --entity-type must be specified")
      else if (entityTypeVals.size > 1 && !entityTypeVals.toSet.equals(Set(ConfigType.User, ConfigType.Client)))
        throw new IllegalArgumentException(s"Only '${ConfigType.User}' and '${ConfigType.Client}' entity types may be specified together")
    }
  }
}
