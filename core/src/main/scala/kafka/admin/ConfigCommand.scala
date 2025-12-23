/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.admin

import joptsimple._
import kafka.server.DynamicConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.clients.admin.{Admin, AlterClientQuotasOptions, AlterConfigOp, AlterConfigsOptions, ConfigEntry, DescribeClusterOptions, DescribeConfigsOptions, ListConfigResourcesOptions, ListTopicsOptions, ScramCredentialInfo, UserScramCredentialDeletion, UserScramCredentialUpsertion, ScramMechanism => PublicScramMechanism}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{InvalidConfigurationException, UnsupportedVersionException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.utils.{Exit, Utils}
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.server.config.{ConfigType, QuotaConfig}
import org.apache.kafka.server.metrics.ClientMetricsConfigs
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}
import org.apache.kafka.storage.internals.log.LogConfig

import java.net.{InetAddress, UnknownHostException}
import java.nio.charset.StandardCharsets
import java.util.concurrent.{ExecutionException, TimeUnit}
import java.util.{Collections, Properties}
import scala.collection._
import scala.jdk.CollectionConverters._

/**
 * This script can be used to change configs for topics/clients/users/brokers/ips/client-metrics/groups dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: --topic <topic> OR --entity-type topics --entity-name <topic>
 *     <li> client: --client <client> OR --entity-type clients --entity-name <client-id>
 *     <li> user: --user <user-principal> OR --entity-type users --entity-name <user-principal>
 *     <li> <user, client>: --user <user-principal> --client <client-id> OR
 *                          --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>
 *     <li> broker: --broker <broker-id> OR --entity-type brokers --entity-name <broker-id>
 *     <li> broker-logger: --broker-logger <broker-id> OR --entity-type broker-loggers --entity-name <broker-id>
 *     <li> ip: --ip <ip> OR --entity-type ips --entity-name <ip>
 *     <li> client-metrics: --client-metrics <name> OR --entity-type client-metrics --entity-name <name>
 *     <li> group: --group <group> OR --entity-type groups --entity-name <group>
 * </ul>
 * --entity-type <users|clients|brokers|ips> --entity-default may be specified in place of --entity-type <users|clients|brokers|ips> --entity-name <entityName>
 * when describing or altering default configuration for users, clients, brokers, or ips, respectively.
 * Alternatively, --user-defaults, --client-defaults, --broker-defaults, or --ip-defaults may be specified in place of
 * --entity-type <users|clients|brokers|ips> --entity-default, respectively.
 */
object ConfigCommand extends Logging {

  private val BrokerDefaultEntityName = ""
  val BrokerLoggerConfigType = "broker-loggers"
  private val BrokerSupportedConfigTypes = ConfigType.values.map(_.value) :+ BrokerLoggerConfigType
  private val DefaultScramIterations = 4096
  private val TopicType = ConfigType.TOPIC.value
  private val ClientMetricsType = ConfigType.CLIENT_METRICS.value
  private val BrokerType = ConfigType.BROKER.value
  private val GroupType = ConfigType.GROUP.value
  private val UserType = ConfigType.USER.value
  private val ClientType = ConfigType.CLIENT.value
  private val IpType = ConfigType.IP.value

  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandOptions(args)

      CommandLineUtils.maybePrintHelpOrVersion(opts,
        "This tool helps to manipulate and describe entity config for a topic, client, user, broker, ip, client-metrics or group")

      opts.checkArgs()
      processCommand(opts)
    } catch {
      case e: UnsupportedVersionException =>
        logger.debug(s"Unsupported API encountered in server when executing config command with args '${args.mkString(" ")}'")
        System.err.println(e.getMessage)
        Exit.exit(1)

      case e @ (_: IllegalArgumentException | _: InvalidConfigurationException | _: OptionException) =>
        logger.debug(s"Failed config command with args '${args.mkString(" ")}'", e)
        System.err.println(e.getMessage)
        Exit.exit(1)

      case t: Throwable =>
        logger.debug(s"Error while executing config command with args '${args.mkString(" ")}'", t)
        System.err.println(s"Error while executing config command with args '${args.mkString(" ")}'")
        t.printStackTrace(System.err)
        Exit.exit(1)
    }
  }

  def parseConfigsToBeAdded(opts: ConfigCommandOptions): Properties = {
    val props = new Properties
    if (opts.options.has(opts.addConfigFile)) {
      val file = opts.options.valueOf(opts.addConfigFile)
      props ++= Utils.loadProps(file)
    }
    if (opts.options.has(opts.addConfig)) {
      // Split list by commas, but avoid those in [], then into KV pairs
      // Each KV pair is of format key=value, split them into key and value, using -1 as the limit for split() to
      // include trailing empty strings. This is to support empty value (e.g. 'ssl.endpoint.identification.algorithm=')
      val pattern = "(?=[^\\]]*(?:\\[|$))"
      val configsToBeAdded = opts.options.valueOf(opts.addConfig)
        .split("," + pattern)
        .map(_.split("""\s*=\s*""" + pattern, -1))
      require(configsToBeAdded.forall(config => config.length == 2), "Invalid entity config: all configs to be added must be in the format \"key=val\" or  \"key=[val1,val2]\" to group values which contain commas.")
      //Create properties, parsing square brackets from values if necessary
      configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).replaceAll("\\[?\\]?", "").trim))
    }
    validatePropsKey(props)
    props
  }

  def parseConfigsToBeDeleted(opts: ConfigCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfig)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfig).asScala.map(_.trim())
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  private def validatePropsKey(props: Properties): Unit = {
    props.keySet.forEach { propsKey =>
      // Allows the '$' symbol to support valid logger names for internal classes (e.g. org.apache.kafka.server.quota.ClientQuotaManager$ThrottledChannelReaper)
      if (!propsKey.toString.matches("[$a-zA-Z0-9._-]*")) {
        throw new IllegalArgumentException(
          s"Invalid character found for config key: $propsKey"
        )
      }
    }
  }

  private def processCommand(opts: ConfigCommandOptions): Unit = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    CommandLineUtils.initializeBootstrapProperties(opts.parser,
      opts.options,
      props,
      opts.bootstrapServerOpt,
      opts.bootstrapControllerOpt)
    val adminClient = Admin.create(props)

    if (opts.options.has(opts.alterOpt) && opts.entityTypes.size != opts.entityNames.size)
      throw new IllegalArgumentException(s"An entity name must be specified for every entity type")

    try {
      if (opts.options.has(opts.alterOpt))
        alterConfig(adminClient, opts)
      else if (opts.options.has(opts.describeOpt))
        describeConfig(adminClient, opts)
    } finally {
      adminClient.close()
    }
  }

  def alterConfig(adminClient: Admin, opts: ConfigCommandOptions): Unit = {
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    val entityTypeHead = entityTypes.head
    val entityNameHead = entityNames.head
    val configsToBeAddedMap = parseConfigsToBeAdded(opts).asScala.toMap // no need for mutability
    val configsToBeAdded = configsToBeAddedMap.map { case (k, v) => (k, new ConfigEntry(k, v)) }
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)

    entityTypeHead match {
      case TopicType | ClientMetricsType | BrokerType | GroupType =>
        val configResourceType = entityTypeHead match {
          case TopicType => ConfigResource.Type.TOPIC
          case ClientMetricsType => ConfigResource.Type.CLIENT_METRICS
          case BrokerType => ConfigResource.Type.BROKER
          case GroupType => ConfigResource.Type.GROUP
          case _ => throw new IllegalArgumentException(s"$entityNameHead is not a valid entity-type.")
        }
        try {
          alterResourceConfig(adminClient, entityTypeHead, entityNameHead, configsToBeDeleted, configsToBeAdded, configResourceType)
        } catch {
          case e: ExecutionException =>
            e.getCause match {
              case _: UnsupportedVersionException =>
                throw new UnsupportedVersionException(s"The ${ApiKeys.INCREMENTAL_ALTER_CONFIGS} API is not supported by the cluster. The API is supported starting from version 2.3.0."
                + " You may want to use an older version of this tool to interact with your cluster, or upgrade your brokers to version 2.3.0 or newer to avoid this error.")
              case _ => throw e
            }
          case e: Throwable => throw e
        }

      case BrokerLoggerConfigType =>
        val validLoggers = getResourceConfig(adminClient, entityTypeHead, entityNameHead, includeSynonyms = true, describeAll = false).map(_.name)
        // fail the command if any of the configured broker loggers do not exist
        val invalidBrokerLoggers = configsToBeDeleted.filterNot(validLoggers.contains) ++ configsToBeAdded.keys.filterNot(validLoggers.contains)
        if (invalidBrokerLoggers.nonEmpty)
          throw new InvalidConfigurationException(s"Invalid broker logger(s): ${invalidBrokerLoggers.mkString(",")}")

        val configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, entityNameHead)
        val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
        val addEntries = configsToBeAdded.values.map(k => new AlterConfigOp(k, AlterConfigOp.OpType.SET))
        val deleteEntries = configsToBeDeleted.map(k => new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE))
        val alterEntries = (deleteEntries ++ addEntries).asJavaCollection
        adminClient.incrementalAlterConfigs(Map(configResource -> alterEntries).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)

      case UserType | ClientType =>
        val hasQuotaConfigsToAdd = configsToBeAdded.keys.exists(QuotaConfig.isClientOrUserQuotaConfig)
        val scramConfigsToAddMap = configsToBeAdded.filter(entry => ScramMechanism.isScram(entry._1))
        val unknownConfigsToAdd = configsToBeAdded.keys.filterNot(key => ScramMechanism.isScram(key) || QuotaConfig.isClientOrUserQuotaConfig(key))
        val hasQuotaConfigsToDelete = configsToBeDeleted.exists(QuotaConfig.isClientOrUserQuotaConfig)
        val scramConfigsToDelete = configsToBeDeleted.filter(ScramMechanism.isScram)
        val unknownConfigsToDelete = configsToBeDeleted.filterNot(key => ScramMechanism.isScram(key) || QuotaConfig.isClientOrUserQuotaConfig(key))
        if (entityTypeHead == ClientType || entityTypes.size == 2) { // size==2 for case where users is specified first on the command line, before clients
          // either just a client or both a user and a client
          if (unknownConfigsToAdd.nonEmpty || scramConfigsToAddMap.nonEmpty)
            throw new IllegalArgumentException(s"Only quota configs can be added for '$ClientType' using --bootstrap-server. Unexpected config names: ${unknownConfigsToAdd ++ scramConfigsToAddMap.keys}")
          if (unknownConfigsToDelete.nonEmpty || scramConfigsToDelete.nonEmpty)
            throw new IllegalArgumentException(s"Only quota configs can be deleted for '$ClientType' using --bootstrap-server. Unexpected config names: ${unknownConfigsToDelete ++ scramConfigsToDelete}")
        } else { // ConfigType.User
          if (unknownConfigsToAdd.nonEmpty)
            throw new IllegalArgumentException(s"Only quota and SCRAM credential configs can be added for '$UserType' using --bootstrap-server. Unexpected config names: $unknownConfigsToAdd")
          if (unknownConfigsToDelete.nonEmpty)
            throw new IllegalArgumentException(s"Only quota and SCRAM credential configs can be deleted for '$UserType' using --bootstrap-server. Unexpected config names: $unknownConfigsToDelete")
          if (scramConfigsToAddMap.nonEmpty || scramConfigsToDelete.nonEmpty) {
            if (entityNames.exists(_.isEmpty)) // either --entity-type users --entity-default or --user-defaults
              throw new IllegalArgumentException("The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.")
            if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete)
              throw new IllegalArgumentException(s"Cannot alter both quota and SCRAM credential configs simultaneously for '$UserType' using --bootstrap-server.")
          }
        }

        if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete) {
          alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted)
        } else {
          // handle altering user SCRAM credential configs
          if (entityNames.size != 1)
            // should never happen, if we get here then it is a bug
            throw new IllegalStateException(s"Altering user SCRAM credentials should never occur for more zero or multiple users: $entityNames")
          alterUserScramCredentialConfigs(adminClient, entityNames.head, scramConfigsToAddMap, scramConfigsToDelete)
        }

      case IpType =>
        val unknownConfigs = (configsToBeAdded.keys ++ configsToBeDeleted).filterNot(key => QuotaConfig.ipConfigs.names.contains(key))
        if (unknownConfigs.nonEmpty)
          throw new IllegalArgumentException(s"Only connection quota configs can be added for '$IpType' using --bootstrap-server. Unexpected config names: ${unknownConfigs.mkString(",")}")
        alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted)

      case _ =>
        throw new IllegalArgumentException(s"Unsupported entity type: $entityTypeHead")
    }

    if (entityNameHead.nonEmpty)
      System.out.println(s"Completed updating config for ${entityTypeHead.dropRight(1)} $entityNameHead.")
    else
      System.out.println(s"Completed updating default config for $entityTypeHead in the cluster.")
  }

  private def alterUserScramCredentialConfigs(adminClient: Admin, user: String, scramConfigsToAddMap: Map[String, ConfigEntry], scramConfigsToDelete: Seq[String]) = {
    val deletions = scramConfigsToDelete.map(mechanismName =>
      new UserScramCredentialDeletion(user, PublicScramMechanism.fromMechanismName(mechanismName)))

    def iterationsAndPasswordBytes(mechanism: ScramMechanism, credentialStr: String): (Integer, Array[Byte]) = {
      val pattern = "(?:iterations=(\\-?[0-9]*),)?password=(.*)".r
      val (iterations, password) = credentialStr match {
        case pattern(iterations, password) => (if (iterations != null && iterations != "-1") iterations.toInt else DefaultScramIterations, password)
        case _ => throw new IllegalArgumentException(s"Invalid credential property $mechanism=$credentialStr")
      }
      if (iterations < mechanism.minIterations)
        throw new IllegalArgumentException(s"Iterations $iterations is less than the minimum ${mechanism.minIterations} required for ${mechanism.mechanismName}")
      (iterations, password.getBytes(StandardCharsets.UTF_8))
    }

    val upsertions = scramConfigsToAddMap.map { case (mechanismName, configEntry) =>
      val (iterations, passwordBytes) = iterationsAndPasswordBytes(ScramMechanism.forMechanismName(mechanismName), configEntry.value)
      new UserScramCredentialUpsertion(user, new ScramCredentialInfo(PublicScramMechanism.fromMechanismName(mechanismName), iterations), passwordBytes)
    }
    // we are altering only a single user by definition, so we don't have to worry about one user succeeding and another
    // failing; therefore just check the success of all the futures (since there will only be 1)
    adminClient.alterUserScramCredentials((deletions ++ upsertions).toList.asJava).all.get(60, TimeUnit.SECONDS)
  }

  private def alterQuotaConfigs(adminClient: Admin, entityTypes: List[String], entityNames: List[String], configsToBeAddedMap: Map[String, String], configsToBeDeleted: Seq[String]) = {
    // handle altering client/user quota configs
    val oldConfig = getClientQuotasConfig(adminClient, entityTypes, entityNames)

    val invalidConfigs = configsToBeDeleted.filterNot(oldConfig.contains)
    if (invalidConfigs.nonEmpty)
      throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

    val alterEntityTypes = entityTypes.map {
      case UserType => ClientQuotaEntity.USER
      case ClientType => ClientQuotaEntity.CLIENT_ID
      case IpType => ClientQuotaEntity.IP
      case entType => throw new IllegalArgumentException(s"Unexpected entity type: $entType")
    }
    val alterEntityNames = entityNames.map(en => if (en.nonEmpty) en else null)

    // Explicitly populate a HashMap to ensure nulls are recorded properly.
    val alterEntityMap = new java.util.HashMap[String, String]
    alterEntityTypes.zip(alterEntityNames).foreach { case (k, v) => alterEntityMap.put(k, v) }
    val entity = new ClientQuotaEntity(alterEntityMap)

    val alterOptions = new AlterClientQuotasOptions().validateOnly(false)
    val alterOps = (configsToBeAddedMap.map { case (key, value) =>
      val doubleValue = try value.toDouble catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"Cannot parse quota configuration value for $key: $value")
      }
      new ClientQuotaAlteration.Op(key, doubleValue)
    } ++ configsToBeDeleted.map(key => new ClientQuotaAlteration.Op(key, null))).asJavaCollection

    adminClient.alterClientQuotas(Collections.singleton(new ClientQuotaAlteration(entity, alterOps)), alterOptions)
      .all().get(60, TimeUnit.SECONDS)
  }

  def describeConfig(adminClient: Admin, opts: ConfigCommandOptions): Unit = {
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    val describeAll = opts.options.has(opts.allOpt)

    entityTypes.head match {
      case TopicType | BrokerType | BrokerLoggerConfigType | ClientMetricsType | GroupType =>
        describeResourceConfig(adminClient, entityTypes.head, entityNames.headOption, describeAll)
      case UserType | ClientType =>
        describeClientQuotaAndUserScramCredentialConfigs(adminClient, entityTypes, entityNames)
      case IpType =>
        describeQuotaConfigs(adminClient, entityTypes, entityNames)
      case entityType => throw new IllegalArgumentException(s"Invalid entity type: $entityType")
    }
  }

  private def describeResourceConfig(adminClient: Admin, entityType: String, entityName: Option[String], describeAll: Boolean): Unit = {
    if (!describeAll) {
      entityName.foreach { name =>
        entityType match {
          case TopicType =>
            Topic.validate(name)
            if (!adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names.get.contains(name)) {
              System.out.println(s"The ${entityType.dropRight(1)} '$name' doesn't exist and doesn't have dynamic config.")
              return
            }
          case BrokerType | BrokerLoggerConfigType =>
            if (adminClient.describeCluster.nodes.get.stream.anyMatch(_.idString == name)) {
              // valid broker id
            } else if (name == BrokerDefaultEntityName) {
              // default broker configs
            } else {
              System.out.println(s"The ${entityType.dropRight(1)} '$name' doesn't exist and doesn't have dynamic config.")
              return
            }
          case ClientMetricsType =>
            if (adminClient.listConfigResources(java.util.Set.of(ConfigResource.Type.CLIENT_METRICS), new ListConfigResourcesOptions).all.get
              .stream.noneMatch(_.name == name)) {
              System.out.println(s"The ${entityType.dropRight(1)} '$name' doesn't exist and doesn't have dynamic config.")
              return
            }
          case GroupType =>
            if (adminClient.listGroups().all.get.stream.noneMatch(_.groupId() == name) &&
              adminClient.listConfigResources(java.util.Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions).all.get
                .stream.noneMatch(_.name == name)) {
              System.out.println(s"The ${entityType.dropRight(1)} '$name' doesn't exist and doesn't have dynamic config.")
              return
            }
          case entityType => throw new IllegalArgumentException(s"Invalid entity type: $entityType")
        }
      }
    }

    val entities = entityName
      .map(name => List(name))
      .getOrElse(entityType match {
        case TopicType =>
          adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get().asScala.toSeq
        case BrokerType | BrokerLoggerConfigType =>
          adminClient.describeCluster(new DescribeClusterOptions()).nodes().get().asScala.map(_.idString).toSeq :+ BrokerDefaultEntityName
        case ClientMetricsType =>
          adminClient.listConfigResources(java.util.Set.of(ConfigResource.Type.CLIENT_METRICS), new ListConfigResourcesOptions).all().get().asScala.map(_.name).toSeq
        case GroupType =>
          adminClient.listGroups().all.get.asScala.map(_.groupId).toSet ++
            adminClient.listConfigResources(java.util.Set.of(ConfigResource.Type.GROUP), new ListConfigResourcesOptions).all().get().asScala.map(_.name).toSet
        case entityType => throw new IllegalArgumentException(s"Invalid entity type: $entityType")
      })

    entities.foreach { entity =>
      entity match {
        case BrokerDefaultEntityName =>
          System.out.println(s"Default configs for $entityType in the cluster are:")
        case _ =>
          val configSourceStr = if (describeAll) "All" else "Dynamic"
          System.out.println(s"$configSourceStr configs for ${entityType.dropRight(1)} $entity are:")
      }
      getResourceConfig(adminClient, entityType, entity, includeSynonyms = true, describeAll).foreach { entry =>
        val synonyms = entry.synonyms.asScala.map(synonym => s"${synonym.source}:${synonym.name}=${synonym.value}").mkString(", ")
        System.out.println(s"  ${entry.name}=${entry.value} sensitive=${entry.isSensitive} synonyms={$synonyms}")
      }
    }
  }

  private def alterResourceConfig(adminClient: Admin, entityTypeHead: String, entityNameHead: String, configsToBeDeleted: Seq[String], configsToBeAdded: Map[String, ConfigEntry], resourceType: ConfigResource.Type): Unit = {
    val oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, includeSynonyms = false, describeAll = false)
      .map { entry => (entry.name, entry) }.toMap

    // fail the command if any of the configs to be deleted does not exist
    val invalidConfigs = configsToBeDeleted.filterNot(oldConfig.contains)
    if (invalidConfigs.nonEmpty)
      throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

    val configResource = new ConfigResource(resourceType, entityNameHead)
    val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
    val addEntries = configsToBeAdded.values.map(k => new AlterConfigOp(k, AlterConfigOp.OpType.SET))
    val deleteEntries = configsToBeDeleted.map(k => new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE))
    val alterEntries = (deleteEntries ++ addEntries).asJavaCollection
    adminClient.incrementalAlterConfigs(Map(configResource -> alterEntries).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)
  }

  private def getResourceConfig(adminClient: Admin, entityType: String, entityName: String, includeSynonyms: Boolean, describeAll: Boolean) = {
    def validateBrokerId(): Unit = try entityName.toInt catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"The entity name for $entityType must be a valid integer broker id, found: $entityName")
    }

    val (configResourceType, dynamicConfigSource) = entityType match {
      case TopicType =>
        if (entityName.nonEmpty)
          Topic.validate(entityName)
        (ConfigResource.Type.TOPIC, Some(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
      case BrokerType => entityName match {
        case BrokerDefaultEntityName =>
          (ConfigResource.Type.BROKER, Some(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
        case _ =>
          validateBrokerId()
          (ConfigResource.Type.BROKER, Some(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG))
      }
      case BrokerLoggerConfigType =>
        if (entityName.nonEmpty)
          validateBrokerId()
        (ConfigResource.Type.BROKER_LOGGER, None)
      case ClientMetricsType =>
        (ConfigResource.Type.CLIENT_METRICS, Some(ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG))
      case GroupType =>
        (ConfigResource.Type.GROUP, Some(ConfigEntry.ConfigSource.DYNAMIC_GROUP_CONFIG))
      case entityType => throw new IllegalArgumentException(s"Invalid entity type: $entityType")
    }

    val configSourceFilter = if (describeAll)
      None
    else
      dynamicConfigSource

    val configResource = new ConfigResource(configResourceType, entityName)
    val describeOptions = new DescribeConfigsOptions().includeSynonyms(includeSynonyms)
    val configs = adminClient.describeConfigs(Collections.singleton(configResource), describeOptions)
      .all.get(30, TimeUnit.SECONDS)
    configs.get(configResource).entries.asScala
      .filter(entry => configSourceFilter match {
        case Some(configSource) => entry.source == configSource
        case None => true
      }).toSeq.sortBy(entry => entry.name())
  }

  private def describeQuotaConfigs(adminClient: Admin, entityTypes: List[String], entityNames: List[String]): Unit = {
    val quotaConfigs = getAllClientQuotasConfigs(adminClient, entityTypes, entityNames)
    quotaConfigs.foreachEntry { (entity, entries) =>
      val entityEntries = entity.entries.asScala

      def entitySubstr(entityType: String): Option[String] =
        entityEntries.get(entityType).map { name =>
          val typeStr = entityType match {
            case ClientQuotaEntity.USER => "user-principal"
            case ClientQuotaEntity.CLIENT_ID => "client-id"
            case ClientQuotaEntity.IP => "ip"
          }
          if (name != null) s"$typeStr '$name'"
          else s"the default $typeStr"
        }

      val entityStr = (entitySubstr(ClientQuotaEntity.USER) ++
                       entitySubstr(ClientQuotaEntity.CLIENT_ID) ++
                       entitySubstr(ClientQuotaEntity.IP)).mkString(", ")
      val entriesStr = entries.asScala.map(e => s"${e._1}=${e._2}").mkString(", ")
      System.out.println(s"Quota configs for $entityStr are $entriesStr")
    }
  }

  private def describeClientQuotaAndUserScramCredentialConfigs(adminClient: Admin, entityTypes: List[String], entityNames: List[String]): Unit = {
    describeQuotaConfigs(adminClient, entityTypes, entityNames)
    // we describe user SCRAM credentials only when we are not describing client information
    // and we are not given either --entity-default or --user-defaults
    if (!entityTypes.contains(ClientType) && !entityNames.contains("")) {
      val result = adminClient.describeUserScramCredentials(entityNames.asJava)
      result.users.get(30, TimeUnit.SECONDS).asScala.foreach(user => {
        try {
          val description = result.description(user).get(30, TimeUnit.SECONDS)
          val descriptionText = description.credentialInfos.asScala.map(info => s"${info.mechanism.mechanismName}=iterations=${info.iterations}").mkString(", ")
          System.out.println(s"SCRAM credential configs for user-principal '$user' are $descriptionText")
        } catch {
          case e: Exception => System.out.println(s"Error retrieving SCRAM credential configs for user-principal '$user': ${e.getClass.getSimpleName}: ${e.getMessage}")
        }
      })
    }
  }

  private def getClientQuotasConfig(adminClient: Admin, entityTypes: List[String], entityNames: List[String]): Map[String, java.lang.Double] = {
    if (entityTypes.size != entityNames.size)
      throw new IllegalArgumentException("Exactly one entity name must be specified for every entity type")
    getAllClientQuotasConfigs(adminClient, entityTypes, entityNames).headOption.map(_._2.asScala).getOrElse(Map.empty)
  }

  private def getAllClientQuotasConfigs(adminClient: Admin, entityTypes: List[String], entityNames: List[String]) = {
    val components = entityTypes.map(Some(_)).zipAll(entityNames.map(Some(_)), None, None).map { case (entityTypeOpt, entityNameOpt) =>
      val entityType = entityTypeOpt match {
        case Some(UserType) => ClientQuotaEntity.USER
        case Some(ClientType) => ClientQuotaEntity.CLIENT_ID
        case Some(IpType) => ClientQuotaEntity.IP
        case Some(_) => throw new IllegalArgumentException(s"Unexpected entity type ${entityTypeOpt.get}")
        case None => throw new IllegalArgumentException("More entity names specified than entity types")
      }
      entityNameOpt match {
        case Some("") => ClientQuotaFilterComponent.ofDefaultEntity(entityType)
        case Some(name) => ClientQuotaFilterComponent.ofEntity(entityType, name)
        case None => ClientQuotaFilterComponent.ofEntityType(entityType)
      }
    }

    adminClient.describeClientQuotas(ClientQuotaFilter.containsOnly(components.asJava)).entities.get(30, TimeUnit.SECONDS).asScala
  }


  class ConfigCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val bootstrapServerOpt: OptionSpec[String] = parser.accepts("bootstrap-server", "The Kafka servers to connect to.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val bootstrapControllerOpt: OptionSpec[String] = parser.accepts("bootstrap-controller", "The Kafka controllers to connect to.")
      .withRequiredArg
      .describedAs("controller to connect to")
      .ofType(classOf[String])
    val commandConfigOpt: OptionSpec[String] = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
      "This is used only with --bootstrap-server option for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])
    val alterOpt: OptionSpecBuilder = parser.accepts("alter", "Alter the configuration for the entity.")
    val describeOpt: OptionSpecBuilder = parser.accepts("describe", "List configs for the given entity.")
    val allOpt: OptionSpecBuilder = parser.accepts("all", "List all configs for the given entity, including static configs if available.")

    val entityType: OptionSpec[String] = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers/ips/client-metrics/groups)")
      .withRequiredArg
      .ofType(classOf[String])
    val entityName: OptionSpec[String] = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id/ip/client metrics/group id)")
      .withRequiredArg
      .ofType(classOf[String])
    private val entityDefault: OptionSpecBuilder = parser.accepts("entity-default", "Default entity name for clients/users/brokers/ips (applies to corresponding entity type)")

    private val nl: String = System.lineSeparator()
    val addConfig: OptionSpec[String] = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
      "For entity-type '" + TopicType + "': " + LogConfig.nonInternalConfigNames.asScala.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + BrokerType + "': " + DynamicConfig.Broker.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + UserType + "': " + QuotaConfig.scramMechanismsPlusUserAndClientQuotaConfigs().names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ClientType + "': " + QuotaConfig.userAndClientQuotaConfigs().names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + IpType + "': " + QuotaConfig.ipConfigs.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ClientMetricsType + "': " + ClientMetricsConfigs.configDef().names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + GroupType + "': " + GroupConfig.configDef().names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      s"Entity types '$UserType' and '$ClientType' may be specified together to update config for clients of a specific user.")
      .withRequiredArg
      .ofType(classOf[String])
    val addConfigFile: OptionSpec[String] = parser.accepts("add-config-file", "Path to a properties file with configs to add. See add-config for a list of valid configurations.")
      .withRequiredArg
      .ofType(classOf[String])
    val deleteConfig: OptionSpec[String] = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
      .withRequiredArg
      .ofType(classOf[String])
      .withValuesSeparatedBy(',')
    val topic: OptionSpec[String] = parser.accepts("topic", "The topic's name.")
      .withRequiredArg
      .ofType(classOf[String])
    val client: OptionSpec[String] = parser.accepts("client", "The client's ID.")
      .withRequiredArg
      .ofType(classOf[String])
    private val clientDefaults = parser.accepts("client-defaults", "The config defaults for all clients.")
    val user: OptionSpec[String] = parser.accepts("user", "The user's principal name.")
      .withRequiredArg
      .ofType(classOf[String])
    private val userDefaults = parser.accepts("user-defaults", "The config defaults for all users.")
    val broker: OptionSpec[String] = parser.accepts("broker", "The broker's ID.")
      .withRequiredArg
      .ofType(classOf[String])
    private val brokerDefaults = parser.accepts("broker-defaults", "The config defaults for all brokers.")
    private val brokerLogger = parser.accepts("broker-logger", "The broker's ID for its logger config.")
      .withRequiredArg
      .ofType(classOf[String])
    private val ipDefaults = parser.accepts("ip-defaults", "The config defaults for all IPs.")
    val ip: OptionSpec[String] = parser.accepts("ip", "The IP address.")
      .withRequiredArg
      .ofType(classOf[String])
    val group: OptionSpec[String] = parser.accepts("group", "The group's ID.")
      .withRequiredArg
      .ofType(classOf[String])
    val clientMetrics: OptionSpec[String] = parser.accepts("client-metrics", "The client metrics config resource name.")
      .withRequiredArg
      .ofType(classOf[String])
    options = parser.parse(args : _*)

    private val entityFlags = List((topic, TopicType),
      (client, ClientType),
      (user, UserType),
      (broker, BrokerType),
      (brokerLogger, BrokerLoggerConfigType),
      (ip, IpType),
      (clientMetrics, ClientMetricsType),
      (group, GroupType))

    private val entityDefaultsFlags = List((clientDefaults, ClientType),
      (userDefaults, UserType),
      (brokerDefaults, BrokerType),
      (ipDefaults, IpType))

    private[admin] def entityTypes: List[String] = {
      options.valuesOf(entityType).asScala.toList ++
        (entityFlags ++ entityDefaultsFlags).filter(entity => options.has(entity._1)).map(_._2)
    }

    private[admin] def entityNames: List[String] = {
      val namesIterator = options.valuesOf(entityName).iterator
      options.specs.asScala
        .filter(spec => spec.options.contains("entity-name") || spec.options.contains("entity-default"))
        .map(spec => if (spec.options.contains("entity-name")) namesIterator.next else "").toList ++
      entityFlags
        .filter(entity => options.has(entity._1))
        .map(entity => options.valueOf(entity._1)) ++
      entityDefaultsFlags
        .filter(entity => options.has(entity._1))
        .map(_ => "")
    }

    def checkArgs(): Unit = {
      // should have exactly one action
      val actions = Seq(alterOpt, describeOpt).count(options.has _)
      if (actions != 1)
        CommandLineUtils.printUsageAndExit(parser, "Command must include exactly one action: --describe, --alter")
      // check required args
      CommandLineUtils.checkInvalidArgs(parser, options, alterOpt, describeOpt)
      CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, alterOpt, addConfig, deleteConfig)

      val entityTypeVals = entityTypes
      if (entityTypeVals.size != entityTypeVals.distinct.size)
        throw new IllegalArgumentException(s"Duplicate entity type(s) specified: ${entityTypeVals.diff(entityTypeVals.distinct).mkString(",")}")

      val (allowedEntityTypes, connectOptString) =
        if (options.has(bootstrapServerOpt) || options.has(bootstrapControllerOpt)) {
          (BrokerSupportedConfigTypes, "--bootstrap-server or --bootstrap-controller")
        } else {
          throw new IllegalArgumentException("Either --bootstrap-server or --bootstrap-controller must be specified.")
        }

      entityTypeVals.foreach(entityTypeVal =>
        if (!allowedEntityTypes.contains(entityTypeVal))
          throw new IllegalArgumentException(s"Invalid entity type $entityTypeVal, the entity type must be one of ${allowedEntityTypes.mkString(", ")} with a $connectOptString argument")
      )
      if (entityTypeVals.isEmpty)
        throw new IllegalArgumentException("At least one entity type must be specified")
      else if (entityTypeVals.size > 1 && !entityTypeVals.toSet.equals(Set(UserType, ClientType)))
        throw new IllegalArgumentException(s"Only '$UserType' and '$ClientType' entity types may be specified together")

      if ((options.has(entityName) || options.has(entityType) || options.has(entityDefault)) &&
        (entityFlags ++ entityDefaultsFlags).exists(entity => options.has(entity._1)))
        throw new IllegalArgumentException("--entity-{type,name,default} should not be used in conjunction with specific entity flags")

      val hasEntityName = entityNames.exists(_.nonEmpty)
      val hasEntityDefault = entityNames.exists(_.isEmpty)

      val numConnectOptions = (if (options.has(bootstrapServerOpt)) 1 else 0) +
        (if (options.has(bootstrapControllerOpt)) 1 else 0)
      if (numConnectOptions > 1)
        throw new IllegalArgumentException("Only one of --bootstrap-server or --bootstrap-controller can be specified")
      if (hasEntityName && (entityTypeVals.contains(BrokerType) || entityTypeVals.contains(BrokerLoggerConfigType))) {
        Seq(entityName, broker, brokerLogger).filter(options.has(_)).map(options.valueOf(_)).foreach { brokerId =>
          try brokerId.toInt catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"The entity name for ${entityTypeVals.head} must be a valid integer broker id, but it is: $brokerId")
          }
        }
      }

      if (hasEntityName && entityTypeVals.contains(IpType)) {
        Seq(entityName, ip).filter(options.has(_)).map(options.valueOf(_)).foreach { ipEntity =>
          if (!isValidIpEntity(ipEntity))
            throw new IllegalArgumentException(s"The entity name for ${entityTypeVals.head} must be a valid IP or resolvable host, but it is: $ipEntity")
        }
      }

      if (options.has(describeOpt)) {
        if (!(entityTypeVals.contains(UserType) ||
          entityTypeVals.contains(ClientType) ||
          entityTypeVals.contains(BrokerType) ||
          entityTypeVals.contains(IpType)) && options.has(entityDefault)) {
          throw new IllegalArgumentException(s"--entity-default must not be specified with --describe of ${entityTypeVals.mkString(",")}")
        }

        if (entityTypeVals.contains(BrokerLoggerConfigType) && !hasEntityName)
          throw new IllegalArgumentException(s"An entity name must be specified with --describe of ${entityTypeVals.mkString(",")}")
      }

      if (options.has(alterOpt)) {
        if (entityTypeVals.contains(UserType) ||
            entityTypeVals.contains(ClientType) ||
            entityTypeVals.contains(BrokerType) ||
            entityTypeVals.contains(IpType)) {
          if (!hasEntityName && !hasEntityDefault)
            throw new IllegalArgumentException("An entity-name or default entity must be specified with --alter of users, clients, brokers or ips")
        } else if (!hasEntityName)
          throw new IllegalArgumentException(s"An entity name must be specified with --alter of ${entityTypeVals.mkString(",")}")

        val isAddConfigPresent = options.has(addConfig)
        val isAddConfigFilePresent = options.has(addConfigFile)
        val isDeleteConfigPresent = options.has(deleteConfig)

        if (isAddConfigPresent && isAddConfigFilePresent)
          throw new IllegalArgumentException("Only one of --add-config or --add-config-file must be specified")

        if (!isAddConfigPresent && !isAddConfigFilePresent && !isDeleteConfigPresent)
          throw new IllegalArgumentException("At least one of --add-config, --add-config-file, or --delete-config must be specified with --alter")
      }
    }
  }

  def isValidIpEntity(ip: String): Boolean = {
    try {
      InetAddress.getByName(ip)
    } catch {
      case _: UnknownHostException => return false
    }
    true
  }
}
