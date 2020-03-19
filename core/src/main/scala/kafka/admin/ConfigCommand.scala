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

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import joptsimple._
import kafka.common.Config
import kafka.log.LogConfig
import kafka.server.{ConfigEntityName, ConfigType, Defaults, DynamicBrokerConfig, DynamicConfig, KafkaConfig}
import kafka.utils.{CommandDefaultOptions, CommandLineUtils, Exit, PasswordEncoder}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AlterClientQuotasOptions, AlterConfigOp, AlterConfigsOptions, ConfigEntry, DescribeClusterOptions, DescribeConfigsOptions, ListTopicsOptions, Config => JConfig}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.scram.internals.{ScramCredentialUtils, ScramFormatter, ScramMechanism}
import org.apache.kafka.common.utils.{Sanitizer, Time, Utils}
import org.apache.zookeeper.client.ZKClientConfig

import scala.collection.JavaConverters._
import scala.collection._


/**
 * This script can be used to change configs for topics/clients/users/brokers dynamically
 * An entity described or altered by the command may be one of:
 * <ul>
 *     <li> topic: --topic <topic> OR --entity-type topics --entity-name <topic>
 *     <li> client: --client <client> OR --entity-type clients --entity-name <client-id>
 *     <li> user: --user <user-principal> OR --entity-type users --entity-name <user-principal>
 *     <li> <user, client>: --user <user-principal> --client <client-id> OR
 *                          --entity-type users --entity-name <user-principal> --entity-type clients --entity-name <client-id>
 *     <li> broker: --broker <broker-id> OR --entity-type brokers --entity-name <broker-id>
 *     <li> broker-logger: --broker-logger <broker-id> OR --entity-type broker-loggers --entity-name <broker-id>
 * </ul>
 * --user-defaults, --client-defaults, or --broker-defaults may be when describing or altering default configuration for users,
 * clients, and brokers, respectively. Alternatively, --entity-default may be used instead of --entity-name.
 *
 */
object ConfigCommand extends Config {

  val BrokerLoggerConfigType = "broker-loggers"
  val BrokerSupportedConfigTypes = ConfigType.all :+ BrokerLoggerConfigType
  val ZkSupportedConfigTypes = ConfigType.all
  val DefaultScramIterations = 4096
  // Dynamic broker configs can only be updated using the new AdminClient once brokers have started
  // so that configs may be fully validated. Prior to starting brokers, updates may be performed using
  // ZooKeeper for bootstrapping. This allows all password configs to be stored encrypted in ZK,
  // avoiding clear passwords in server.properties. For consistency with older versions, quota-related
  // broker configs can still be updated using ZooKeeper at any time. ConfigCommand will be migrated
  // to the new AdminClient later for these configs (KIP-248).
  val BrokerConfigsUpdatableUsingZooKeeperWhileBrokerRunning = Set(
    DynamicConfig.Broker.LeaderReplicationThrottledRateProp,
    DynamicConfig.Broker.FollowerReplicationThrottledRateProp,
    DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp,
    DynamicConfig.Broker.LeaderReplicationThrottledProp,
    DynamicConfig.Broker.FollowerReplicationThrottledProp)

  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandOptions(args)

      CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to manipulate and describe entity config for a topic, client, user or broker")

      opts.checkArgs()

      if (opts.options.has(opts.zkConnectOpt)) {
        println(s"Warning: --zookeeper is deprecated and will be removed in a future version of Kafka.")
        println(s"Use --bootstrap-server instead to specify a broker to connect to.")
        processCommandWithZk(opts.options.valueOf(opts.zkConnectOpt), opts)
      } else {
        processCommand(opts)
      }
    } catch {
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

  private def processCommandWithZk(zkConnectString: String, opts: ConfigCommandOptions): Unit = {
    val zkClientConfig = ZkSecurityMigrator.createZkClientConfigFromOption(opts.options, opts.zkTlsConfigFile)
      .getOrElse(new ZKClientConfig())
    val zkClient = KafkaZkClient(zkConnectString, JaasUtils.isZkSaslEnabled || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig), 30000, 30000,
      Int.MaxValue, Time.SYSTEM, zkClientConfig = Some(zkClientConfig))
    val adminZkClient = new AdminZkClient(zkClient)
    try {
      if (opts.options.has(opts.alterOpt))
        alterConfigWithZk(zkClient, opts, adminZkClient)
      else if (opts.options.has(opts.describeOpt))
        describeConfigWithZk(zkClient, opts, adminZkClient)
    } finally {
      zkClient.close()
    }
  }

  private[admin] def alterConfigWithZk(zkClient: KafkaZkClient, opts: ConfigCommandOptions, adminZkClient: AdminZkClient): Unit = {
    val configsToBeAdded = parseConfigsToBeAdded(opts)
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)
    val entity = parseEntity(opts)
    val entityType = entity.root.entityType
    val entityName = entity.fullSanitizedName

    if (entityType == ConfigType.User)
      preProcessScramCredentials(configsToBeAdded)
    else if (entityType == ConfigType.Broker) {
      // Replication quota configs may be updated using ZK at any time. Other dynamic broker configs
      // may be updated using ZooKeeper only if the corresponding broker is not running. Dynamic broker
      // configs at cluster-default level may be configured using ZK only if there are no brokers running.
      val dynamicBrokerConfigs = configsToBeAdded.asScala.keySet.filterNot(BrokerConfigsUpdatableUsingZooKeeperWhileBrokerRunning.contains)
      if (dynamicBrokerConfigs.nonEmpty) {
        val perBrokerConfig = entityName != ConfigEntityName.Default
        val errorMessage = s"--bootstrap-server option must be specified to update broker configs $dynamicBrokerConfigs."
        val info = "Broker configuration updates using ZooKeeper are supported for bootstrapping before brokers" +
          " are started to enable encrypted password configs to be stored in ZooKeeper."
        if (perBrokerConfig) {
          adminZkClient.parseBroker(entityName).foreach { brokerId =>
            require(zkClient.getBroker(brokerId).isEmpty, s"$errorMessage when broker $entityName is running. $info")
          }
        } else {
          require(zkClient.getAllBrokersInCluster.isEmpty, s"$errorMessage for default cluster if any broker is running. $info")
        }
        preProcessBrokerConfigs(configsToBeAdded, perBrokerConfig)
      }
    }

    // compile the final set of configs
    val configs = adminZkClient.fetchEntityConfig(entityType, entityName)

    // fail the command if any of the configs to be deleted does not exist
    val invalidConfigs = configsToBeDeleted.filterNot(configs.containsKey(_))
    if (invalidConfigs.nonEmpty)
      throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

    configs ++= configsToBeAdded
    configsToBeDeleted.foreach(configs.remove(_))

    adminZkClient.changeConfigs(entityType, entityName, configs)

    println(s"Completed updating config for entity: $entity.")
  }

  private def preProcessScramCredentials(configsToBeAdded: Properties): Unit = {
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

  private[admin] def createPasswordEncoder(encoderConfigs: Map[String, String]): PasswordEncoder = {
    encoderConfigs.get(KafkaConfig.PasswordEncoderSecretProp)
    val encoderSecret = encoderConfigs.getOrElse(KafkaConfig.PasswordEncoderSecretProp,
      throw new IllegalArgumentException("Password encoder secret not specified"))
    new PasswordEncoder(new Password(encoderSecret),
      None,
      encoderConfigs.get(KafkaConfig.PasswordEncoderCipherAlgorithmProp).getOrElse(Defaults.PasswordEncoderCipherAlgorithm),
      encoderConfigs.get(KafkaConfig.PasswordEncoderKeyLengthProp).map(_.toInt).getOrElse(Defaults.PasswordEncoderKeyLength),
      encoderConfigs.get(KafkaConfig.PasswordEncoderIterationsProp).map(_.toInt).getOrElse(Defaults.PasswordEncoderIterations))
  }

  /**
   * Pre-process broker configs provided to convert them to persistent format.
   * Password configs are encrypted using the secret `KafkaConfig.PasswordEncoderSecretProp`.
   * The secret is removed from `configsToBeAdded` and will not be persisted in ZooKeeper.
   */
  private def preProcessBrokerConfigs(configsToBeAdded: Properties, perBrokerConfig: Boolean): Unit = {
    val passwordEncoderConfigs = new Properties
    passwordEncoderConfigs ++= configsToBeAdded.asScala.filter { case (key, _) => key.startsWith("password.encoder.") }
    if (!passwordEncoderConfigs.isEmpty) {
      info(s"Password encoder configs ${passwordEncoderConfigs.keySet} will be used for encrypting" +
        " passwords, but will not be stored in ZooKeeper.")
      passwordEncoderConfigs.asScala.keySet.foreach(configsToBeAdded.remove)
    }

    DynamicBrokerConfig.validateConfigs(configsToBeAdded, perBrokerConfig)
    val passwordConfigs = configsToBeAdded.asScala.keySet.filter(DynamicBrokerConfig.isPasswordConfig)
    if (passwordConfigs.nonEmpty) {
      require(passwordEncoderConfigs.containsKey(KafkaConfig.PasswordEncoderSecretProp),
        s"${KafkaConfig.PasswordEncoderSecretProp} must be specified to update $passwordConfigs." +
          " Other password encoder configs like cipher algorithm and iterations may also be specified" +
          " to override the default encoding parameters. Password encoder configs will not be persisted" +
          " in ZooKeeper."
      )

      val passwordEncoder = createPasswordEncoder(passwordEncoderConfigs.asScala)
      passwordConfigs.foreach { configName =>
        val encodedValue = passwordEncoder.encode(new Password(configsToBeAdded.getProperty(configName)))
        configsToBeAdded.setProperty(configName, encodedValue)
      }
    }
  }

  private def describeConfigWithZk(zkClient: KafkaZkClient, opts: ConfigCommandOptions, adminZkClient: AdminZkClient): Unit = {
    val configEntity = parseEntity(opts)
    val describeAllUsers = configEntity.root.entityType == ConfigType.User && !configEntity.root.sanitizedName.isDefined && !configEntity.child.isDefined
    val entities = configEntity.getAllEntities(zkClient)
    for (entity <- entities) {
      val configs = adminZkClient.fetchEntityConfig(entity.root.entityType, entity.fullSanitizedName)
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
      // Split list by commas, but avoid those in [], then into KV pairs
      // Each KV pair is of format key=value, split them into key and value, using -1 as the limit for split() to
      // include trailing empty strings. This is to support empty value (e.g. 'ssl.endpoint.identification.algorithm=')
      val pattern = "(?=[^\\]]*(?:\\[|$))"
      val configsToBeAdded = opts.options.valueOf(opts.addConfig)
        .split("," + pattern)
        .map(_.split("""\s*=\s*""" + pattern, -1))
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

  private def processCommand(opts: ConfigCommandOptions): Unit = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
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

  private[admin] def alterConfig(adminClient: Admin, opts: ConfigCommandOptions): Unit = {
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    val entityTypeHead = entityTypes.head
    val entityNameHead = entityNames.head
    val configsToBeAddedMap = parseConfigsToBeAdded(opts).asScala
    val configsToBeAdded = configsToBeAddedMap.map { case (k, v) => (k, new ConfigEntry(k, v)) }
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)

    entityTypeHead match {
      case ConfigType.Topic =>
        val oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, includeSynonyms = false, describeAll = false)
          .map { entry => (entry.name, entry) }.toMap

        // fail the command if any of the configs to be deleted does not exist
        val invalidConfigs = configsToBeDeleted.filterNot(oldConfig.contains)
        if (invalidConfigs.nonEmpty)
          throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

        val configResource = new ConfigResource(ConfigResource.Type.TOPIC, entityNameHead)
        val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
        val alterEntries = (configsToBeAdded.values.map(new AlterConfigOp(_, AlterConfigOp.OpType.SET))
          ++ configsToBeDeleted.map { k => new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE) }
        ).asJavaCollection
        adminClient.incrementalAlterConfigs(Map(configResource -> alterEntries).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)

      case ConfigType.Broker =>
        val oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, includeSynonyms = false, describeAll = false)
          .map { entry => (entry.name, entry) }.toMap

        // fail the command if any of the configs to be deleted does not exist
        val invalidConfigs = configsToBeDeleted.filterNot(oldConfig.contains)
        if (invalidConfigs.nonEmpty)
          throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

        val newEntries = oldConfig ++ configsToBeAdded -- configsToBeDeleted
        val sensitiveEntries = newEntries.filter(_._2.value == null)
        if (sensitiveEntries.nonEmpty)
          throw new InvalidConfigurationException(s"All sensitive broker config entries must be specified for --alter, missing entries: ${sensitiveEntries.keySet}")
        val newConfig = new JConfig(newEntries.asJava.values)

        val configResource = new ConfigResource(ConfigResource.Type.BROKER, entityNameHead)
        val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
        adminClient.alterConfigs(Map(configResource -> newConfig).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)

      case BrokerLoggerConfigType =>
        val validLoggers = getResourceConfig(adminClient, entityTypeHead, entityNameHead, includeSynonyms = true, describeAll = false).map(_.name)
        // fail the command if any of the configured broker loggers do not exist
        val invalidBrokerLoggers = configsToBeDeleted.filterNot(validLoggers.contains) ++ configsToBeAdded.keys.filterNot(validLoggers.contains)
        if (invalidBrokerLoggers.nonEmpty)
          throw new InvalidConfigurationException(s"Invalid broker logger(s): ${invalidBrokerLoggers.mkString(",")}")

        val configResource = new ConfigResource(ConfigResource.Type.BROKER_LOGGER, entityNameHead)
        val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
        val alterLogLevelEntries = (configsToBeAdded.values.map(new AlterConfigOp(_, AlterConfigOp.OpType.SET))
          ++ configsToBeDeleted.map { k => new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE) }
        ).asJavaCollection
        adminClient.incrementalAlterConfigs(Map(configResource -> alterLogLevelEntries).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)

      case ConfigType.User =>
      case ConfigType.Client =>
        val oldConfig: Map[String, java.lang.Double] = getClientQuotasConfig(adminClient, entityTypes, entityNames)

        val invalidConfigs = configsToBeDeleted.filterNot(oldConfig.contains)
        if (invalidConfigs.nonEmpty)
          throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

        val entity = new ClientQuotaEntity(opts.entityTypes.map { entType =>
          entType match {
            case ConfigType.User => ClientQuotaEntity.USER
            case ConfigType.Client => ClientQuotaEntity.CLIENT_ID
            case _ => throw new IllegalArgumentException(s"Unexpected entity type: ${entType}")
          }
        }.zip(opts.entityNames).toMap.asJava)

        val alterOptions = new AlterClientQuotasOptions().validateOnly(false)
        val alterOps = (configsToBeAddedMap.map { case (key, value) =>
          val doubleValue = try value.toDouble catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"Cannot parse quota configuration value for ${key}: ${value}")
          }
          new ClientQuotaAlteration.Op(key, doubleValue)
        } ++ configsToBeDeleted.map(key => new ClientQuotaAlteration.Op(key, null))).asJavaCollection

        adminClient.alterClientQuotas(Collections.singleton(new ClientQuotaAlteration(entity, alterOps)), alterOptions)
          .all().get(60, TimeUnit.SECONDS)

      case _ => throw new IllegalArgumentException(s"Unsupported entity type: $entityTypeHead")
    }

    if (entityNameHead.nonEmpty)
      println(s"Completed updating config for ${entityTypeHead.dropRight(1)} $entityNameHead.")
    else
      println(s"Completed updating default config for $entityTypeHead in the cluster.")
  }

  private[admin] def describeConfig(adminClient: Admin, opts: ConfigCommandOptions): Unit = {
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    val describeAll = opts.options.has(opts.allOpt)

    entityTypes.head match {
      case ConfigType.Topic | ConfigType.Broker | BrokerLoggerConfigType =>
        describeResourceConfig(adminClient, entityTypes.head, entityNames.headOption, describeAll)
      case ConfigType.User | ConfigType.Client =>
        describeClientQuotasConfig(adminClient, entityTypes, entityNames)
    }
  }

  private def describeResourceConfig(adminClient: Admin, entityType: String, entityName: Option[String], describeAll: Boolean): Unit = {
    val entities = entityName
      .map(name => List(name))
      .getOrElse(entityType match {
        case ConfigType.Topic =>
          adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get().asScala.toSeq
        case ConfigType.Broker | BrokerLoggerConfigType =>
          adminClient.describeCluster(new DescribeClusterOptions()).nodes().get().asScala.map(_.idString).toSeq :+ ConfigEntityName.Default
      })

    entities.foreach { entity =>
      entity match {
        case "" =>
          println(s"Default configs for $entityType in the cluster are:")
        case _ =>
          val configSourceStr = if (describeAll) "All" else "Dynamic"
          println(s"$configSourceStr configs for ${entityType.dropRight(1)} $entity are:")
      }
      getResourceConfig(adminClient, entityType, entity, includeSynonyms = true, describeAll).foreach { entry =>
        val synonyms = entry.synonyms.asScala.map(synonym => s"${synonym.source}:${synonym.name}=${synonym.value}").mkString(", ")
        println(s"  ${entry.name}=${entry.value} sensitive=${entry.isSensitive} synonyms={$synonyms}")
      }
    }
  }

  private def getResourceConfig(adminClient: Admin, entityType: String, entityName: String, includeSynonyms: Boolean, describeAll: Boolean) = {
    def validateBrokerId(): Unit = try entityName.toInt catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"The entity name for $entityType must be a valid integer broker id, found: $entityName")
    }

    val (configResourceType, dynamicConfigSource) = entityType match {
      case ConfigType.Topic =>
        if (!entityName.isEmpty)
          Topic.validate(entityName)
        (ConfigResource.Type.TOPIC, Some(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
      case ConfigType.Broker => entityName match {
        case "" =>
          (ConfigResource.Type.BROKER, Some(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG))
        case _ =>
          validateBrokerId()
          (ConfigResource.Type.BROKER, Some(ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG))
      }
      case BrokerLoggerConfigType =>
        if (!entityName.isEmpty)
          validateBrokerId()
        (ConfigResource.Type.BROKER_LOGGER, None)
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
      }).toSeq
  }

  private def describeClientQuotasConfig(adminClient: Admin, entityTypes: List[String], entityNames: List[String]) = {
    getAllClientQuotasConfigs(adminClient, entityTypes, entityNames).foreach { case (entity, entries) =>
      val entityEntries = entity.entries.asScala
      val entityStr = (entityEntries.get(ClientQuotaEntity.USER).map(u => s"user-principal '${u}'") ++
        entityEntries.get(ClientQuotaEntity.CLIENT_ID).map(c => s"client-id '${c}'")).mkString(", ")
      val entriesStr = entries.asScala.map(e => s"${e._1}=${e._2}").mkString(", ")
      println(s"Configs for ${entityStr} are ${entriesStr}")
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
        case Some(ConfigType.User) => ClientQuotaEntity.USER
        case Some(ConfigType.Client) => ClientQuotaEntity.CLIENT_ID
        case Some(_) => throw new IllegalArgumentException(s"Unexpected entity type ${entityTypeOpt.get}")
        case None => throw new IllegalArgumentException("More entity names specified than entity types")
      }
      entityNameOpt.map(ClientQuotaFilterComponent.ofEntity(entityType, _)).getOrElse(ClientQuotaFilterComponent.ofEntityType(entityType))
    }

    adminClient.describeClientQuotas(ClientQuotaFilter.containsOnly(components.asJava)).entities.get(30, TimeUnit.SECONDS).asScala
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

    def getAllEntities(zkClient: KafkaZkClient) : Seq[ConfigEntity] = {
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
          val rootEntities = zkClient.getAllEntitiesWithConfig(root.entityType)
                                   .map(name => ConfigEntity(Entity(root.entityType, Some(name)), child))
          child match {
            case Some(s) =>
                rootEntities.flatMap(rootEntity =>
                  ConfigEntity(rootEntity.root, Some(Entity(s.entityType, None))).getAllEntities(zkClient))
            case None => rootEntities
          }
        case (_, Some(childEntity)) =>
          childEntity.sanitizedName match {
            case Some(_) => Seq(this)
            case None =>
                zkClient.getAllEntitiesWithConfig(root.entityPath + "/" + childEntity.entityType)
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
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    if (entityTypes.head == ConfigType.User || entityTypes.head == ConfigType.Client)
      parseClientQuotaEntity(opts, entityTypes, entityNames)
    else {
      // Exactly one entity type and at-most one entity name expected for other entities
      val name = entityNames.headOption match {
        case Some("") => Some(ConfigEntityName.Default)
        case v => v
      }
      ConfigEntity(Entity(entityTypes.head, name), None)
    }
  }

  private def parseClientQuotaEntity(opts: ConfigCommandOptions, types: List[String], names: List[String]): ConfigEntity = {
    if (opts.options.has(opts.alterOpt) && names.size != types.size)
      throw new IllegalArgumentException("--entity-name or --entity-default must be specified with each --entity-type for --alter")

    val reverse = types.size == 2 && types.head == ConfigType.Client
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

  class ConfigCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {

    val zkConnectOpt = parser.accepts("zookeeper", "DEPRECATED. The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over. Replaced by --bootstrap-server, REQUIRED unless --bootstrap-server is given.")
            .withRequiredArg
            .describedAs("urls")
            .ofType(classOf[String])
    val bootstrapServerOpt = parser.accepts("bootstrap-server", "The Kafka server to connect to. " +
      "This is required for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("server to connect to")
      .ofType(classOf[String])
    val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client. " +
      "This is used only with --bootstrap-server option for describing and altering broker configs.")
      .withRequiredArg
      .describedAs("command config property file")
      .ofType(classOf[String])
    val alterOpt = parser.accepts("alter", "Alter the configuration for the entity.")
    val describeOpt = parser.accepts("describe", "List configs for the given entity.")
    val allOpt = parser.accepts("all", "List all configs for the given entity (includes static configuration when the entity type is brokers)")

    val entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityDefault = parser.accepts("entity-default", "Default entity name for clients/users/brokers (applies to corresponding entity type in command line)")

    val nl = System.getProperty("line.separator")
    val addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
            "For entity-type '" + ConfigType.Topic + "': " + LogConfig.configNames.map("\t" + _).mkString(nl, nl, nl) +
            "For entity-type '" + ConfigType.Broker + "': " + DynamicConfig.Broker.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
            "For entity-type '" + ConfigType.User + "': " + DynamicConfig.User.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
            "For entity-type '" + ConfigType.Client + "': " + DynamicConfig.Client.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
            s"Entity types '${ConfigType.User}' and '${ConfigType.Client}' may be specified together to update config for clients of a specific user.")
            .withRequiredArg
            .ofType(classOf[String])
    val deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
            .withRequiredArg
            .ofType(classOf[String])
            .withValuesSeparatedBy(',')
    val forceOpt = parser.accepts("force", "Suppress console prompts")
    val topic = parser.accepts("topic", "The topic's name.")
      .withRequiredArg
      .ofType(classOf[String])
    val client = parser.accepts("client", "The client's ID.")
      .withRequiredArg
      .ofType(classOf[String])
    val clientDefaults = parser.accepts("client-defaults", "The config defaults for all clients.")
    val user = parser.accepts("user", "The user's principal name.")
      .withRequiredArg
      .ofType(classOf[String])
    val userDefaults = parser.accepts("user-defaults", "The config defaults for all users.")
    val broker = parser.accepts("broker", "The broker's ID.")
      .withRequiredArg
      .ofType(classOf[String])
    val brokerDefaults = parser.accepts("broker-defaults", "The config defaults for all brokers.")
    val brokerLogger = parser.accepts("broker-logger", "The broker's ID for its logger config.")
      .withRequiredArg
      .ofType(classOf[String])
    val zkTlsConfigFile = parser.accepts("zk-tls-config-file",
      "Identifies the file where ZooKeeper client TLS connectivity properties are defined.  Any properties other than " +
        KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList.sorted.mkString(", ") + " are ignored.")
      .withRequiredArg().describedAs("ZooKeeper TLS configuration").ofType(classOf[String])
    options = parser.parse(args : _*)

    private val entityFlags = List((topic, ConfigType.Topic),
      (client, ConfigType.Client),
      (user, ConfigType.User),
      (broker, ConfigType.Broker),
      (brokerLogger, BrokerLoggerConfigType))

    private val entityDefaultsFlags = List((clientDefaults, ConfigType.Client),
      (userDefaults, ConfigType.User),
      (brokerDefaults, ConfigType.Broker))

    private[admin] def entityTypes(): List[String] = {
      options.valuesOf(entityType).asScala.toList ++
        (entityFlags ++ entityDefaultsFlags).filter(entity => options.has(entity._1)).map(_._2)
    }

    private[admin] def entityNames(): List[String] = {
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
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --describe, --alter")
      // check required args
      CommandLineUtils.checkInvalidArgs(parser, options, alterOpt, Set(describeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, Set(alterOpt, addConfig, deleteConfig))

      val entityTypeVals = entityTypes
      if (entityTypeVals.size != entityTypeVals.distinct.size)
        throw new IllegalArgumentException(s"Duplicate entity type(s) specified: ${entityTypeVals.diff(entityTypeVals.distinct).mkString(",")}")

      val (allowedEntityTypes, connectOptString) = if (options.has(bootstrapServerOpt))
        (BrokerSupportedConfigTypes, "--bootstrap-server")
      else
        (ZkSupportedConfigTypes, "--zookeeper")

      entityTypeVals.foreach(entityTypeVal =>
        if (!allowedEntityTypes.contains(entityTypeVal))
          throw new IllegalArgumentException(s"Invalid entity type $entityTypeVal, the entity type must be one of ${allowedEntityTypes.mkString(",")} with the $connectOptString argument")
      )
      if (entityTypeVals.isEmpty)
        throw new IllegalArgumentException("At least one entity type must be specified")
      else if (entityTypeVals.size > 1 && !entityTypeVals.toSet.equals(Set(ConfigType.User, ConfigType.Client)))
        throw new IllegalArgumentException(s"Only '${ConfigType.User}' and '${ConfigType.Client}' entity types may be specified together")

      if ((options.has(entityName) || options.has(entityType) || options.has(entityDefault)) &&
        (entityFlags ++ entityDefaultsFlags).exists(entity => options.has(entity._1)))
        throw new IllegalArgumentException("--entity-{type,name,default} should not be used in conjunction with specific entity flags")

      val hasEntityName = entityNames.exists(!_.isEmpty)
      val hasEntityDefault = entityNames.exists(_.isEmpty)

      if (!options.has(bootstrapServerOpt) && !options.has(zkConnectOpt))
        throw new IllegalArgumentException("One of the required --bootstrap-server or --zookeeper arguments must be specified")
      else if (options.has(bootstrapServerOpt) && options.has(zkConnectOpt))
        throw new IllegalArgumentException("Only one of --bootstrap-server or --zookeeper must be specified")

      if (options.has(allOpt) && options.has(zkConnectOpt)) {
        throw new IllegalArgumentException(s"--bootstrap-server must be specified for --all")
      }

      if (hasEntityName && (entityTypeVals.contains(ConfigType.Broker) || entityTypeVals.contains(BrokerLoggerConfigType))) {
        Seq(entityName, broker, brokerLogger).filter(options.has(_)).map(options.valueOf(_)).foreach { brokerId =>
          try brokerId.toInt catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"The entity name for ${entityTypeVals.head} must be a valid integer broker id, but it is: $brokerId")
          }
        }
      }

      if (options.has(describeOpt) && entityTypeVals.contains(BrokerLoggerConfigType) && !hasEntityName)
        throw new IllegalArgumentException(s"an entity name must be specified with --describe of ${entityTypeVals.mkString(",")}")

      if (options.has(alterOpt)) {
        if (entityTypeVals.contains(ConfigType.User) || entityTypeVals.contains(ConfigType.Client) || entityTypeVals.contains(ConfigType.Broker)) {
          if (!hasEntityName && !hasEntityDefault)
            throw new IllegalArgumentException("an entity-name or default entity must be specified with --alter of users, clients or brokers")
        } else if (!hasEntityName)
          throw new IllegalArgumentException(s"an entity name must be specified with --alter of ${entityTypeVals.mkString(",")}")

        val isAddConfigPresent: Boolean = options.has(addConfig)
        val isDeleteConfigPresent: Boolean = options.has(deleteConfig)
        if (!isAddConfigPresent && !isDeleteConfigPresent)
          throw new IllegalArgumentException("At least one of --add-config or --delete-config must be specified with --alter")
      }
    }
  }
}
