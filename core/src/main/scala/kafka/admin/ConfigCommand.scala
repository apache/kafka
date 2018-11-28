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
import org.apache.kafka.clients.admin.{AlterConfigsOptions, ConfigEntry, DescribeConfigsOptions, AdminClient => JAdminClient, Config => JConfig}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.scram.internals.{ScramCredentialUtils, ScramFormatter, ScramMechanism}
import org.apache.kafka.common.utils.{Sanitizer, Time, Utils}

import scala.collection.JavaConverters._
import scala.collection._


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
  // Dynamic broker configs can only be updated using the new AdminClient once brokers have started
  // so that configs may be fully validated. Prior to starting brokers, updates may be performed using
  // ZooKeeper for bootstrapping. This allows all password configs to be stored encrypted in ZK,
  // avoiding clear passwords in server.properties. For consistency with older versions, quota-related
  // broker configs can still be updated using ZooKeeper at any time. ConfigCommand will be migrated
  // to the new AdminClient later for these configs (KIP-248).
  val BrokerConfigsUpdatableUsingZooKeeperWhileBrokerRunning = Set(
    DynamicConfig.Broker.LeaderReplicationThrottledRateProp,
    DynamicConfig.Broker.FollowerReplicationThrottledRateProp,
    DynamicConfig.Broker.ReplicaAlterLogDirsIoMaxBytesPerSecondProp)

  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandOptions(args)

      CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to manipulate and describe entity config for a topic, client, user or broker")

      opts.checkArgs()

      if (opts.options.has(opts.zkConnectOpt)) {
        processCommandWithZk(opts.options.valueOf(opts.zkConnectOpt), opts)
      } else {
        processBrokerConfig(opts)
      }
    } catch {
      case e @ (_: IllegalArgumentException | _: InvalidConfigurationException | _: OptionException) =>
        logger.debug(s"Failed config command with args '${args.mkString(" ")}'", e)
        System.err.println(e.getMessage)
        Exit.exit(1)

      case t: Throwable =>
        System.err.println(s"Error while executing config command with args '${args.mkString(" ")}'")
        t.printStackTrace(System.err)
        Exit.exit(1)
    }
  }

  private def processCommandWithZk(zkConnectString: String, opts: ConfigCommandOptions): Unit = {
    val zkClient = KafkaZkClient(zkConnectString, JaasUtils.isZkSecurityEnabled, 30000, 30000,
      Int.MaxValue, Time.SYSTEM)
    val adminZkClient = new AdminZkClient(zkClient)
    try {
      if (opts.options.has(opts.alterOpt))
        alterConfig(zkClient, opts, adminZkClient)
      else if (opts.options.has(opts.describeOpt))
        describeConfig(zkClient, opts, adminZkClient)
    } finally {
      zkClient.close()
    }
  }

  private[admin] def alterConfig(zkClient: KafkaZkClient, opts: ConfigCommandOptions, adminZkClient: AdminZkClient) {
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
        val info = "Broker configuraton updates using ZooKeeper are supported for bootstrapping before brokers" +
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
  private def preProcessBrokerConfigs(configsToBeAdded: Properties, perBrokerConfig: Boolean) {
    val passwordEncoderConfigs = new Properties
    passwordEncoderConfigs ++= configsToBeAdded.asScala.filterKeys(_.startsWith("password.encoder."))
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

  private def describeConfig(zkClient: KafkaZkClient, opts: ConfigCommandOptions, adminZkClient: AdminZkClient) {
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

  private def processBrokerConfig(opts: ConfigCommandOptions): Unit = {
    val props = if (opts.options.has(opts.commandConfigOpt))
      Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
    else
      new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
    val adminClient = JAdminClient.create(props)
    val entityName = if (opts.options.has(opts.entityName))
      opts.options.valueOf(opts.entityName)
    else if (opts.options.has(opts.entityDefault))
      ""
    else
      throw new IllegalArgumentException("At least one of --entity-name or --entity-default must be specified with --bootstrap-server")

    val entityTypes = opts.options.valuesOf(opts.entityType).asScala
    if (entityTypes.size != 1)
      throw new IllegalArgumentException("Exactly one --entity-type must be specified with --bootstrap-server")
    if (entityTypes.head != ConfigType.Broker)
      throw new IllegalArgumentException(s"--zookeeper option must be specified for entity-type $entityTypes")

    try {
      if (opts.options.has(opts.alterOpt))
        alterBrokerConfig(adminClient, opts, entityName)
      else if (opts.options.has(opts.describeOpt))
        describeBrokerConfig(adminClient, opts, entityName)
    } finally {
      adminClient.close()
    }
  }

  private[admin] def alterBrokerConfig(adminClient: JAdminClient, opts: ConfigCommandOptions, entityName: String) {
    val configsToBeAdded = parseConfigsToBeAdded(opts).asScala.map { case (k, v) => (k, new ConfigEntry(k, v)) }
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)

    // compile the final set of configs
    val configResource = new ConfigResource(ConfigResource.Type.BROKER, entityName)
    val oldConfig = brokerConfig(adminClient, entityName, includeSynonyms = false)
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

    val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
    adminClient.alterConfigs(Map(configResource -> newConfig).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)

    if (entityName.nonEmpty)
      println(s"Completed updating config for broker: $entityName.")
    else
      println(s"Completed updating default config for brokers in the cluster,")
  }

  private def describeBrokerConfig(adminClient: JAdminClient, opts: ConfigCommandOptions, entityName: String) {
    val configs = brokerConfig(adminClient, entityName, includeSynonyms = true)
    if (entityName.nonEmpty)
      println(s"Configs for broker $entityName are:")
    else
      println(s"Default config for brokers in the cluster are:")
    configs.foreach { config =>
      val synonyms = config.synonyms.asScala.map(synonym => s"${synonym.source}:${synonym.name}=${synonym.value}").mkString(", ")
      println(s"  ${config.name}=${config.value} sensitive=${config.isSensitive} synonyms={$synonyms}")
    }
  }

  private def brokerConfig(adminClient: JAdminClient, entityName: String, includeSynonyms: Boolean): Seq[ConfigEntry] = {
    val configResource = new ConfigResource(ConfigResource.Type.BROKER, entityName)
    val configSource = if (!entityName.isEmpty)
      ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG
    else
      ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG
    val describeOpts = new DescribeConfigsOptions().includeSynonyms(includeSynonyms)
    val configs = adminClient.describeConfigs(Collections.singleton(configResource), describeOpts).all.get(30, TimeUnit.SECONDS)
    configs.get(configResource).entries.asScala
      .filter(entry => entry.source == configSource)
      .toSeq
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
    val entityTypes = opts.options.valuesOf(opts.entityType).asScala
    if (entityTypes.head == ConfigType.User || entityTypes.head == ConfigType.Client)
      parseQuotaEntity(opts)
    else {
      // Exactly one entity type and at-most one entity name expected for other entities
      val name = if (opts.options.has(opts.entityName))
        Some(opts.options.valueOf(opts.entityName))
      else if (entityTypes.head == ConfigType.Broker && opts.options.has(opts.entityDefault))
        Some(ConfigEntityName.Default)
      else
        None
      ConfigEntity(Entity(entityTypes.head, name), None)
    }
  }

  private def entityNames(opts: ConfigCommandOptions): Seq[String] = {
    val namesIterator = opts.options.valuesOf(opts.entityName).iterator
    opts.options.specs.asScala
      .filter(spec => spec.options.contains("entity-name") || spec.options.contains("entity-default"))
      .map(spec => if (spec.options.contains("entity-name")) namesIterator.next else "")
  }

  private def parseQuotaEntity(opts: ConfigCommandOptions): ConfigEntity = {
    val types = opts.options.valuesOf(opts.entityType).asScala
    val names = entityNames(opts)

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

  class ConfigCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {

    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
            "Multiple URLS can be given to allow fail-over.")
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
    val entityType = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityName = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id)")
            .withRequiredArg
            .ofType(classOf[String])
    val entityDefault = parser.accepts("entity-default", "Default entity name for clients/users/brokers (applies to corresponding entity type in command line)")

    val nl = System.getProperty("line.separator")
    val addConfig = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
            "For entity-type '" + ConfigType.Topic + "': " + LogConfig.configNames.map("\t" + _).mkString(nl, nl, nl) +
            "For entity-type '" + ConfigType.Broker + "': " + DynamicConfig.Broker.names.asScala.map("\t" + _).mkString(nl, nl, nl) +
            "For entity-type '" + ConfigType.User + "': " + DynamicConfig.User.names.asScala.map("\t" + _).mkString(nl, nl, nl) +
            "For entity-type '" + ConfigType.Client + "': " + DynamicConfig.Client.names.asScala.map("\t" + _).mkString(nl, nl, nl) +
            s"Entity types '${ConfigType.User}' and '${ConfigType.Client}' may be specified together to update config for clients of a specific user.")
            .withRequiredArg
            .ofType(classOf[String])
    val deleteConfig = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
            .withRequiredArg
            .ofType(classOf[String])
            .withValuesSeparatedBy(',')
    val forceOpt = parser.accepts("force", "Suppress console prompts")
    options = parser.parse(args : _*)

    val allOpts: Set[OptionSpec[_]] = Set(alterOpt, describeOpt, entityType, entityName, addConfig, deleteConfig, helpOpt)

    def checkArgs() {
      // should have exactly one action
      val actions = Seq(alterOpt, describeOpt).count(options.has _)
      if(actions != 1)
        CommandLineUtils.printUsageAndDie(parser, "Command must include exactly one action: --describe, --alter")

      // check required args
      CommandLineUtils.checkInvalidArgs(parser, options, alterOpt, Set(describeOpt))
      CommandLineUtils.checkInvalidArgs(parser, options, describeOpt, Set(alterOpt, addConfig, deleteConfig))
      val entityTypeVals = options.valuesOf(entityType).asScala

      if (options.has(bootstrapServerOpt) == options.has(zkConnectOpt))
        throw new IllegalArgumentException("Only one of --bootstrap-server or --zookeeper must be specified")
      if (entityTypeVals.contains(ConfigType.Client) || entityTypeVals.contains(ConfigType.Topic) || entityTypeVals.contains(ConfigType.User))
        CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, entityType)
      if(options.has(alterOpt)) {
        if (entityTypeVals.contains(ConfigType.User) || entityTypeVals.contains(ConfigType.Client) || entityTypeVals.contains(ConfigType.Broker)) {
          if (!options.has(entityName) && !options.has(entityDefault))
            throw new IllegalArgumentException("--entity-name or --entity-default must be specified with --alter of users, clients or brokers")
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
