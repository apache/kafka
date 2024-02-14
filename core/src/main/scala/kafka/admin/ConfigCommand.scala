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

import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}
import joptsimple._
import kafka.server.{DynamicBrokerConfig, DynamicConfig, KafkaConfig}
import kafka.server.DynamicConfig.QuotaConfigs
import kafka.utils.Implicits._
import kafka.utils.{Exit, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{Admin, AlterClientQuotasOptions, AlterConfigOp, AlterConfigsOptions, ConfigEntry, DescribeClusterOptions, DescribeConfigsOptions, ListTopicsOptions, ScramCredentialInfo, UserScramCredentialDeletion, UserScramCredentialUpsertion, Config => JConfig, ScramMechanism => PublicScramMechanism}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidConfigurationException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter, ClientQuotaFilterComponent}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.scram.internals.{ScramCredentialUtils, ScramFormatter, ScramMechanism}
import org.apache.kafka.common.utils.{Sanitizer, Time, Utils}
import org.apache.kafka.server.config.{ConfigEntityName, ConfigType}
import org.apache.kafka.security.{PasswordEncoder, PasswordEncoderConfigs}
import org.apache.kafka.server.util.{CommandDefaultOptions, CommandLineUtils}
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.zookeeper.client.ZKClientConfig

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection._

/**
 * This script can be used to change configs for topics/clients/users/brokers/ips/client-metrics dynamically
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
 * </ul>
 * --entity-type <users|clients|brokers|ips> --entity-default may be specified in place of --entity-type <users|clients|brokers|ips> --entity-name <entityName>
 * when describing or altering default configuration for users, clients, brokers, or ips, respectively.
 * Alternatively, --user-defaults, --client-defaults, --broker-defaults, or --ip-defaults may be specified in place of
 * --entity-type <users|clients|brokers|ips> --entity-default, respectively.
 *
 * For most use cases, this script communicates with a kafka cluster (specified via the
 * `--bootstrap-server` option). There are three exceptions where direct communication with a
 * ZooKeeper ensemble (specified via the `--zookeeper` option) is allowed:
 *
 * 1. Describe/alter user configs where the config is a SCRAM mechanism name (i.e. a SCRAM credential for a user)
 * 2. Describe/alter broker configs for a particular broker when that broker is down
 * 3. Describe/alter broker default configs when all brokers are down
 *
 * For example, this allows password configs to be stored encrypted in ZK before brokers are started,
 * avoiding cleartext passwords in `server.properties`.
 */
object ConfigCommand extends Logging {

  val BrokerDefaultEntityName = ""
  val BrokerLoggerConfigType = "broker-loggers"
  private val BrokerSupportedConfigTypes = ConfigType.ALL.asScala :+ BrokerLoggerConfigType :+ ConfigType.CLIENT_METRICS
  private val ZkSupportedConfigTypes = Seq(ConfigType.USER, ConfigType.BROKER)
  private val DefaultScramIterations = 4096

  def main(args: Array[String]): Unit = {
    try {
      val opts = new ConfigCommandOptions(args)

      CommandLineUtils.maybePrintHelpOrVersion(opts, "This tool helps to manipulate and describe entity config for a topic, client, user, broker, ip or client-metrics")

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
      Int.MaxValue, Time.SYSTEM, zkClientConfig = zkClientConfig, name = "ConfigCommand")
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
    val errorMessage = s"--bootstrap-server option must be specified to update $entityType configs: {add: $configsToBeAdded, delete: $configsToBeDeleted}"
    var isUserClientId = false

    if (entityType == ConfigType.USER) {
      isUserClientId = entity.child.exists(e => ConfigType.CLIENT.equals(e.entityType))
      if (!configsToBeAdded.isEmpty || configsToBeDeleted.nonEmpty) {
        val info = "User configuration updates using ZooKeeper are only supported for SCRAM credential updates."
        val scramMechanismNames = ScramMechanism.values.map(_.mechanismName)
        // make sure every added/deleted configs are SCRAM related, other configs are not supported using zookeeper
        require(configsToBeAdded.stringPropertyNames.asScala.forall(scramMechanismNames.contains),
          s"$errorMessage. $info")
        require(configsToBeDeleted.forall(scramMechanismNames.contains), s"$errorMessage. $info")
      }
      preProcessScramCredentials(configsToBeAdded)
    } else if (entityType == ConfigType.BROKER) {
      // Dynamic broker configs can be updated using ZooKeeper only if the corresponding broker is not running.
      if (!configsToBeAdded.isEmpty || configsToBeDeleted.nonEmpty) {
        validateBrokersNotRunning(entityName, adminZkClient, zkClient, errorMessage)

        val perBrokerConfig = entityName != ConfigEntityName.DEFAULT
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

    adminZkClient.changeConfigs(entityType, entityName, configs, isUserClientId)

    println(s"Completed updating config for entity: $entity.")
  }

  private def validateBrokersNotRunning(entityName: String,
                                        adminZkClient: AdminZkClient,
                                        zkClient: KafkaZkClient,
                                        errorMessage: String): Unit = {
    val perBrokerConfig = entityName != ConfigEntityName.DEFAULT
    val info = "Broker configuration operations using ZooKeeper are only supported if the affected broker(s) are not running."
    if (perBrokerConfig) {
      adminZkClient.parseBroker(entityName).foreach { brokerId =>
        require(zkClient.getBroker(brokerId).isEmpty, s"$errorMessage - broker $brokerId is running. $info")
      }
    } else {
      val runningBrokersCount = zkClient.getAllBrokersInCluster.size
      require(runningBrokersCount == 0, s"$errorMessage - $runningBrokersCount brokers are running. $info")
    }
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
    encoderConfigs.get(PasswordEncoderConfigs.SECRET)
    val encoderSecret = encoderConfigs.getOrElse(PasswordEncoderConfigs.SECRET,
      throw new IllegalArgumentException("Password encoder secret not specified"))
    PasswordEncoder.encrypting(new Password(encoderSecret),
      null,
      encoderConfigs.getOrElse(PasswordEncoderConfigs.CIPHER_ALGORITHM, PasswordEncoderConfigs.DEFAULT_CIPHER_ALGORITHM),
      encoderConfigs.get(PasswordEncoderConfigs.KEY_LENGTH).map(_.toInt).getOrElse(PasswordEncoderConfigs.DEFAULT_KEY_LENGTH),
      encoderConfigs.get(PasswordEncoderConfigs.ITERATIONS).map(_.toInt).getOrElse(PasswordEncoderConfigs.DEFAULT_ITERATIONS))
  }

  /**
   * Pre-process broker configs provided to convert them to persistent format.
   * Password configs are encrypted using the secret `PasswordEncoderConfigs.SECRET`.
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
      require(passwordEncoderConfigs.containsKey(PasswordEncoderConfigs.SECRET),
        s"${PasswordEncoderConfigs.SECRET} must be specified to update $passwordConfigs." +
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

  private[admin] def describeConfigWithZk(zkClient: KafkaZkClient, opts: ConfigCommandOptions, adminZkClient: AdminZkClient): Unit = {
    val configEntity = parseEntity(opts)
    val entityType = configEntity.root.entityType
    val describeAllUsers = entityType == ConfigType.USER && configEntity.root.sanitizedName.isEmpty && configEntity.child.isEmpty
    val entityName = configEntity.fullSanitizedName
    val errorMessage = s"--bootstrap-server option must be specified to describe $entityType"
    if (entityType == ConfigType.BROKER) {
      // Dynamic broker configs can be described using ZooKeeper only if the corresponding broker is not running.
      validateBrokersNotRunning(entityName, adminZkClient, zkClient, errorMessage)
    }

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

  @nowarn("cat=deprecation")
  private[admin] def parseConfigsToBeAdded(opts: ConfigCommandOptions): Properties = {
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
      require(configsToBeAdded.forall(config => config.length == 2), "Invalid entity config: all configs to be added must be in the format \"key=val\".")
      //Create properties, parsing square brackets from values if necessary
      configsToBeAdded.foreach(pair => props.setProperty(pair(0).trim, pair(1).replaceAll("\\[?\\]?", "").trim))
    }
    if (props.containsKey(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)) {
      println(s"WARNING: The configuration ${TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG}=${props.getProperty(TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG)} is specified. " +
        "This configuration will be ignored if the version is newer than the inter.broker.protocol.version specified in the broker or " +
        "if the inter.broker.protocol.version is 3.0 or newer. This configuration is deprecated and it will be removed in Apache Kafka 4.0.")
    }
    validatePropsKey(props)
    props
  }

  private[admin] def parseConfigsToBeDeleted(opts: ConfigCommandOptions): Seq[String] = {
    if (opts.options.has(opts.deleteConfig)) {
      val configsToBeDeleted = opts.options.valuesOf(opts.deleteConfig).asScala.map(_.trim())
      configsToBeDeleted
    }
    else
      Seq.empty
  }

  private def validatePropsKey(props: Properties): Unit = {
    props.keySet.forEach { propsKey =>
      if (!propsKey.toString.matches("[a-zA-Z0-9._-]*")) {
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

  @nowarn("cat=deprecation")
  private[admin] def alterConfig(adminClient: Admin, opts: ConfigCommandOptions): Unit = {
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    val entityTypeHead = entityTypes.head
    val entityNameHead = entityNames.head
    val configsToBeAddedMap = parseConfigsToBeAdded(opts).asScala.toMap // no need for mutability
    val configsToBeAdded = configsToBeAddedMap.map { case (k, v) => (k, new ConfigEntry(k, v)) }
    val configsToBeDeleted = parseConfigsToBeDeleted(opts)

    entityTypeHead match {
      case ConfigType.TOPIC =>
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

      case ConfigType.BROKER =>
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

      case ConfigType.USER | ConfigType.CLIENT =>
        val hasQuotaConfigsToAdd = configsToBeAdded.keys.exists(QuotaConfigs.isClientOrUserQuotaConfig)
        val scramConfigsToAddMap = configsToBeAdded.filter(entry => ScramMechanism.isScram(entry._1))
        val unknownConfigsToAdd = configsToBeAdded.keys.filterNot(key => ScramMechanism.isScram(key) || QuotaConfigs.isClientOrUserQuotaConfig(key))
        val hasQuotaConfigsToDelete = configsToBeDeleted.exists(QuotaConfigs.isClientOrUserQuotaConfig)
        val scramConfigsToDelete = configsToBeDeleted.filter(ScramMechanism.isScram)
        val unknownConfigsToDelete = configsToBeDeleted.filterNot(key => ScramMechanism.isScram(key) || QuotaConfigs.isClientOrUserQuotaConfig(key))
        if (entityTypeHead == ConfigType.CLIENT || entityTypes.size == 2) { // size==2 for case where users is specified first on the command line, before clients
          // either just a client or both a user and a client
          if (unknownConfigsToAdd.nonEmpty || scramConfigsToAddMap.nonEmpty)
            throw new IllegalArgumentException(s"Only quota configs can be added for '${ConfigType.CLIENT}' using --bootstrap-server. Unexpected config names: ${unknownConfigsToAdd ++ scramConfigsToAddMap.keys}")
          if (unknownConfigsToDelete.nonEmpty || scramConfigsToDelete.nonEmpty)
            throw new IllegalArgumentException(s"Only quota configs can be deleted for '${ConfigType.CLIENT}' using --bootstrap-server. Unexpected config names: ${unknownConfigsToDelete ++ scramConfigsToDelete}")
        } else { // ConfigType.User
          if (unknownConfigsToAdd.nonEmpty)
            throw new IllegalArgumentException(s"Only quota and SCRAM credential configs can be added for '${ConfigType.USER}' using --bootstrap-server. Unexpected config names: $unknownConfigsToAdd")
          if (unknownConfigsToDelete.nonEmpty)
            throw new IllegalArgumentException(s"Only quota and SCRAM credential configs can be deleted for '${ConfigType.USER}' using --bootstrap-server. Unexpected config names: $unknownConfigsToDelete")
          if (scramConfigsToAddMap.nonEmpty || scramConfigsToDelete.nonEmpty) {
            if (entityNames.exists(_.isEmpty)) // either --entity-type users --entity-default or --user-defaults
              throw new IllegalArgumentException("The use of --entity-default or --user-defaults is not allowed with User SCRAM Credentials using --bootstrap-server.")
            if (hasQuotaConfigsToAdd || hasQuotaConfigsToDelete)
              throw new IllegalArgumentException(s"Cannot alter both quota and SCRAM credential configs simultaneously for '${ConfigType.USER}' using --bootstrap-server.")
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
      case ConfigType.IP =>
        val unknownConfigs = (configsToBeAdded.keys ++ configsToBeDeleted).filterNot(key => DynamicConfig.Ip.names.contains(key))
        if (unknownConfigs.nonEmpty)
          throw new IllegalArgumentException(s"Only connection quota configs can be added for '${ConfigType.IP}' using --bootstrap-server. Unexpected config names: ${unknownConfigs.mkString(",")}")
        alterQuotaConfigs(adminClient, entityTypes, entityNames, configsToBeAddedMap, configsToBeDeleted)
      case ConfigType.CLIENT_METRICS =>
        val oldConfig = getResourceConfig(adminClient, entityTypeHead, entityNameHead, includeSynonyms = false, describeAll = false)
          .map { entry => (entry.name, entry) }.toMap

        // fail the command if any of the configs to be deleted does not exist
        val invalidConfigs = configsToBeDeleted.filterNot(oldConfig.contains)
        if (invalidConfigs.nonEmpty)
          throw new InvalidConfigurationException(s"Invalid config(s): ${invalidConfigs.mkString(",")}")

        val configResource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, entityNameHead)
        val alterOptions = new AlterConfigsOptions().timeoutMs(30000).validateOnly(false)
        val alterEntries = (configsToBeAdded.values.map(new AlterConfigOp(_, AlterConfigOp.OpType.SET))
          ++ configsToBeDeleted.map { k => new AlterConfigOp(new ConfigEntry(k, ""), AlterConfigOp.OpType.DELETE) }
          ).asJavaCollection
        adminClient.incrementalAlterConfigs(Map(configResource -> alterEntries).asJava, alterOptions).all().get(60, TimeUnit.SECONDS)
      case _ => throw new IllegalArgumentException(s"Unsupported entity type: $entityTypeHead")
    }

    if (entityNameHead.nonEmpty)
      println(s"Completed updating config for ${entityTypeHead.dropRight(1)} $entityNameHead.")
    else
      println(s"Completed updating default config for $entityTypeHead in the cluster.")
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
      case ConfigType.USER => ClientQuotaEntity.USER
      case ConfigType.CLIENT => ClientQuotaEntity.CLIENT_ID
      case ConfigType.IP => ClientQuotaEntity.IP
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

  private[admin] def describeConfig(adminClient: Admin, opts: ConfigCommandOptions): Unit = {
    val entityTypes = opts.entityTypes
    val entityNames = opts.entityNames
    val describeAll = opts.options.has(opts.allOpt)

    entityTypes.head match {
      case ConfigType.TOPIC | ConfigType.BROKER | BrokerLoggerConfigType | ConfigType.CLIENT_METRICS =>
        describeResourceConfig(adminClient, entityTypes.head, entityNames.headOption, describeAll)
      case ConfigType.USER | ConfigType.CLIENT =>
        describeClientQuotaAndUserScramCredentialConfigs(adminClient, entityTypes, entityNames)
      case ConfigType.IP =>
        describeQuotaConfigs(adminClient, entityTypes, entityNames)
      case entityType => throw new IllegalArgumentException(s"Invalid entity type: $entityType")
    }
  }

  private def describeResourceConfig(adminClient: Admin, entityType: String, entityName: Option[String], describeAll: Boolean): Unit = {
    val entities = entityName
      .map(name => List(name))
      .getOrElse(entityType match {
        case ConfigType.TOPIC =>
          adminClient.listTopics(new ListTopicsOptions().listInternal(true)).names().get().asScala.toSeq
        case ConfigType.BROKER | BrokerLoggerConfigType =>
          adminClient.describeCluster(new DescribeClusterOptions()).nodes().get().asScala.map(_.idString).toSeq :+ BrokerDefaultEntityName
        case ConfigType.CLIENT_METRICS =>
          adminClient.listClientMetricsResources().all().get().asScala.map(_.name).toSeq
        case entityType => throw new IllegalArgumentException(s"Invalid entity type: $entityType")
      })

    entities.foreach { entity =>
      entity match {
        case BrokerDefaultEntityName =>
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
      case ConfigType.TOPIC =>
        if (entityName.nonEmpty)
          Topic.validate(entityName)
        (ConfigResource.Type.TOPIC, Some(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG))
      case ConfigType.BROKER => entityName match {
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
      case ConfigType.CLIENT_METRICS =>
        (ConfigResource.Type.CLIENT_METRICS, Some(ConfigEntry.ConfigSource.DYNAMIC_CLIENT_METRICS_CONFIG))
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
    quotaConfigs.forKeyValue { (entity, entries) =>
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
      println(s"Quota configs for $entityStr are $entriesStr")
    }
  }

  private def describeClientQuotaAndUserScramCredentialConfigs(adminClient: Admin, entityTypes: List[String], entityNames: List[String]): Unit = {
    describeQuotaConfigs(adminClient, entityTypes, entityNames)
    // we describe user SCRAM credentials only when we are not describing client information
    // and we are not given either --entity-default or --user-defaults
    if (!entityTypes.contains(ConfigType.CLIENT) && !entityNames.contains("")) {
      val result = adminClient.describeUserScramCredentials(entityNames.asJava)
      result.users.get(30, TimeUnit.SECONDS).asScala.foreach(user => {
        try {
          val description = result.description(user).get(30, TimeUnit.SECONDS)
          val descriptionText = description.credentialInfos.asScala.map(info => s"${info.mechanism.mechanismName}=iterations=${info.iterations}").mkString(", ")
          println(s"SCRAM credential configs for user-principal '$user' are $descriptionText")
        } catch {
          case e: Exception => println(s"Error retrieving SCRAM credential configs for user-principal '$user': ${e.getClass.getSimpleName}: ${e.getMessage}")
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
        case Some(ConfigType.USER) => ClientQuotaEntity.USER
        case Some(ConfigType.CLIENT) => ClientQuotaEntity.CLIENT_ID
        case Some(ConfigType.IP) => ClientQuotaEntity.IP
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

  case class Entity(entityType: String, sanitizedName: Option[String]) {
    val entityPath: String = sanitizedName match {
      case Some(n) => entityType + "/" + n
      case None => entityType
    }
    override def toString: String = {
      val typeName = entityType match {
        case ConfigType.USER => "user-principal"
        case ConfigType.CLIENT => "client-id"
        case ConfigType.TOPIC => "topic"
        case t => t
      }
      sanitizedName match {
        case Some(ConfigEntityName.DEFAULT) => "default " + typeName
        case Some(n) =>
          val desanitized = if (entityType == ConfigType.USER || entityType == ConfigType.CLIENT) Sanitizer.desanitize(n) else n
          s"$typeName '$desanitized'"
        case None => entityType
      }
    }
  }

  case class ConfigEntity(root: Entity, child: Option[Entity]) {
    val fullSanitizedName: String = root.sanitizedName.getOrElse("") + child.map(s => "/" + s.entityPath).getOrElse("")

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
    if (entityTypes.head == ConfigType.USER || entityTypes.head == ConfigType.CLIENT)
      parseClientQuotaEntity(opts, entityTypes, entityNames)
    else {
      // Exactly one entity type and at-most one entity name expected for other entities
      val name = entityNames.headOption match {
        case Some("") => Some(ConfigEntityName.DEFAULT)
        case v => v
      }
      ConfigEntity(Entity(entityTypes.head, name), None)
    }
  }

  private def parseClientQuotaEntity(opts: ConfigCommandOptions, types: List[String], names: List[String]): ConfigEntity = {
    if (opts.options.has(opts.alterOpt) && names.size != types.size)
      throw new IllegalArgumentException("--entity-name or --entity-default must be specified with each --entity-type for --alter")

    val reverse = types.size == 2 && types.head == ConfigType.CLIENT
    val entityTypes = if (reverse) types.reverse else types
    val sortedNames = (if (reverse && names.length == 2) names.reverse else names).iterator

    def sanitizeName(entityType: String, name: String) = {
      if (name.isEmpty)
        ConfigEntityName.DEFAULT
      else {
        entityType match {
          case ConfigType.USER | ConfigType.CLIENT => Sanitizer.sanitize(name)
          case _ => throw new IllegalArgumentException("Invalid entity type " + entityType)
        }
      }
    }

    val entities = entityTypes.map(t => Entity(t, if (sortedNames.hasNext) Some(sanitizeName(t, sortedNames.next())) else None))
    ConfigEntity(entities.head, if (entities.size > 1) Some(entities(1)) else None)
  }

  class ConfigCommandOptions(args: Array[String]) extends CommandDefaultOptions(args) {

    val zkConnectOpt: OptionSpec[String] = parser.accepts("zookeeper", "DEPRECATED. The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over. Required when configuring SCRAM credentials for users or " +
      "dynamic broker configs when the relevant broker(s) are down. Not allowed otherwise.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])
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
    val allOpt: OptionSpecBuilder = parser.accepts("all", "List all configs for the given topic, broker, or broker-logger entity (includes static configuration when the entity type is brokers)")

    val entityType: OptionSpec[String] = parser.accepts("entity-type", "Type of entity (topics/clients/users/brokers/broker-loggers/ips/client-metrics)")
      .withRequiredArg
      .ofType(classOf[String])
    val entityName: OptionSpec[String] = parser.accepts("entity-name", "Name of entity (topic name/client id/user principal name/broker id/ip/client metrics)")
      .withRequiredArg
      .ofType(classOf[String])
    private val entityDefault: OptionSpecBuilder = parser.accepts("entity-default", "Default entity name for clients/users/brokers/ips (applies to corresponding entity type in command line)")

    val nl: String = System.getProperty("line.separator")
    val addConfig: OptionSpec[String] = parser.accepts("add-config", "Key Value pairs of configs to add. Square brackets can be used to group values which contain commas: 'k1=v1,k2=[v1,v2,v2],k3=v3'. The following is a list of valid configurations: " +
      "For entity-type '" + ConfigType.TOPIC + "': " + LogConfig.configNames.asScala.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ConfigType.BROKER + "': " + DynamicConfig.Broker.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ConfigType.USER + "': " + DynamicConfig.User.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ConfigType.CLIENT + "': " + DynamicConfig.Client.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ConfigType.IP + "': " + DynamicConfig.Ip.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      "For entity-type '" + ConfigType.CLIENT_METRICS + "': " + DynamicConfig.ClientMetrics.names.asScala.toSeq.sorted.map("\t" + _).mkString(nl, nl, nl) +
      s"Entity types '${ConfigType.USER}' and '${ConfigType.CLIENT}' may be specified together to update config for clients of a specific user.")
      .withRequiredArg
      .ofType(classOf[String])
    val addConfigFile: OptionSpec[String] = parser.accepts("add-config-file", "Path to a properties file with configs to add. See add-config for a list of valid configurations.")
      .withRequiredArg
      .ofType(classOf[String])
    val deleteConfig: OptionSpec[String] = parser.accepts("delete-config", "config keys to remove 'k1,k2'")
      .withRequiredArg
      .ofType(classOf[String])
      .withValuesSeparatedBy(',')
    val forceOpt: OptionSpecBuilder = parser.accepts("force", "Suppress console prompts")
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
    val zkTlsConfigFile: OptionSpec[String] = parser.accepts("zk-tls-config-file",
      "Identifies the file where ZooKeeper client TLS connectivity properties are defined.  Any properties other than " +
        KafkaConfig.ZkSslConfigToSystemPropertyMap.keys.toList.sorted.mkString(", ") + " are ignored.")
      .withRequiredArg().describedAs("ZooKeeper TLS configuration").ofType(classOf[String])
    options = parser.parse(args : _*)

    private val entityFlags = List((topic, ConfigType.TOPIC),
      (client, ConfigType.CLIENT),
      (user, ConfigType.USER),
      (broker, ConfigType.BROKER),
      (brokerLogger, BrokerLoggerConfigType),
      (ip, ConfigType.IP))

    private val entityDefaultsFlags = List((clientDefaults, ConfigType.CLIENT),
      (userDefaults, ConfigType.USER),
      (brokerDefaults, ConfigType.BROKER),
      (ipDefaults, ConfigType.IP))

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

      val (allowedEntityTypes, connectOptString) = if (options.has(bootstrapServerOpt) || options.has(bootstrapControllerOpt))
        (BrokerSupportedConfigTypes, "--bootstrap-server or --bootstrap-controller")
      else
        (ZkSupportedConfigTypes, "--zookeeper")

      entityTypeVals.foreach(entityTypeVal =>
        if (!allowedEntityTypes.contains(entityTypeVal))
          throw new IllegalArgumentException(s"Invalid entity type $entityTypeVal, the entity type must be one of ${allowedEntityTypes.mkString(", ")} with a $connectOptString argument")
      )
      if (entityTypeVals.isEmpty)
        throw new IllegalArgumentException("At least one entity type must be specified")
      else if (entityTypeVals.size > 1 && !entityTypeVals.toSet.equals(Set(ConfigType.USER, ConfigType.CLIENT)))
        throw new IllegalArgumentException(s"Only '${ConfigType.USER}' and '${ConfigType.CLIENT}' entity types may be specified together")

      if ((options.has(entityName) || options.has(entityType) || options.has(entityDefault)) &&
        (entityFlags ++ entityDefaultsFlags).exists(entity => options.has(entity._1)))
        throw new IllegalArgumentException("--entity-{type,name,default} should not be used in conjunction with specific entity flags")

      val hasEntityName = entityNames.exists(_.nonEmpty)
      val hasEntityDefault = entityNames.exists(_.isEmpty)

      val numConnectOptions = (if (options.has(bootstrapServerOpt)) 1 else 0) +
        (if (options.has(bootstrapControllerOpt)) 1 else 0) +
        (if (options.has(zkConnectOpt)) 1 else 0)
      if (numConnectOptions == 0)
        throw new IllegalArgumentException("One of the required --bootstrap-server, --boostrap-controller, or --zookeeper arguments must be specified")
      else if (numConnectOptions > 1)
        throw new IllegalArgumentException("Only one of --bootstrap-server, --boostrap-controller, and --zookeeper can be specified")

      if (options.has(allOpt) && options.has(zkConnectOpt)) {
        throw new IllegalArgumentException(s"--bootstrap-server must be specified for --all")
      }
      if (options.has(zkTlsConfigFile) && !options.has(zkConnectOpt)) {
        throw new IllegalArgumentException("Only the --zookeeper option can be used with the --zk-tls-config-file option.")
      }
      if (hasEntityName && (entityTypeVals.contains(ConfigType.BROKER) || entityTypeVals.contains(BrokerLoggerConfigType))) {
        Seq(entityName, broker, brokerLogger).filter(options.has(_)).map(options.valueOf(_)).foreach { brokerId =>
          try brokerId.toInt catch {
            case _: NumberFormatException =>
              throw new IllegalArgumentException(s"The entity name for ${entityTypeVals.head} must be a valid integer broker id, but it is: $brokerId")
          }
        }
      }

      if (hasEntityName && entityTypeVals.contains(ConfigType.IP)) {
        Seq(entityName, ip).filter(options.has(_)).map(options.valueOf(_)).foreach { ipEntity =>
          if (!DynamicConfig.Ip.isValidIpEntity(ipEntity))
            throw new IllegalArgumentException(s"The entity name for ${entityTypeVals.head} must be a valid IP or resolvable host, but it is: $ipEntity")
        }
      }

      if (options.has(describeOpt) && entityTypeVals.contains(BrokerLoggerConfigType) && !hasEntityName)
        throw new IllegalArgumentException(s"an entity name must be specified with --describe of ${entityTypeVals.mkString(",")}")

      if (options.has(alterOpt)) {
        if (entityTypeVals.contains(ConfigType.USER) ||
            entityTypeVals.contains(ConfigType.CLIENT) ||
            entityTypeVals.contains(ConfigType.BROKER) ||
            entityTypeVals.contains(ConfigType.IP)) {
          if (!hasEntityName && !hasEntityDefault)
            throw new IllegalArgumentException("an entity-name or default entity must be specified with --alter of users, clients, brokers or ips")
        } else if (!hasEntityName)
          throw new IllegalArgumentException(s"an entity name must be specified with --alter of ${entityTypeVals.mkString(",")}")

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
}
