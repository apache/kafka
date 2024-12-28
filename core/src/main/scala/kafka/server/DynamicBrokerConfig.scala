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

package kafka.server

import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.cluster.EndPoint
import kafka.log.{LogCleaner, LogManager}
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.server.DynamicBrokerConfig._
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, SaslConfigs, SslConfigs}
import org.apache.kafka.common.metrics.{Metrics, MetricsReporter}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.{ListenerName, ListenerReconfigurable}
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.utils.{ConfigUtils, Utils}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.PasswordEncoder
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.config.{ConfigType, ReplicationConfigs, ServerConfigs, ServerLogConfigs, ServerTopicConfigSynonyms, ZooKeeperInternals}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.{ClientMetricsReceiverPlugin, MetricConfigs}
import org.apache.kafka.server.telemetry.ClientTelemetry
import org.apache.kafka.storage.internals.log.{LogConfig, ProducerStateManagerConfig}

import scala.collection._
import scala.jdk.CollectionConverters._

/**
  * Dynamic broker configurations are stored in ZooKeeper and may be defined at two levels:
  * <ul>
  *   <li>Per-broker configs persisted at <tt>/configs/brokers/{brokerId}</tt>: These can be described/altered
  *       using AdminClient using the resource name brokerId.</li>
  *   <li>Cluster-wide defaults persisted at <tt>/configs/brokers/&lt;default&gt;</tt>: These can be described/altered
  *       using AdminClient using an empty resource name.</li>
  * </ul>
  * The order of precedence for broker configs is:
  * <ol>
  *   <li>DYNAMIC_BROKER_CONFIG: stored in ZK at /configs/brokers/{brokerId}</li>
  *   <li>DYNAMIC_DEFAULT_BROKER_CONFIG: stored in ZK at /configs/brokers/&lt;default&gt;</li>
  *   <li>STATIC_BROKER_CONFIG: properties that broker is started up with, typically from server.properties file</li>
  *   <li>DEFAULT_CONFIG: Default configs defined in KafkaConfig</li>
  * </ol>
  * Log configs use topic config overrides if defined and fallback to broker defaults using the order of precedence above.
  * Topic config overrides may use a different config name from the default broker config.
  * See [[org.apache.kafka.storage.internals.log.LogConfig#TopicConfigSynonyms]] for the mapping.
  * <p>
  * AdminClient returns all config synonyms in the order of precedence when configs are described with
  * <code>includeSynonyms</code>. In addition to configs that may be defined with the same name at different levels,
  * some configs have additional synonyms.
  * </p>
  * <ul>
  *   <li>Listener configs may be defined using the prefix <tt>listener.name.{listenerName}.{configName}</tt>. These may be
  *       configured as dynamic or static broker configs. Listener configs have higher precedence than the base configs
  *       that don't specify the listener name. Listeners without a listener config use the base config. Base configs
  *       may be defined only as STATIC_BROKER_CONFIG or DEFAULT_CONFIG and cannot be updated dynamically.<li>
  *   <li>Some configs may be defined using multiple properties. For example, <tt>log.roll.ms</tt> and
  *       <tt>log.roll.hours</tt> refer to the same config that may be defined in milliseconds or hours. The order of
  *       precedence of these synonyms is described in the docs of these configs in [[kafka.server.KafkaConfig]].</li>
  * </ul>
  *
  */
object DynamicBrokerConfig {

  private[server] val DynamicSecurityConfigs = SslConfigs.RECONFIGURABLE_CONFIGS.asScala
  private[server] val DynamicProducerStateManagerConfig = Set(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG, TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG)

  val AllDynamicConfigs = DynamicSecurityConfigs ++
    LogCleaner.ReconfigurableConfigs ++
    DynamicLogConfig.ReconfigurableConfigs ++
    DynamicThreadPool.ReconfigurableConfigs ++
    Set(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG) ++
    DynamicListenerConfig.ReconfigurableConfigs ++
    SocketServer.ReconfigurableConfigs ++
    DynamicProducerStateManagerConfig ++
    DynamicRemoteLogConfig.ReconfigurableConfigs

  private val ClusterLevelListenerConfigs = Set(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG, SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG)
  private val PerBrokerConfigs = (DynamicSecurityConfigs ++ DynamicListenerConfig.ReconfigurableConfigs).diff(
    ClusterLevelListenerConfigs)
  private val ListenerMechanismConfigs = Set(SaslConfigs.SASL_JAAS_CONFIG,
    SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
    SaslConfigs.SASL_LOGIN_CLASS,
    BrokerSecurityConfigs.SASL_SERVER_CALLBACK_HANDLER_CLASS_CONFIG,
    BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG)

  private val ReloadableFileConfigs = Set(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)

  private val ListenerConfigRegex = """listener\.name\.[^.]*\.(.*)""".r

  private val DynamicPasswordConfigs = {
    val passwordConfigs = KafkaConfig.configKeys.filter(_._2.`type` == ConfigDef.Type.PASSWORD).keySet
    AllDynamicConfigs.intersect(passwordConfigs)
  }

  def isPasswordConfig(name: String): Boolean = DynamicBrokerConfig.DynamicPasswordConfigs.exists(name.endsWith)

  def brokerConfigSynonyms(name: String, matchListenerOverride: Boolean): List[String] = {
    name match {
      case ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG | ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG =>
        List(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG)
      case ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG | ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG =>
        List(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_JITTER_HOURS_CONFIG)
      case ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG => // KafkaLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG is used as default
        List(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, ServerLogConfigs.LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG)
      case ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG | ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG | ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG =>
        List(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG)
      case ListenerConfigRegex(baseName) if matchListenerOverride =>
        // `ListenerMechanismConfigs` are specified as listenerPrefix.mechanism.<configName>
        // and other listener configs are specified as listenerPrefix.<configName>
        // Add <configName> as a synonym in both cases.
        val mechanismConfig = ListenerMechanismConfigs.find(baseName.endsWith)
        List(name, mechanismConfig.getOrElse(baseName))
      case _ => List(name)
    }
  }

  def validateConfigs(props: Properties, perBrokerConfig: Boolean): Unit = {
    def checkInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
      if (invalidPropNames.nonEmpty)
        throw new ConfigException(s"$errorMessage: $invalidPropNames")
    }
    checkInvalidProps(nonDynamicConfigs(props), "Cannot update these configs dynamically")
    checkInvalidProps(securityConfigsWithoutListenerPrefix(props),
      "These security configs can be dynamically updated only per-listener using the listener prefix")
    validateConfigTypes(props)
    if (!perBrokerConfig) {
      checkInvalidProps(perBrokerConfigs(props),
        "Cannot update these configs at default cluster level, broker id must be specified")
    }
  }

  private def perBrokerConfigs(props: Properties): Set[String] = {
    val configNames = props.asScala.keySet
    def perBrokerListenerConfig(name: String): Boolean = {
      name match {
        case ListenerConfigRegex(baseName) => !ClusterLevelListenerConfigs.contains(baseName)
        case _ => false
      }
    }
    configNames.intersect(PerBrokerConfigs) ++ configNames.filter(perBrokerListenerConfig)
  }

  private def nonDynamicConfigs(props: Properties): Set[String] = {
    props.asScala.keySet.intersect(DynamicConfig.Broker.nonDynamicProps)
  }

  private def securityConfigsWithoutListenerPrefix(props: Properties): Set[String] = {
    DynamicSecurityConfigs.filter(props.containsKey)
  }

  private def validateConfigTypes(props: Properties): Unit = {
    val baseProps = new Properties
    props.asScala.foreach {
      case (ListenerConfigRegex(baseName), v) => baseProps.put(baseName, v)
      case (k, v) => baseProps.put(k, v)
    }
    DynamicConfig.Broker.validate(baseProps)
  }

  private[server] def dynamicConfigUpdateModes: util.Map[String, String] = {
    AllDynamicConfigs.map { name =>
      val mode = if (PerBrokerConfigs.contains(name)) "per-broker" else "cluster-wide"
      name -> mode
    }.toMap.asJava
  }

  private[server] def resolveVariableConfigs(propsOriginal: Properties): Properties = {
    val props = new Properties
    val config = new AbstractConfig(new ConfigDef(), propsOriginal, Utils.castToStringObjectMap(propsOriginal), false)
    config.originals.forEach { (key, value) =>
      if (!key.startsWith(AbstractConfig.CONFIG_PROVIDERS_CONFIG)) {
        props.put(key, value)
      }
    }
    props
  }
}

class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging {

  private[server] val staticBrokerConfigs = ConfigDef.convertToStringMapWithPasswordValues(kafkaConfig.originalsFromThisConfig).asScala
  private[server] val staticDefaultConfigs = ConfigDef.convertToStringMapWithPasswordValues(KafkaConfig.defaultValues.asJava).asScala
  private val dynamicBrokerConfigs = mutable.Map[String, String]()
  private val dynamicDefaultConfigs = mutable.Map[String, String]()

  // Use COWArrayList to prevent concurrent modification exception when an item is added by one thread to these
  // collections, while another thread is iterating over them.
  private[server] val reconfigurables = new CopyOnWriteArrayList[Reconfigurable]()
  private val brokerReconfigurables = new CopyOnWriteArrayList[BrokerReconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var metricsReceiverPluginOpt: Option[ClientMetricsReceiverPlugin] = _
  private var currentConfig: KafkaConfig = _
  private val dynamicConfigPasswordEncoder = if (kafkaConfig.processRoles.isEmpty) {
    maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret)
  } else {
    Some(PasswordEncoder.NOOP)
  }

  private[server] def initialize(zkClientOpt: Option[KafkaZkClient], clientMetricsReceiverPluginOpt: Option[ClientMetricsReceiverPlugin]): Unit = {
    currentConfig = new KafkaConfig(kafkaConfig.props, false)
    metricsReceiverPluginOpt = clientMetricsReceiverPluginOpt

    zkClientOpt.foreach { zkClient =>
      val adminZkClient = new AdminZkClient(zkClient)
      updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.BROKER, ZooKeeperInternals.DEFAULT_STRING), false)
      val props = adminZkClient.fetchEntityConfig(ConfigType.BROKER, kafkaConfig.brokerId.toString)
      val brokerConfig = maybeReEncodePasswords(props, adminZkClient)
      updateBrokerConfig(kafkaConfig.brokerId, brokerConfig)
    }
  }

  /**
   * Clear all cached values. This is used to clear state on broker shutdown to avoid
   * exceptions in tests when broker is restarted. These fields are re-initialized when
   * broker starts up.
   */
  private[server] def clear(): Unit = {
    dynamicBrokerConfigs.clear()
    dynamicDefaultConfigs.clear()
    reconfigurables.clear()
    brokerReconfigurables.clear()
  }

  /**
   * Add reconfigurables to be notified when a dynamic broker config is updated.
   *
   * `Reconfigurable` is the public API used by configurable plugins like metrics reporter
   * and quota callbacks. These are reconfigured before `KafkaConfig` is updated so that
   * the update can be aborted if `reconfigure()` fails with an exception.
   *
   * `BrokerReconfigurable` is used for internal reconfigurable classes. These are
   * reconfigured after `KafkaConfig` is updated so that they can access `KafkaConfig`
   * directly. They are provided both old and new configs.
   */
  def addReconfigurables(kafkaServer: KafkaBroker): Unit = {
    kafkaServer.authorizer match {
      case Some(authz: Reconfigurable) => addReconfigurable(authz)
      case _ =>
    }
    addReconfigurable(kafkaServer.kafkaYammerMetrics)
    addReconfigurable(new DynamicMetricsReporters(kafkaConfig.brokerId, kafkaServer.config, kafkaServer.metrics, kafkaServer.clusterId))
    addReconfigurable(new DynamicClientQuotaCallback(kafkaServer.quotaManagers, kafkaServer.config))

    addBrokerReconfigurable(new BrokerDynamicThreadPool(kafkaServer))
    addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager, kafkaServer))
    addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
    addBrokerReconfigurable(kafkaServer.socketServer)
    addBrokerReconfigurable(new DynamicProducerStateManagerConfig(kafkaServer.logManager.producerStateManagerConfig))
    addBrokerReconfigurable(new DynamicRemoteLogConfig(kafkaServer))
  }

  /**
   * Add reconfigurables to be notified when a dynamic controller config is updated.
   */
  def addReconfigurables(controller: ControllerServer): Unit = {
    controller.authorizer match {
      case Some(authz: Reconfigurable) => addReconfigurable(authz)
      case _ =>
    }
    if (!kafkaConfig.processRoles.contains(ProcessRole.BrokerRole)) {
      // only add these if the controller isn't also running the broker role
      // because these would already be added via the broker in that case
      addReconfigurable(controller.kafkaYammerMetrics)
      addReconfigurable(new DynamicMetricsReporters(kafkaConfig.nodeId, controller.config, controller.metrics, controller.clusterId))
    }
    addReconfigurable(new DynamicClientQuotaCallback(controller.quotaManagers, controller.config))
    addBrokerReconfigurable(new ControllerDynamicThreadPool(controller))
    // TODO: addBrokerReconfigurable(new DynamicListenerConfig(controller))
    addBrokerReconfigurable(controller.socketServer)
  }

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
    reconfigurables.add(reconfigurable)
  }

  def addBrokerReconfigurable(reconfigurable: BrokerReconfigurable): Unit = {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs)
    brokerReconfigurables.add(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = {
    reconfigurables.remove(reconfigurable)
  }

  private def verifyReconfigurableConfigs(configNames: Set[String]): Unit = {
    val nonDynamic = configNames.intersect(DynamicConfig.Broker.nonDynamicProps)
    require(nonDynamic.isEmpty, s"Reconfigurable contains non-dynamic configs $nonDynamic")
  }

  // Visibility for testing
  private[server] def currentKafkaConfig: KafkaConfig = CoreUtils.inReadLock(lock) {
    currentConfig
  }

  private[server] def currentDynamicBrokerConfigs: Map[String, String] = CoreUtils.inReadLock(lock) {
    dynamicBrokerConfigs.clone()
  }

  private[server] def currentDynamicDefaultConfigs: Map[String, String] = CoreUtils.inReadLock(lock) {
    dynamicDefaultConfigs.clone()
  }

  private[server] def clientMetricsReceiverPlugin: Option[ClientMetricsReceiverPlugin] = CoreUtils.inReadLock(lock) {
    metricsReceiverPluginOpt
  }

  private[server] def updateBrokerConfig(brokerId: Int, persistentProps: Properties, doLog: Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
    try {
      val props = fromPersistentProps(persistentProps, perBrokerConfig = true)
      dynamicBrokerConfigs.clear()
      dynamicBrokerConfigs ++= props.asScala
      updateCurrentConfig(doLog)
    } catch {
      case e: Exception => error(s"Per-broker configs of $brokerId could not be applied: ${persistentProps.keys()}", e)
    }
  }

  private[server] def updateDefaultConfig(persistentProps: Properties, doLog: Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
    try {
      val props = fromPersistentProps(persistentProps, perBrokerConfig = false)
      dynamicDefaultConfigs.clear()
      dynamicDefaultConfigs ++= props.asScala
      updateCurrentConfig(doLog)
    } catch {
      case e: Exception => error(s"Cluster default configs could not be applied: ${persistentProps.keys()}", e)
    }
  }

  /**
   * All config updates through ZooKeeper are triggered through actual changes in values stored in ZooKeeper.
   * For some configs like SSL keystores and truststores, we also want to reload the store if it was modified
   * in-place, even though the actual value of the file path and password haven't changed. This scenario alone
   * is handled here when a config update request using admin client is processed by ZkAdminManager. If any of
   * the SSL configs have changed, then the update will not be done here, but will be handled later when ZK
   * changes are processed. At the moment, only listener configs are considered for reloading.
   */
  private[server] def reloadUpdatedFilesWithoutConfigChange(newProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables.forEach(r => {
      if (ReloadableFileConfigs.exists(r.reconfigurableConfigs.contains)) {
        r match {
          case reconfigurable: ListenerReconfigurable =>
            val kafkaProps = validatedKafkaProps(newProps, perBrokerConfig = true)
            val newConfig = new KafkaConfig(kafkaProps.asJava, false)
            processListenerReconfigurable(reconfigurable, newConfig, Collections.emptyMap(), validateOnly = false, reloadOnly = true)
          case reconfigurable =>
            trace(s"Files will not be reloaded without config change for $reconfigurable")
        }
      }
    })
  }

  private def maybeCreatePasswordEncoder(secret: Option[Password]): Option[PasswordEncoder] = {
   secret.map { secret =>
     PasswordEncoder.encrypting(secret,
        kafkaConfig.passwordEncoderKeyFactoryAlgorithm,
        kafkaConfig.passwordEncoderCipherAlgorithm,
        kafkaConfig.passwordEncoderKeyLength,
        kafkaConfig.passwordEncoderIterations)
    }
  }

  private def passwordEncoder: PasswordEncoder = {
    dynamicConfigPasswordEncoder.getOrElse(throw new ConfigException("Password encoder secret not configured"))
  }

  private[server] def toPersistentProps(configProps: Properties, perBrokerConfig: Boolean): Properties = {
    val props = configProps.clone().asInstanceOf[Properties]

    def encodePassword(configName: String, value: String): Unit = {
      if (value != null) {
        if (!perBrokerConfig)
          throw new ConfigException("Password config can be defined only at broker level")
        props.setProperty(configName, passwordEncoder.encode(new Password(value)))
      }
    }
    configProps.asScala.foreachEntry { (name, value) =>
      if (isPasswordConfig(name))
        encodePassword(name, value)
    }
    props
  }

  private[server] def fromPersistentProps(persistentProps: Properties,
                                          perBrokerConfig: Boolean): Properties = {
    val props = persistentProps.clone().asInstanceOf[Properties]

    // Remove all invalid configs from `props`
    removeInvalidConfigs(props, perBrokerConfig)
    def removeInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
      if (invalidPropNames.nonEmpty) {
        invalidPropNames.foreach(props.remove)
        error(s"$errorMessage: $invalidPropNames")
      }
    }
    removeInvalidProps(nonDynamicConfigs(props), "Non-dynamic configs configured in ZooKeeper will be ignored")
    removeInvalidProps(securityConfigsWithoutListenerPrefix(props),
      "Security configs can be dynamically updated only using listener prefix, base configs will be ignored")
    if (!perBrokerConfig)
      removeInvalidProps(perBrokerConfigs(props), "Per-broker configs defined at default cluster level will be ignored")

    def decodePassword(configName: String, value: String): Unit = {
      if (value != null) {
        try {
          props.setProperty(configName, passwordEncoder.decode(value).value)
        } catch {
          case e: Exception =>
            error(s"Dynamic password config $configName could not be decoded, ignoring.", e)
            props.remove(configName)
        }
      }
    }

    props.asScala.foreachEntry { (name, value) =>
      if (isPasswordConfig(name))
        decodePassword(name, value)
    }
    props
  }

  // If the secret has changed, password.encoder.old.secret contains the old secret that was used
  // to encode the configs in ZK. Decode passwords using the old secret and update ZK with values
  // encoded using the current secret. Ignore any errors during decoding since old secret may not
  // have been removed during broker restart.
  private def maybeReEncodePasswords(persistentProps: Properties, adminZkClient: AdminZkClient): Properties = {
    val props = persistentProps.clone().asInstanceOf[Properties]
    if (props.asScala.keySet.exists(isPasswordConfig)) {
      maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderOldSecret).foreach { passwordDecoder =>
        persistentProps.asScala.foreachEntry { (configName, value) =>
          if (isPasswordConfig(configName) && value != null) {
            val decoded = try {
              Some(passwordDecoder.decode(value).value)
            } catch {
              case _: Exception =>
                debug(s"Dynamic password config $configName could not be decoded using old secret, new secret will be used.")
                None
            }
            decoded.foreach(value => props.put(configName, passwordEncoder.encode(new Password(value))))
          }
        }
        adminZkClient.changeBrokerConfig(Some(kafkaConfig.brokerId), props)
      }
    }
    props
  }

  /**
   * Validate the provided configs `propsOverride` and return the full Kafka configs with
   * the configured defaults and these overrides.
   *
   * Note: The caller must acquire the read or write lock before invoking this method.
   */
  private def validatedKafkaProps(propsOverride: Properties, perBrokerConfig: Boolean): Map[String, String] = {
    val propsResolved = DynamicBrokerConfig.resolveVariableConfigs(propsOverride)
    validateConfigs(propsResolved, perBrokerConfig)
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    if (perBrokerConfig) {
      overrideProps(newProps, dynamicDefaultConfigs)
      overrideProps(newProps, propsResolved.asScala)
    } else {
      overrideProps(newProps, propsResolved.asScala)
      overrideProps(newProps, dynamicBrokerConfigs)
    }
    newProps
  }

  private[server] def validate(props: Properties, perBrokerConfig: Boolean): Unit = CoreUtils.inReadLock(lock) {
    val newProps = validatedKafkaProps(props, perBrokerConfig)
    processReconfiguration(newProps, validateOnly = true)
  }

  private def removeInvalidConfigs(props: Properties, perBrokerConfig: Boolean): Unit = {
    try {
      validateConfigTypes(props)
      props.asScala
    } catch {
      case e: Exception =>
        val invalidProps = props.asScala.filter { case (k, v) =>
          val props1 = new Properties
          props1.put(k, v)
          try {
            validateConfigTypes(props1)
            false
          } catch {
            case _: Exception => true
          }
        }
        invalidProps.keys.foreach(props.remove)
        val configSource = if (perBrokerConfig) "broker" else "default cluster"
        error(s"Dynamic $configSource config contains invalid values in: ${invalidProps.keys}, these configs will be ignored", e)
    }
  }

  private[server] def maybeReconfigure(reconfigurable: Reconfigurable, oldConfig: KafkaConfig, newConfig: util.Map[String, _]): Unit = {
    if (reconfigurable.reconfigurableConfigs.asScala.exists(key => oldConfig.originals.get(key) != newConfig.get(key)))
      reconfigurable.reconfigure(newConfig)
  }

  /**
   * Returns the change in configurations between the new props and current props by returning a
   * map of the changed configs, as well as the set of deleted keys
   */
  private def updatedConfigs(newProps: java.util.Map[String, _],
                             currentProps: java.util.Map[String, _]): (mutable.Map[String, _], Set[String]) = {
    val changeMap = newProps.asScala.filter {
      case (k, v) => v != currentProps.get(k)
    }
    val deletedKeySet = currentProps.asScala.filter {
      case (k, _) => !newProps.containsKey(k)
    }.keySet
    (changeMap, deletedKeySet)
  }

  /**
    * Updates values in `props` with the new values from `propsOverride`. Synonyms of updated configs
    * are removed from `props` to ensure that the config with the higher precedence is applied. For example,
    * if `log.roll.ms` was defined in server.properties and `log.roll.hours` is configured dynamically,
    * `log.roll.hours` from the dynamic configuration will be used and `log.roll.ms` will be removed from
    * `props` (even though `log.roll.hours` is secondary to `log.roll.ms`).
    */
  private def overrideProps(props: mutable.Map[String, String], propsOverride: mutable.Map[String, String]): Unit = {
    propsOverride.foreachEntry { (k, v) =>
      // Remove synonyms of `k` to ensure the right precedence is applied. But disable `matchListenerOverride`
      // so that base configs corresponding to listener configs are not removed. Base configs should not be removed
      // since they may be used by other listeners. It is ok to retain them in `props` since base configs cannot be
      // dynamically updated and listener-specific configs have the higher precedence.
      brokerConfigSynonyms(k, matchListenerOverride = false).foreach(props.remove)
      props.put(k, v)
    }
  }

  private def updateCurrentConfig(doLog: Boolean): Unit = {
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    overrideProps(newProps, dynamicDefaultConfigs)
    overrideProps(newProps, dynamicBrokerConfigs)

    val oldConfig = currentConfig
    val (newConfig, brokerReconfigurablesToUpdate) = processReconfiguration(newProps, validateOnly = false, doLog)
    if (newConfig ne currentConfig) {
      currentConfig = newConfig
      kafkaConfig.updateCurrentConfig(newConfig)

      // Process BrokerReconfigurable updates after current config is updated
      brokerReconfigurablesToUpdate.foreach(_.reconfigure(oldConfig, newConfig))
    }
  }

  private def processReconfiguration(newProps: Map[String, String], validateOnly: Boolean, doLog: Boolean = false): (KafkaConfig, List[BrokerReconfigurable]) = {
    val newConfig = new KafkaConfig(newProps.asJava, doLog)
    val (changeMap, deletedKeySet) = updatedConfigs(newConfig.originalsFromThisConfig, currentConfig.originals)
    if (changeMap.nonEmpty || deletedKeySet.nonEmpty) {
      try {
        val customConfigs = new util.HashMap[String, Object](newConfig.originalsFromThisConfig) // non-Kafka configs
        newConfig.valuesFromThisConfig.keySet.forEach(k => customConfigs.remove(k))
        reconfigurables.forEach {
          case listenerReconfigurable: ListenerReconfigurable =>
            processListenerReconfigurable(listenerReconfigurable, newConfig, customConfigs, validateOnly, reloadOnly = false)
          case reconfigurable =>
            if (needsReconfiguration(reconfigurable.reconfigurableConfigs, changeMap.keySet, deletedKeySet))
              processReconfigurable(reconfigurable, changeMap.keySet, newConfig.valuesFromThisConfig, customConfigs, validateOnly)
        }

        // BrokerReconfigurable updates are processed after config is updated. Only do the validation here.
        val brokerReconfigurablesToUpdate = mutable.Buffer[BrokerReconfigurable]()
        brokerReconfigurables.forEach { reconfigurable =>
          if (needsReconfiguration(reconfigurable.reconfigurableConfigs.asJava, changeMap.keySet, deletedKeySet)) {
            reconfigurable.validateReconfiguration(newConfig)
            if (!validateOnly)
              brokerReconfigurablesToUpdate += reconfigurable
          }
        }
        (newConfig, brokerReconfigurablesToUpdate.toList)
      } catch {
        case e: Exception =>
          if (!validateOnly)
            error(s"Failed to update broker configuration with configs : " +
                  s"${ConfigUtils.configMapToRedactedString(newConfig.originalsFromThisConfig, KafkaConfig.configDef)}", e)
          throw new ConfigException("Invalid dynamic configuration", e)
      }
    }
    else
      (currentConfig, List.empty)
  }

  private def needsReconfiguration(reconfigurableConfigs: util.Set[String], updatedKeys: Set[String], deletedKeys: Set[String]): Boolean = {
    reconfigurableConfigs.asScala.intersect(updatedKeys).nonEmpty ||
      reconfigurableConfigs.asScala.intersect(deletedKeys).nonEmpty
  }

  private def processListenerReconfigurable(listenerReconfigurable: ListenerReconfigurable,
                                            newConfig: KafkaConfig,
                                            customConfigs: util.Map[String, Object],
                                            validateOnly: Boolean,
                                            reloadOnly:  Boolean): Unit = {
    val listenerName = listenerReconfigurable.listenerName
    val oldValues = currentConfig.valuesWithPrefixOverride(listenerName.configPrefix)
    val newValues = newConfig.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    val (changeMap, deletedKeys) = updatedConfigs(newValues, oldValues)
    val updatedKeys = changeMap.keySet
    val configsChanged = needsReconfiguration(listenerReconfigurable.reconfigurableConfigs, updatedKeys, deletedKeys)
    // if `reloadOnly`, reconfigure if configs haven't changed. Otherwise reconfigure if configs have changed
    if (reloadOnly != configsChanged)
      processReconfigurable(listenerReconfigurable, updatedKeys, newValues, customConfigs, validateOnly)
  }

  private def processReconfigurable(reconfigurable: Reconfigurable,
                                    updatedConfigNames: Set[String],
                                    allNewConfigs: util.Map[String, _],
                                    newCustomConfigs: util.Map[String, Object],
                                    validateOnly: Boolean): Unit = {
    val newConfigs = new util.HashMap[String, Object]
    allNewConfigs.forEach((k, v) => newConfigs.put(k, v.asInstanceOf[AnyRef]))
    newConfigs.putAll(newCustomConfigs)
    try {
      reconfigurable.validateReconfiguration(newConfigs)
    } catch {
      case e: ConfigException => throw e
      case _: Exception =>
        throw new ConfigException(s"Validation of dynamic config update of $updatedConfigNames failed with class ${reconfigurable.getClass}")
    }

    if (!validateOnly) {
      info(s"Reconfiguring $reconfigurable, updated configs: $updatedConfigNames " +
           s"custom configs: ${ConfigUtils.configMapToRedactedString(newCustomConfigs, KafkaConfig.configDef)}")
      reconfigurable.reconfigure(newConfigs)
    }
  }
}

trait BrokerReconfigurable {

  def reconfigurableConfigs: Set[String]

  def validateReconfiguration(newConfig: KafkaConfig): Unit

  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit
}

object DynamicLogConfig {
  /**
   * The broker configurations pertaining to logs that are reconfigurable. This set contains
   * the names you would use when setting a static or dynamic broker configuration (not topic
   * configuration).
   */
  val ReconfigurableConfigs: Set[String] =
    ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.asScala.values.toSet
}

class DynamicLogConfig(logManager: LogManager, server: KafkaBroker) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicLogConfig.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    // For update of topic config overrides, only config names and types are validated
    // Names and types have already been validated. For consistency with topic config
    // validation, no additional validation is performed.

    def validateLogLocalRetentionMs(): Unit = {
      val logRetentionMs = newConfig.logRetentionTimeMillis
      val logLocalRetentionMs: java.lang.Long = newConfig.remoteLogManagerConfig.logLocalRetentionMs
      if (logRetentionMs != -1L && logLocalRetentionMs != -2L) {
        if (logLocalRetentionMs == -1L) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
            s"Value must not be -1 as ${ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG} value is set as $logRetentionMs.")
        }
        if (logLocalRetentionMs > logRetentionMs) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
            s"Value must not be more than ${ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG} property value: $logRetentionMs")
        }
      }
    }

    def validateLogLocalRetentionBytes(): Unit = {
      val logRetentionBytes = newConfig.logRetentionBytes
      val logLocalRetentionBytes: java.lang.Long = newConfig.remoteLogManagerConfig.logLocalRetentionBytes
      if (logRetentionBytes > -1 && logLocalRetentionBytes != -2) {
        if (logLocalRetentionBytes == -1) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
            s"Value must not be -1 as ${ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG} value is set as $logRetentionBytes.")
        }
        if (logLocalRetentionBytes > logRetentionBytes) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
            s"Value must not be more than ${ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG} property value: $logRetentionBytes")
        }
      }
    }

    validateLogLocalRetentionMs()
    validateLogLocalRetentionBytes()
  }

  private def updateLogsConfig(newBrokerDefaults: Map[String, Object]): Unit = {
    logManager.brokerConfigUpdated()
    logManager.allLogs.foreach { log =>
      val props = mutable.Map.empty[Any, Any]
      props ++= newBrokerDefaults
      props ++= log.config.originals.asScala.filter { case (k, _) =>
        log.config.overriddenConfigs.contains(k)
      }

      val logConfig = new LogConfig(props.asJava, log.config.overriddenConfigs)
      log.updateConfig(logConfig)
    }
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val originalLogConfig = logManager.currentDefaultConfig
    val originalUncleanLeaderElectionEnable = originalLogConfig.uncleanLeaderElectionEnable
    val newBrokerDefaults = new util.HashMap[String, Object](newConfig.extractLogConfigMap)

    logManager.reconfigureDefaultLogConfig(new LogConfig(newBrokerDefaults))

    updateLogsConfig(newBrokerDefaults.asScala)

    if (logManager.currentDefaultConfig.uncleanLeaderElectionEnable && !originalUncleanLeaderElectionEnable) {
      server match {
        case kafkaServer: KafkaServer => kafkaServer.kafkaController.enableDefaultUncleanLeaderElection()
        case _ =>
      }
    }
  }
}

object DynamicThreadPool {
  val ReconfigurableConfigs = Set(
    ServerConfigs.NUM_IO_THREADS_CONFIG,
    ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG,
    ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG,
    ServerConfigs.BACKGROUND_THREADS_CONFIG)

  def validateReconfiguration(currentConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    newConfig.values.forEach { (k, v) =>
      if (ReconfigurableConfigs.contains(k)) {
        val newValue = v.asInstanceOf[Int]
        val oldValue = getValue(currentConfig, k)
        if (newValue != oldValue) {
          val errorMsg = s"Dynamic thread count update validation failed for $k=$v"
          if (newValue <= 0)
            throw new ConfigException(s"$errorMsg, value should be at least 1")
          if (newValue < oldValue / 2)
            throw new ConfigException(s"$errorMsg, value should be at least half the current value $oldValue")
          if (newValue > oldValue * 2)
            throw new ConfigException(s"$errorMsg, value should not be greater than double the current value $oldValue")
        }
      }
    }
  }

  def getValue(config: KafkaConfig, name: String): Int = {
    name match {
      case ServerConfigs.NUM_IO_THREADS_CONFIG => config.numIoThreads
      case ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG => config.numReplicaFetchers
      case ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG => config.numRecoveryThreadsPerDataDir
      case ServerConfigs.BACKGROUND_THREADS_CONFIG => config.backgroundThreads
      case n => throw new IllegalStateException(s"Unexpected config $n")
    }
  }
}

class ControllerDynamicThreadPool(controller: ControllerServer) extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    Set(ServerConfigs.NUM_IO_THREADS_CONFIG)
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    DynamicThreadPool.validateReconfiguration(controller.config, newConfig) // common validation
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (newConfig.numIoThreads != oldConfig.numIoThreads)
      controller.controllerApisHandlerPool.resizeThreadPool(newConfig.numIoThreads)
  }
}

class BrokerDynamicThreadPool(server: KafkaBroker) extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicThreadPool.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    DynamicThreadPool.validateReconfiguration(server.config, newConfig)
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (newConfig.numIoThreads != oldConfig.numIoThreads)
      server.dataPlaneRequestHandlerPool.resizeThreadPool(newConfig.numIoThreads)
    if (newConfig.numReplicaFetchers != oldConfig.numReplicaFetchers)
      server.replicaManager.resizeFetcherThreadPool(newConfig.numReplicaFetchers)
    if (newConfig.numRecoveryThreadsPerDataDir != oldConfig.numRecoveryThreadsPerDataDir)
      server.logManager.resizeRecoveryThreadPool(newConfig.numRecoveryThreadsPerDataDir)
    if (newConfig.backgroundThreads != oldConfig.backgroundThreads)
      server.kafkaScheduler.resizeThreadPool(newConfig.backgroundThreads)
  }
}

class DynamicMetricsReporters(brokerId: Int, config: KafkaConfig, metrics: Metrics, clusterId: String) extends Reconfigurable {
  private val reporterState = new DynamicMetricReporterState(brokerId, config, metrics, clusterId)
  private[server] val currentReporters = reporterState.currentReporters
  private val dynamicConfig = reporterState.dynamicConfig

  private def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] =
    reporterState.metricsReporterClasses(configs)

  private def createReporters(reporterClasses: util.List[String], updatedConfigs: util.Map[String, _]): Unit =
    reporterState.createReporters(reporterClasses, updatedConfigs)

  private def removeReporter(className: String): Unit = reporterState.removeReporter(className)

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    val configs = new util.HashSet[String]()
    configs.add(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG)
    currentReporters.values.foreach {
      case reporter: Reconfigurable => configs.addAll(reporter.reconfigurableConfigs)
      case _ =>
    }
    configs
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    val updatedMetricsReporters = metricsReporterClasses(configs)

    // Ensure all the reporter classes can be loaded and have a default constructor
    updatedMetricsReporters.foreach { className =>
      val clazz = Utils.loadClass(className, classOf[MetricsReporter])
      clazz.getConstructor()
    }

    // Validate the new configuration using every reconfigurable reporter instance that is not being deleted
    currentReporters.values.foreach {
      case reporter: Reconfigurable =>
        if (updatedMetricsReporters.contains(reporter.getClass.getName))
          reporter.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val updatedMetricsReporters = metricsReporterClasses(configs)
    val deleted = currentReporters.keySet.toSet -- updatedMetricsReporters
    deleted.foreach(removeReporter)
    currentReporters.values.foreach {
      case reporter: Reconfigurable => dynamicConfig.maybeReconfigure(reporter, dynamicConfig.currentKafkaConfig, configs)
      case _ =>
    }
    val added = updatedMetricsReporters.filterNot(currentReporters.keySet)
    createReporters(added.asJava, configs)
  }
}

class DynamicMetricReporterState(brokerId: Int, config: KafkaConfig, metrics: Metrics, clusterId: String) {
  private[server] val dynamicConfig = config.dynamicConfig
  private val propsOverride = Map[String, AnyRef](ServerConfigs.BROKER_ID_CONFIG -> brokerId.toString)
  private[server] val currentReporters = mutable.Map[String, MetricsReporter]()
  createReporters(config, clusterId, metricsReporterClasses(dynamicConfig.currentKafkaConfig.values()).asJava,
    Collections.emptyMap[String, Object])

  private[server] def createReporters(reporterClasses: util.List[String],
                                      updatedConfigs: util.Map[String, _]): Unit = {
    createReporters(config, clusterId, reporterClasses, updatedConfigs)
  }

  private def createReporters(config: KafkaConfig,
                              clusterId: String,
                              reporterClasses: util.List[String],
                              updatedConfigs: util.Map[String, _]): Unit = {
    val props = new util.HashMap[String, AnyRef]
    updatedConfigs.forEach((k, v) => props.put(k, v.asInstanceOf[AnyRef]))
    propsOverride.foreachEntry((k, v) => props.put(k, v))
    val reporters = dynamicConfig.currentKafkaConfig.getConfiguredInstances(reporterClasses, classOf[MetricsReporter], props)

    // Call notifyMetricsReporters first to satisfy the contract for MetricsReporter.contextChange,
    // which provides that MetricsReporter.contextChange must be called before the first call to MetricsReporter.init.
    // The first call to MetricsReporter.init is done when we call metrics.addReporter below.
    KafkaBroker.notifyMetricsReporters(clusterId, config, reporters.asScala)
    reporters.forEach { reporter =>
      metrics.addReporter(reporter)
      currentReporters += reporter.getClass.getName -> reporter
      val clientTelemetryReceiver = reporter match {
        case telemetry: ClientTelemetry => telemetry.clientReceiver()
        case _ => null
      }

      if (clientTelemetryReceiver != null) {
        dynamicConfig.clientMetricsReceiverPlugin match {
          case Some(receiverPlugin) =>
            receiverPlugin.add(clientTelemetryReceiver)
          case None =>
            // Do nothing
        }
      }
    }
    KafkaBroker.notifyClusterListeners(clusterId, reporters.asScala)
  }

  private[server] def removeReporter(className: String): Unit = {
    currentReporters.remove(className).foreach(metrics.removeReporter)
  }

  private[server] def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] = {
    val reporters = mutable.Buffer[String]()
    reporters ++= configs.get(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG).asInstanceOf[util.List[String]].asScala
    reporters
  }
}

object DynamicListenerConfig {
  /**
   * The set of configurations which the DynamicListenerConfig object listens for. Many of
   * these are also monitored by other objects such as ChannelBuilders and SocketServers.
   */
  val ReconfigurableConfigs = Set(
    // Listener configs
    SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG,
    SocketServerConfigs.LISTENERS_CONFIG,
    SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG,

    // SSL configs
    BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
    SslConfigs.SSL_PROTOCOL_CONFIG,
    SslConfigs.SSL_PROVIDER_CONFIG,
    SslConfigs.SSL_CIPHER_SUITES_CONFIG,
    SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG,
    SslConfigs.SSL_KEYSTORE_TYPE_CONFIG,
    SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
    SslConfigs.SSL_KEY_PASSWORD_CONFIG,
    SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
    SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG,
    SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG,
    SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
    SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG,
    BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG,
    SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG,

    // SASL configs
    BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG,
    SaslConfigs.SASL_JAAS_CONFIG,
    BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG,
    SaslConfigs.SASL_KERBEROS_SERVICE_NAME,
    SaslConfigs.SASL_KERBEROS_KINIT_CMD,
    SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR,
    SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER,
    SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN,
    BrokerSecurityConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES_CONFIG,
    SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_FACTOR,
    SaslConfigs.SASL_LOGIN_REFRESH_WINDOW_JITTER,
    SaslConfigs.SASL_LOGIN_REFRESH_MIN_PERIOD_SECONDS,
    SaslConfigs.SASL_LOGIN_REFRESH_BUFFER_SECONDS,

    // Connection limit configs
    SocketServerConfigs.MAX_CONNECTIONS_CONFIG,
    SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG,

    // Network threads
    SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG
  )
}

class DynamicClientQuotaCallback(
  quotaManagers: QuotaFactory.QuotaManagers,
  serverConfig: KafkaConfig
) extends Reconfigurable {

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    val configs = new util.HashSet[String]()
    quotaManagers.clientQuotaCallback.ifPresent {
      case callback: Reconfigurable => configs.addAll(callback.reconfigurableConfigs)
      case _ =>
    }
    configs
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    quotaManagers.clientQuotaCallback.ifPresent {
      case callback: Reconfigurable => callback.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    quotaManagers.clientQuotaCallback.ifPresent {
      case callback: Reconfigurable =>
        serverConfig.dynamicConfig.maybeReconfigure(callback, serverConfig.dynamicConfig.currentKafkaConfig, configs)
      case _ =>
    }
  }
}

class DynamicListenerConfig(server: KafkaBroker) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicListenerConfig.ReconfigurableConfigs
  }

  private def listenerRegistrationsAltered(
    oldAdvertisedListeners: Map[ListenerName, EndPoint],
    newAdvertisedListeners: Map[ListenerName, EndPoint]
  ): Boolean = {
    if (oldAdvertisedListeners.size != newAdvertisedListeners.size) return true
    oldAdvertisedListeners.foreachEntry {
      case (oldListenerName, oldEndpoint) =>
        newAdvertisedListeners.get(oldListenerName) match {
          case None => return true
          case Some(newEndpoint) => if (!newEndpoint.equals(oldEndpoint)) {
            return true
          }
        }
    }
    false
  }

  private def verifyListenerRegistrationAlterationSupported(): Unit = {
    if (!server.config.requiresZookeeper) {
      throw new ConfigException("Advertised listeners cannot be altered when using a " +
        "Raft-based metadata quorum.")
    }
  }

  def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    val oldConfig = server.config
    val newListeners = listenersToMap(newConfig.listeners)
    val newAdvertisedListeners = listenersToMap(newConfig.effectiveAdvertisedBrokerListeners)
    val oldListeners = listenersToMap(oldConfig.listeners)
    if (!newAdvertisedListeners.keySet.subsetOf(newListeners.keySet))
      throw new ConfigException(s"Advertised listeners '$newAdvertisedListeners' must be a subset of listeners '$newListeners'")
    if (!newListeners.keySet.subsetOf(newConfig.effectiveListenerSecurityProtocolMap.keySet))
      throw new ConfigException(s"Listeners '$newListeners' must be subset of listener map '${newConfig.effectiveListenerSecurityProtocolMap}'")
    newListeners.keySet.intersect(oldListeners.keySet).foreach { listenerName =>
      def immutableListenerConfigs(kafkaConfig: KafkaConfig, prefix: String): Map[String, AnyRef] = {
        kafkaConfig.originalsWithPrefix(prefix, true).asScala.filter { case (key, _) =>
          // skip the reconfigurable configs
          !DynamicSecurityConfigs.contains(key) && !SocketServer.ListenerReconfigurableConfigs.contains(key) && !DataPlaneAcceptor.ListenerReconfigurableConfigs.contains(key)
        }
      }
      if (immutableListenerConfigs(newConfig, listenerName.configPrefix) != immutableListenerConfigs(oldConfig, listenerName.configPrefix))
        throw new ConfigException(s"Configs cannot be updated dynamically for existing listener $listenerName, " +
          "restart broker or create a new listener for update")
      if (oldConfig.effectiveListenerSecurityProtocolMap(listenerName) != newConfig.effectiveListenerSecurityProtocolMap(listenerName))
        throw new ConfigException(s"Security protocol cannot be updated for existing listener $listenerName")
    }
    if (!newAdvertisedListeners.contains(newConfig.interBrokerListenerName))
      throw new ConfigException(s"Advertised listener must be specified for inter-broker listener ${newConfig.interBrokerListenerName}")

    // Currently, we do not support adding or removing listeners when in KRaft mode.
    // However, we support changing other listener configurations (max connections, etc.)
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedBrokerListeners),
        listenersToMap(newConfig.effectiveAdvertisedBrokerListeners))) {
      verifyListenerRegistrationAlterationSupported()
    }
  }

  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val newListeners = newConfig.listeners
    val newListenerMap = listenersToMap(newListeners)
    val oldListeners = oldConfig.listeners
    val oldListenerMap = listenersToMap(oldListeners)
    val listenersRemoved = oldListeners.filterNot(e => newListenerMap.contains(e.listenerName))
    val listenersAdded = newListeners.filterNot(e => oldListenerMap.contains(e.listenerName))
    if (listenersRemoved.nonEmpty || listenersAdded.nonEmpty) {
      LoginManager.closeAll() // Clear SASL login cache to force re-login
      if (listenersRemoved.nonEmpty) server.socketServer.removeListeners(listenersRemoved)
      if (listenersAdded.nonEmpty) server.socketServer.addListeners(listenersAdded)
    }
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedBrokerListeners),
        listenersToMap(newConfig.effectiveAdvertisedBrokerListeners))) {
      verifyListenerRegistrationAlterationSupported()
      server match {
        case kafkaServer: KafkaServer => kafkaServer.kafkaController.updateBrokerInfo(kafkaServer.createBrokerInfo)
        case _ => throw new RuntimeException("Unable to handle non-kafkaServer")
      }
    }
  }

  private def listenersToMap(listeners: Seq[EndPoint]): Map[ListenerName, EndPoint] =
    listeners.map(e => (e.listenerName, e)).toMap

}

class DynamicProducerStateManagerConfig(val producerStateManagerConfig: ProducerStateManagerConfig) extends BrokerReconfigurable with Logging {
  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (producerStateManagerConfig.producerIdExpirationMs != newConfig.transactionLogConfig.producerIdExpirationMs) {
      info(s"Reconfigure ${TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG} from ${producerStateManagerConfig.producerIdExpirationMs} to ${newConfig.transactionLogConfig.producerIdExpirationMs}")
      producerStateManagerConfig.setProducerIdExpirationMs(newConfig.transactionLogConfig.producerIdExpirationMs)
    }
    if (producerStateManagerConfig.transactionVerificationEnabled != newConfig.transactionLogConfig.transactionPartitionVerificationEnable) {
      info(s"Reconfigure ${TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG} from ${producerStateManagerConfig.transactionVerificationEnabled} to ${newConfig.transactionLogConfig.transactionPartitionVerificationEnable}")
      producerStateManagerConfig.setTransactionVerificationEnabled(newConfig.transactionLogConfig.transactionPartitionVerificationEnable)
    }
  }

  def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    if (newConfig.transactionLogConfig.producerIdExpirationMs < 0)
      throw new ConfigException(s"${TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_CONFIG} cannot be less than 0, current value is ${producerStateManagerConfig.producerIdExpirationMs}, and new value is ${newConfig.transactionLogConfig.producerIdExpirationMs}")
  }

  override def reconfigurableConfigs: Set[String] = DynamicProducerStateManagerConfig

}

class DynamicRemoteLogConfig(server: KafkaBroker) extends BrokerReconfigurable with Logging {
  override def reconfigurableConfigs: Set[String] = {
    DynamicRemoteLogConfig.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    newConfig.values.forEach { (k, v) =>
      if (RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP.equals(k) ||
        RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP.equals(k) ||
        RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP.equals(k)) {
        val newValue = v.asInstanceOf[Long]
        val oldValue = getValue(server.config, k)
        if (newValue != oldValue && newValue <= 0) {
          val errorMsg = s"Dynamic remote log manager config update validation failed for $k=$v"
          throw new ConfigException(s"$errorMsg, value should be at least 1")
        }
      }

      if (RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP.equals(k) ||
          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP.equals(k) ||
          RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP.equals(k)) {
        val newValue = v.asInstanceOf[Int]
        val oldValue = server.config.getInt(k)
        if (newValue != oldValue) {
          val errorMsg = s"Dynamic thread count update validation failed for $k=$v"
          if (newValue <= 0)
            throw new ConfigException(s"$errorMsg, value should be at least 1")
          if (newValue < oldValue / 2)
            throw new ConfigException(s"$errorMsg, value should be at least half the current value $oldValue")
          if (newValue > oldValue * 2)
            throw new ConfigException(s"$errorMsg, value should not be greater than double the current value $oldValue")
        }
      }
    }
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    def oldLongValue(k: String): Long = oldConfig.getLong(k)
    def newLongValue(k: String): Long = newConfig.getLong(k)

    def isChangedLongValue(k : String): Boolean = oldLongValue(k) != newLongValue(k)

    if (server.remoteLogManagerOpt.nonEmpty) {
      val remoteLogManager = server.remoteLogManagerOpt.get
      if (isChangedLongValue(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)) {
        val oldValue = oldLongValue(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)
        val newValue = newLongValue(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)
        remoteLogManager.resizeCacheSize(newValue)
        info(s"Dynamic remote log manager config: ${RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP} updated, " +
          s"old value: $oldValue, new value: $newValue")
      }
      if (isChangedLongValue(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP)) {
        val oldValue = oldLongValue(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP)
        val newValue = newLongValue(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP)
        remoteLogManager.updateCopyQuota(newValue)
        info(s"Dynamic remote log manager config: ${RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP} updated, " +
          s"old value: $oldValue, new value: $newValue")
      }
      if (isChangedLongValue(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP)) {
        val oldValue = oldLongValue(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP)
        val newValue = newLongValue(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP)
        remoteLogManager.updateFetchQuota(newValue)
        info(s"Dynamic remote log manager config: ${RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP} updated, " +
          s"old value: $oldValue, new value: $newValue")
      }

      val newRLMConfig = newConfig.remoteLogManagerConfig
      val oldRLMConfig = oldConfig.remoteLogManagerConfig
      if (newRLMConfig.remoteLogManagerCopierThreadPoolSize() != oldRLMConfig.remoteLogManagerCopierThreadPoolSize())
        remoteLogManager.resizeCopierThreadPool(newRLMConfig.remoteLogManagerCopierThreadPoolSize())

      if (newRLMConfig.remoteLogManagerExpirationThreadPoolSize() != oldRLMConfig.remoteLogManagerExpirationThreadPoolSize())
        remoteLogManager.resizeExpirationThreadPool(newRLMConfig.remoteLogManagerExpirationThreadPoolSize())

      if (newRLMConfig.remoteLogReaderThreads() != oldRLMConfig.remoteLogReaderThreads())
        remoteLogManager.resizeReaderThreadPool(newRLMConfig.remoteLogReaderThreads())
    }
  }

  private def getValue(config: KafkaConfig, name: String): Long = {
    name match {
      case RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP |
           RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP |
           RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP =>
        config.getLong(name)
      case n => throw new IllegalStateException(s"Unexpected dynamic remote log manager config $n")
    }
  }
}

object DynamicRemoteLogConfig {
  val ReconfigurableConfigs = Set(
    RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP,
    RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP,
    RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP,
    RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP,
    RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP,
    RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP,
    RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP,
    RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP
  )
}
