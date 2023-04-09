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
import kafka.log.{LogCleaner, LogConfig, LogManager, ProducerStateManagerConfig}
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.server.DynamicBrokerConfig._
import kafka.utils.{CoreUtils, Logging, PasswordEncoder}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef, ConfigException, SslConfigs}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.{ListenerName, ListenerReconfigurable}
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.utils.{ConfigUtils, Utils}

import scala.annotation.nowarn
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
  * See [[kafka.log.LogConfig#TopicConfigSynonyms]] for the mapping.
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

  val AllDynamicConfigs = DynamicSecurityConfigs ++
    LogCleaner.ReconfigurableConfigs ++
    DynamicLogConfig.ReconfigurableConfigs ++
    DynamicThreadPool.ReconfigurableConfigs ++
    Set(KafkaConfig.MetricReporterClassesProp) ++
    DynamicListenerConfig.ReconfigurableConfigs ++
    SocketServer.ReconfigurableConfigs ++
    ProducerStateManagerConfig.ReconfigurableConfigs

  private val ClusterLevelListenerConfigs = Set(KafkaConfig.MaxConnectionsProp, KafkaConfig.MaxConnectionCreationRateProp, KafkaConfig.NumNetworkThreadsProp)
  private val PerBrokerConfigs = (DynamicSecurityConfigs ++ DynamicListenerConfig.ReconfigurableConfigs).diff(
    ClusterLevelListenerConfigs)
  private val ListenerMechanismConfigs = Set(KafkaConfig.SaslJaasConfigProp,
    KafkaConfig.SaslLoginCallbackHandlerClassProp,
    KafkaConfig.SaslLoginClassProp,
    KafkaConfig.SaslServerCallbackHandlerClassProp,
    KafkaConfig.ConnectionsMaxReauthMsProp)

  private val ReloadableFileConfigs = Set(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG)

  val ListenerConfigRegex = """listener\.name\.[^.]*\.(.*)""".r

  private val DynamicPasswordConfigs = {
    val passwordConfigs = KafkaConfig.configKeys.filter(_._2.`type` == ConfigDef.Type.PASSWORD).keySet
    AllDynamicConfigs.intersect(passwordConfigs)
  }

  def isPasswordConfig(name: String): Boolean = DynamicBrokerConfig.DynamicPasswordConfigs.exists(name.endsWith)

  def brokerConfigSynonyms(name: String, matchListenerOverride: Boolean): List[String] = {
    name match {
      case KafkaConfig.LogRollTimeMillisProp | KafkaConfig.LogRollTimeHoursProp =>
        List(KafkaConfig.LogRollTimeMillisProp, KafkaConfig.LogRollTimeHoursProp)
      case KafkaConfig.LogRollTimeJitterMillisProp | KafkaConfig.LogRollTimeJitterHoursProp =>
        List(KafkaConfig.LogRollTimeJitterMillisProp, KafkaConfig.LogRollTimeJitterHoursProp)
      case KafkaConfig.LogFlushIntervalMsProp => // LogFlushSchedulerIntervalMsProp is used as default
        List(KafkaConfig.LogFlushIntervalMsProp, KafkaConfig.LogFlushSchedulerIntervalMsProp)
      case KafkaConfig.LogRetentionTimeMillisProp | KafkaConfig.LogRetentionTimeMinutesProp | KafkaConfig.LogRetentionTimeHoursProp =>
        List(KafkaConfig.LogRetentionTimeMillisProp, KafkaConfig.LogRetentionTimeMinutesProp, KafkaConfig.LogRetentionTimeHoursProp)
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

  private[server] def addDynamicConfigs(configDef: ConfigDef): Unit = {
    KafkaConfig.configKeys.forKeyValue { (configName, config) =>
      if (AllDynamicConfigs.contains(configName)) {
        configDef.define(config.name, config.`type`, config.defaultValue, config.validator,
          config.importance, config.documentation, config.group, config.orderInGroup, config.width,
          config.displayName, config.dependents, config.recommender)
      }
    }
  }

  private[server] def dynamicConfigUpdateModes: util.Map[String, String] = {
    AllDynamicConfigs.map { name =>
      val mode = if (PerBrokerConfigs.contains(name)) "per-broker" else "cluster-wide"
      name -> mode
    }.toMap.asJava
  }

  private[server] def resolveVariableConfigs(propsOriginal: Properties): Properties = {
    val props = new Properties
    val config = new AbstractConfig(new ConfigDef(), propsOriginal, false)
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
  private val reconfigurables = new CopyOnWriteArrayList[Reconfigurable]()
  private val brokerReconfigurables = new CopyOnWriteArrayList[BrokerReconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var currentConfig: KafkaConfig = _
  private val dynamicConfigPasswordEncoder = if (kafkaConfig.processRoles.isEmpty) {
    maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret)
  } else {
    Some(PasswordEncoder.noop())
  }

  private[server] def initialize(zkClientOpt: Option[KafkaZkClient]): Unit = {
    currentConfig = new KafkaConfig(kafkaConfig.props, false, None)

    zkClientOpt.foreach { zkClient =>
      val adminZkClient = new AdminZkClient(zkClient)
      updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default), false)
      val props = adminZkClient.fetchEntityConfig(ConfigType.Broker, kafkaConfig.brokerId.toString)
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
    addReconfigurable(new DynamicClientQuotaCallback(kafkaServer))

    addBrokerReconfigurable(new DynamicThreadPool(kafkaServer))
    addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager, kafkaServer))
    addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
    addBrokerReconfigurable(kafkaServer.socketServer)
    addBrokerReconfigurable(kafkaServer.logManager.producerStateManagerConfig)
  }

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
    reconfigurables.add(reconfigurable)
  }

  def addBrokerReconfigurable(reconfigurable: BrokerReconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs)
    brokerReconfigurables.add(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables.remove(reconfigurable)
  }

  private def verifyReconfigurableConfigs(configNames: Set[String]): Unit = CoreUtils.inWriteLock(lock) {
    val nonDynamic = configNames.filter(DynamicConfig.Broker.nonDynamicProps.contains)
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
    reconfigurables.asScala
      .filter(reconfigurable => ReloadableFileConfigs.exists(reconfigurable.reconfigurableConfigs.contains))
      .foreach {
        case reconfigurable: ListenerReconfigurable =>
          val kafkaProps = validatedKafkaProps(newProps, perBrokerConfig = true)
          val newConfig = new KafkaConfig(kafkaProps.asJava, false, None)
          processListenerReconfigurable(reconfigurable, newConfig, Collections.emptyMap(), validateOnly = false, reloadOnly = true)
        case reconfigurable =>
          trace(s"Files will not be reloaded without config change for $reconfigurable")
      }
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
    configProps.asScala.forKeyValue { (name, value) =>
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

    props.asScala.forKeyValue { (name, value) =>
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
        persistentProps.asScala.forKeyValue { (configName, value) =>
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
    propsOverride.forKeyValue { (k, v) =>
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
    val newConfig = new KafkaConfig(newProps.asJava, doLog, None)
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
  // Exclude message.format.version for now since we need to check that the version
  // is supported on all brokers in the cluster.
  @nowarn("cat=deprecation")
  val ExcludedConfigs = Set(KafkaConfig.LogMessageFormatVersionProp)

  val ReconfigurableConfigs = LogConfig.TopicConfigSynonyms.values.toSet -- ExcludedConfigs
  val KafkaConfigToLogConfigName = LogConfig.TopicConfigSynonyms.map { case (k, v) => (v, k) }
}

class DynamicLogConfig(logManager: LogManager, server: KafkaBroker) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicLogConfig.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    // For update of topic config overrides, only config names and types are validated
    // Names and types have already been validated. For consistency with topic config
    // validation, no additional validation is performed.
  }

  private def updateLogsConfig(newBrokerDefaults: Map[String, Object]): Unit = {
    logManager.brokerConfigUpdated()
    logManager.allLogs.foreach { log =>
      val props = mutable.Map.empty[Any, Any]
      props ++= newBrokerDefaults
      props ++= log.config.originals.asScala.filter { case (k, _) =>
        log.config.overriddenConfigs.contains(k)
      }

      val logConfig = LogConfig(props.asJava, log.config.overriddenConfigs)
      log.updateConfig(logConfig)
    }
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val originalLogConfig = logManager.currentDefaultConfig
    val originalUncleanLeaderElectionEnable = originalLogConfig.uncleanLeaderElectionEnable
    val newBrokerDefaults = new util.HashMap[String, Object](originalLogConfig.originals)
    newConfig.valuesFromThisConfig.forEach { (k, v) =>
      if (DynamicLogConfig.ReconfigurableConfigs.contains(k)) {
        DynamicLogConfig.KafkaConfigToLogConfigName.get(k).foreach { configName =>
          if (v == null)
             newBrokerDefaults.remove(configName)
          else
            newBrokerDefaults.put(configName, v.asInstanceOf[AnyRef])
        }
      }
    }

    logManager.reconfigureDefaultLogConfig(LogConfig(newBrokerDefaults))

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
    KafkaConfig.NumIoThreadsProp,
    KafkaConfig.NumReplicaFetchersProp,
    KafkaConfig.NumRecoveryThreadsPerDataDirProp,
    KafkaConfig.BackgroundThreadsProp)
}

class DynamicThreadPool(server: KafkaBroker) extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicThreadPool.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    newConfig.values.forEach { (k, v) =>
      if (DynamicThreadPool.ReconfigurableConfigs.contains(k)) {
        val newValue = v.asInstanceOf[Int]
        val oldValue = currentValue(k)
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
    if (newConfig.numIoThreads != oldConfig.numIoThreads)
      server.dataPlaneRequestHandlerPool.resizeThreadPool(newConfig.numIoThreads)
    if (newConfig.numReplicaFetchers != oldConfig.numReplicaFetchers)
      server.replicaManager.resizeFetcherThreadPool(newConfig.numReplicaFetchers)
    if (newConfig.numRecoveryThreadsPerDataDir != oldConfig.numRecoveryThreadsPerDataDir)
      server.logManager.resizeRecoveryThreadPool(newConfig.numRecoveryThreadsPerDataDir)
    if (newConfig.backgroundThreads != oldConfig.backgroundThreads)
      server.kafkaScheduler.resizeThreadPool(newConfig.backgroundThreads)
  }

  private def currentValue(name: String): Int = {
    name match {
      case KafkaConfig.NumIoThreadsProp => server.config.numIoThreads
      case KafkaConfig.NumReplicaFetchersProp => server.config.numReplicaFetchers
      case KafkaConfig.NumRecoveryThreadsPerDataDirProp => server.config.numRecoveryThreadsPerDataDir
      case KafkaConfig.BackgroundThreadsProp => server.config.backgroundThreads
      case n => throw new IllegalStateException(s"Unexpected config $n")
    }
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
    configs.add(KafkaConfig.MetricReporterClassesProp)
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
  private val propsOverride = Map[String, AnyRef](KafkaConfig.BrokerIdProp -> brokerId.toString)
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
    propsOverride.forKeyValue((k, v) => props.put(k, v))
    val reporters = dynamicConfig.currentKafkaConfig.getConfiguredInstances(reporterClasses, classOf[MetricsReporter], props)

    // Call notifyMetricsReporters first to satisfy the contract for MetricsReporter.contextChange,
    // which provides that MetricsReporter.contextChange must be called before the first call to MetricsReporter.init.
    // The first call to MetricsReporter.init is done when we call metrics.addReporter below.
    KafkaBroker.notifyMetricsReporters(clusterId, config, reporters.asScala)
    reporters.forEach { reporter =>
      metrics.addReporter(reporter)
      currentReporters += reporter.getClass.getName -> reporter
    }
    KafkaBroker.notifyClusterListeners(clusterId, reporters.asScala)
  }

  private[server] def removeReporter(className: String): Unit = {
    currentReporters.remove(className).foreach(metrics.removeReporter)
  }

  @nowarn("cat=deprecation")
  private[server] def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] = {
    val reporters = mutable.Buffer[String]()
    reporters ++= configs.get(KafkaConfig.MetricReporterClassesProp).asInstanceOf[util.List[String]].asScala
    if (configs.get(KafkaConfig.AutoIncludeJmxReporterProp).asInstanceOf[Boolean] &&
        !reporters.contains(classOf[JmxReporter].getName)) {
      reporters += classOf[JmxReporter].getName
    }
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
    KafkaConfig.AdvertisedListenersProp,
    KafkaConfig.ListenersProp,
    KafkaConfig.ListenerSecurityProtocolMapProp,

    // SSL configs
    KafkaConfig.PrincipalBuilderClassProp,
    KafkaConfig.SslProtocolProp,
    KafkaConfig.SslProviderProp,
    KafkaConfig.SslCipherSuitesProp,
    KafkaConfig.SslEnabledProtocolsProp,
    KafkaConfig.SslKeystoreTypeProp,
    KafkaConfig.SslKeystoreLocationProp,
    KafkaConfig.SslKeystorePasswordProp,
    KafkaConfig.SslKeyPasswordProp,
    KafkaConfig.SslTruststoreTypeProp,
    KafkaConfig.SslTruststoreLocationProp,
    KafkaConfig.SslTruststorePasswordProp,
    KafkaConfig.SslKeyManagerAlgorithmProp,
    KafkaConfig.SslTrustManagerAlgorithmProp,
    KafkaConfig.SslEndpointIdentificationAlgorithmProp,
    KafkaConfig.SslSecureRandomImplementationProp,
    KafkaConfig.SslClientAuthProp,
    KafkaConfig.SslEngineFactoryClassProp,

    // SASL configs
    KafkaConfig.SaslMechanismInterBrokerProtocolProp,
    KafkaConfig.SaslJaasConfigProp,
    KafkaConfig.SaslEnabledMechanismsProp,
    KafkaConfig.SaslKerberosServiceNameProp,
    KafkaConfig.SaslKerberosKinitCmdProp,
    KafkaConfig.SaslKerberosTicketRenewWindowFactorProp,
    KafkaConfig.SaslKerberosTicketRenewJitterProp,
    KafkaConfig.SaslKerberosMinTimeBeforeReloginProp,
    KafkaConfig.SaslKerberosPrincipalToLocalRulesProp,
    KafkaConfig.SaslLoginRefreshWindowFactorProp,
    KafkaConfig.SaslLoginRefreshWindowJitterProp,
    KafkaConfig.SaslLoginRefreshMinPeriodSecondsProp,
    KafkaConfig.SaslLoginRefreshBufferSecondsProp,

    // Connection limit configs
    KafkaConfig.MaxConnectionsProp,
    KafkaConfig.MaxConnectionCreationRateProp,

    // Network threads
    KafkaConfig.NumNetworkThreadsProp
  )
}

class DynamicClientQuotaCallback(server: KafkaBroker) extends Reconfigurable {

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    val configs = new util.HashSet[String]()
    server.quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable => configs.addAll(callback.reconfigurableConfigs)
      case _ =>
    }
    configs
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    server.quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable => callback.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    val config = server.config
    server.quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable =>
        config.dynamicConfig.maybeReconfigure(callback, config.dynamicConfig.currentKafkaConfig, configs)
        true
      case _ => false
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
    oldAdvertisedListeners.forKeyValue {
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
    val newAdvertisedListeners = listenersToMap(newConfig.effectiveAdvertisedListeners)
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
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedListeners),
        listenersToMap(newConfig.effectiveAdvertisedListeners))) {
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
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedListeners),
        listenersToMap(newConfig.effectiveAdvertisedListeners))) {
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
