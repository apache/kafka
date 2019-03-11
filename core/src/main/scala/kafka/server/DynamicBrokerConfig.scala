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
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.cluster.EndPoint
import kafka.log.{LogCleaner, LogConfig, LogManager}
import kafka.server.DynamicBrokerConfig._
import kafka.utils.{CoreUtils, Logging, PasswordEncoder}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{ConfigDef, ConfigException, SslConfigs}
import org.apache.kafka.common.metrics.MetricsReporter
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.{ListenerName, ListenerReconfigurable}
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.utils.Utils

import scala.collection._
import scala.collection.JavaConverters._

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
    DynamicConnectionQuota.ReconfigurableConfigs

  private val PerBrokerConfigs = DynamicSecurityConfigs  ++
    DynamicListenerConfig.ReconfigurableConfigs
  private val ListenerMechanismConfigs = Set(KafkaConfig.SaslJaasConfigProp)

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

  def validateConfigs(props: Properties, perBrokerConfig: Boolean): Unit =  {
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
    configNames.intersect(PerBrokerConfigs) ++ configNames.filter(ListenerConfigRegex.findFirstIn(_).nonEmpty)
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
    KafkaConfig.configKeys.filterKeys(AllDynamicConfigs.contains).values.foreach { config =>
      configDef.define(config.name, config.`type`, config.defaultValue, config.validator,
        config.importance, config.documentation, config.group, config.orderInGroup, config.width,
        config.displayName, config.dependents, config.recommender)
    }
  }

  private[server] def dynamicConfigUpdateModes: util.Map[String, String] = {
    AllDynamicConfigs.map { name =>
      val mode = if (PerBrokerConfigs.contains(name)) "per-broker" else "cluster-wide"
      (name -> mode)
    }.toMap.asJava
  }
}

class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging {

  private[server] val staticBrokerConfigs = ConfigDef.convertToStringMapWithPasswordValues(kafkaConfig.originalsFromThisConfig).asScala
  private[server] val staticDefaultConfigs = ConfigDef.convertToStringMapWithPasswordValues(KafkaConfig.defaultValues.asJava).asScala
  private val dynamicBrokerConfigs = mutable.Map[String, String]()
  private val dynamicDefaultConfigs = mutable.Map[String, String]()
  private val reconfigurables = mutable.Buffer[Reconfigurable]()
  private val brokerReconfigurables = mutable.Buffer[BrokerReconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var currentConfig = kafkaConfig
  private val dynamicConfigPasswordEncoder = maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret)

  private[server] def initialize(zkClient: KafkaZkClient): Unit = {
    currentConfig = new KafkaConfig(kafkaConfig.props, false, None)
    val adminZkClient = new AdminZkClient(zkClient)
    updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default))
    val props = adminZkClient.fetchEntityConfig(ConfigType.Broker, kafkaConfig.brokerId.toString)
    val brokerConfig = maybeReEncodePasswords(props, adminZkClient)
    updateBrokerConfig(kafkaConfig.brokerId, brokerConfig)
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
  def addReconfigurables(kafkaServer: KafkaServer): Unit = {
    addReconfigurable(new DynamicMetricsReporters(kafkaConfig.brokerId, kafkaServer))
    addReconfigurable(new DynamicClientQuotaCallback(kafkaConfig.brokerId, kafkaServer))

    addBrokerReconfigurable(new DynamicThreadPool(kafkaServer))
    if (kafkaServer.logManager.cleaner != null)
      addBrokerReconfigurable(kafkaServer.logManager.cleaner)
    addBrokerReconfigurable(new DynamicLogConfig(kafkaServer.logManager, kafkaServer))
    addBrokerReconfigurable(new DynamicListenerConfig(kafkaServer))
    addBrokerReconfigurable(new DynamicConnectionQuota(kafkaServer))
  }

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
    reconfigurables += reconfigurable
  }

  def addBrokerReconfigurable(reconfigurable: BrokerReconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs)
    brokerReconfigurables += reconfigurable
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables -= reconfigurable
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

  private[server] def updateBrokerConfig(brokerId: Int, persistentProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    try {
      val props = fromPersistentProps(persistentProps, perBrokerConfig = true)
      dynamicBrokerConfigs.clear()
      dynamicBrokerConfigs ++= props.asScala
      updateCurrentConfig()
    } catch {
      case e: Exception => error(s"Per-broker configs of $brokerId could not be applied: $persistentProps", e)
    }
  }

  private[server] def updateDefaultConfig(persistentProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    try {
      val props = fromPersistentProps(persistentProps, perBrokerConfig = false)
      dynamicDefaultConfigs.clear()
      dynamicDefaultConfigs ++= props.asScala
      updateCurrentConfig()
    } catch {
      case e: Exception => error(s"Cluster default configs could not be applied: $persistentProps", e)
    }
  }

  /**
   * All config updates through ZooKeeper are triggered through actual changes in values stored in ZooKeeper.
   * For some configs like SSL keystores and truststores, we also want to reload the store if it was modified
   * in-place, even though the actual value of the file path and password haven't changed. This scenario alone
   * is handled here when a config update request using admin client is processed by AdminManager. If any of
   * the SSL configs have changed, then the update will not be done here, but will be handled later when ZK
   * changes are processed. At the moment, only listener configs are considered for reloading.
   */
  private[server] def reloadUpdatedFilesWithoutConfigChange(newProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables
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
      new PasswordEncoder(secret,
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
    configProps.asScala.filterKeys(isPasswordConfig).foreach { case (name, value) => encodePassword(name, value) }
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

    props.asScala.filterKeys(isPasswordConfig).foreach { case (name, value) => decodePassword(name, value) }
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
        persistentProps.asScala.filterKeys(isPasswordConfig).foreach { case (configName, value) =>
          if (value != null) {
            val decoded = try {
              Some(passwordDecoder.decode(value).value)
            } catch {
              case _: Exception =>
                debug(s"Dynamic password config $configName could not be decoded using old secret, new secret will be used.")
                None
            }
            decoded.foreach { value => props.put(configName, passwordEncoder.encode(new Password(value))) }
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
    validateConfigs(propsOverride, perBrokerConfig)
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    if (perBrokerConfig) {
      overrideProps(newProps, dynamicDefaultConfigs)
      overrideProps(newProps, propsOverride.asScala)
    } else {
      overrideProps(newProps, propsOverride.asScala)
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
        invalidProps.foreach(props.remove)
        val configSource = if (perBrokerConfig) "broker" else "default cluster"
        error(s"Dynamic $configSource config contains invalid values: $invalidProps, these configs will be ignored", e)
    }
  }

  private[server] def maybeReconfigure(reconfigurable: Reconfigurable, oldConfig: KafkaConfig, newConfig: util.Map[String, _]): Unit = {
    if (reconfigurable.reconfigurableConfigs.asScala.exists(key => oldConfig.originals.get(key) != newConfig.get(key)))
      reconfigurable.reconfigure(newConfig)
  }

  private def updatedConfigs(newProps: java.util.Map[String, _], currentProps: java.util.Map[_, _]): mutable.Map[String, _] = {
    newProps.asScala.filter {
      case (k, v) => v != currentProps.get(k)
    }
  }

  /**
    * Updates values in `props` with the new values from `propsOverride`. Synonyms of updated configs
    * are removed from `props` to ensure that the config with the higher precedence is applied. For example,
    * if `log.roll.ms` was defined in server.properties and `log.roll.hours` is configured dynamically,
    * `log.roll.hours` from the dynamic configuration will be used and `log.roll.ms` will be removed from
    * `props` (even though `log.roll.hours` is secondary to `log.roll.ms`).
    */
  private def overrideProps(props: mutable.Map[String, String], propsOverride: mutable.Map[String, String]): Unit = {
    propsOverride.foreach { case (k, v) =>
      // Remove synonyms of `k` to ensure the right precedence is applied. But disable `matchListenerOverride`
      // so that base configs corresponding to listener configs are not removed. Base configs should not be removed
      // since they may be used by other listeners. It is ok to retain them in `props` since base configs cannot be
      // dynamically updated and listener-specific configs have the higher precedence.
      brokerConfigSynonyms(k, matchListenerOverride = false).foreach(props.remove)
      props.put(k, v)
    }
  }

  private def updateCurrentConfig(): Unit = {
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    overrideProps(newProps, dynamicDefaultConfigs)
    overrideProps(newProps, dynamicBrokerConfigs)
    val oldConfig = currentConfig
    val (newConfig, brokerReconfigurablesToUpdate) = processReconfiguration(newProps, validateOnly = false)
    if (newConfig ne currentConfig) {
      currentConfig = newConfig
      kafkaConfig.updateCurrentConfig(newConfig)

      // Process BrokerReconfigurable updates after current config is updated
      brokerReconfigurablesToUpdate.foreach(_.reconfigure(oldConfig, newConfig))
    }
  }

  private def processReconfiguration(newProps: Map[String, String], validateOnly: Boolean): (KafkaConfig, List[BrokerReconfigurable]) = {
    val newConfig = new KafkaConfig(newProps.asJava, !validateOnly, None)
    val updatedMap = updatedConfigs(newConfig.originalsFromThisConfig, currentConfig.originals)
    if (updatedMap.nonEmpty) {
      try {
        val customConfigs = new util.HashMap[String, Object](newConfig.originalsFromThisConfig) // non-Kafka configs
        newConfig.valuesFromThisConfig.keySet.asScala.foreach(customConfigs.remove)
        reconfigurables.foreach {
          case listenerReconfigurable: ListenerReconfigurable =>
            processListenerReconfigurable(listenerReconfigurable, newConfig, customConfigs, validateOnly, reloadOnly = false)
          case reconfigurable =>
            if (needsReconfiguration(reconfigurable.reconfigurableConfigs, updatedMap.keySet))
              processReconfigurable(reconfigurable, updatedMap.keySet, newConfig.valuesFromThisConfig, customConfigs, validateOnly)
        }

        // BrokerReconfigurable updates are processed after config is updated. Only do the validation here.
        val brokerReconfigurablesToUpdate = mutable.Buffer[BrokerReconfigurable]()
        brokerReconfigurables.foreach { reconfigurable =>
          if (needsReconfiguration(reconfigurable.reconfigurableConfigs.asJava, updatedMap.keySet)) {
            reconfigurable.validateReconfiguration(newConfig)
            if (!validateOnly)
              brokerReconfigurablesToUpdate += reconfigurable
          }
        }
        (newConfig, brokerReconfigurablesToUpdate.toList)
      } catch {
        case e: Exception =>
          if (!validateOnly)
            error(s"Failed to update broker configuration with configs : ${newConfig.originalsFromThisConfig}", e)
          throw new ConfigException("Invalid dynamic configuration", e)
      }
    }
    else
      (currentConfig, List.empty)
  }

  private def needsReconfiguration(reconfigurableConfigs: util.Set[String], updatedKeys: Set[String]): Boolean = {
    reconfigurableConfigs.asScala.intersect(updatedKeys).nonEmpty
  }

  private def processListenerReconfigurable(listenerReconfigurable: ListenerReconfigurable,
                                            newConfig: KafkaConfig,
                                            customConfigs: util.Map[String, Object],
                                            validateOnly: Boolean,
                                            reloadOnly:  Boolean): Unit = {
    val listenerName = listenerReconfigurable.listenerName
    val oldValues = currentConfig.valuesWithPrefixOverride(listenerName.configPrefix)
    val newValues = newConfig.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
    val updatedKeys = updatedConfigs(newValues, oldValues).keySet
    val configsChanged = needsReconfiguration(listenerReconfigurable.reconfigurableConfigs, updatedKeys)
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
    allNewConfigs.asScala.foreach { case (k, v) => newConfigs.put(k, v.asInstanceOf[AnyRef]) }
    newConfigs.putAll(newCustomConfigs)
    try {
      reconfigurable.validateReconfiguration(newConfigs)
    } catch {
      case e: ConfigException => throw e
      case _: Exception =>
        throw new ConfigException(s"Validation of dynamic config update of $updatedConfigNames failed with class ${reconfigurable.getClass}")
    }

    if (!validateOnly) {
      info(s"Reconfiguring $reconfigurable, updated configs: $updatedConfigNames custom configs: $newCustomConfigs")
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
  val ExcludedConfigs = Set(KafkaConfig.LogMessageFormatVersionProp)

  val ReconfigurableConfigs = LogConfig.TopicConfigSynonyms.values.toSet -- ExcludedConfigs
  val KafkaConfigToLogConfigName = LogConfig.TopicConfigSynonyms.map { case (k, v) => (v, k) }
}

class DynamicLogConfig(logManager: LogManager, server: KafkaServer) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicLogConfig.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    // For update of topic config overrides, only config names and types are validated
    // Names and types have already been validated. For consistency with topic config
    // validation, no additional validation is performed.
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val currentLogConfig = logManager.currentDefaultConfig
    val origUncleanLeaderElectionEnable = logManager.currentDefaultConfig.uncleanLeaderElectionEnable
    val newBrokerDefaults = new util.HashMap[String, Object](currentLogConfig.originals)
    newConfig.valuesFromThisConfig.asScala.filterKeys(DynamicLogConfig.ReconfigurableConfigs.contains).foreach { case (k, v) =>
      if (v != null) {
        DynamicLogConfig.KafkaConfigToLogConfigName.get(k).foreach { configName =>
          newBrokerDefaults.put(configName, v.asInstanceOf[AnyRef])
        }
      }
    }

    logManager.reconfigureDefaultLogConfig(LogConfig(newBrokerDefaults))

    logManager.allLogs.foreach { log =>
      val props = mutable.Map.empty[Any, Any]
      props ++= newBrokerDefaults.asScala
      props ++= log.config.originals.asScala.filterKeys(log.config.overriddenConfigs.contains)

      val logConfig = LogConfig(props.asJava)
      log.updateConfig(newBrokerDefaults.asScala.keySet, logConfig)
    }
    if (logManager.currentDefaultConfig.uncleanLeaderElectionEnable && !origUncleanLeaderElectionEnable) {
      server.kafkaController.enableDefaultUncleanLeaderElection()
    }
  }
}

object DynamicThreadPool {
  val ReconfigurableConfigs = Set(
    KafkaConfig.NumIoThreadsProp,
    KafkaConfig.NumNetworkThreadsProp,
    KafkaConfig.NumReplicaFetchersProp,
    KafkaConfig.NumRecoveryThreadsPerDataDirProp,
    KafkaConfig.BackgroundThreadsProp)
}

class DynamicThreadPool(server: KafkaServer) extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicThreadPool.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    newConfig.values.asScala.filterKeys(DynamicThreadPool.ReconfigurableConfigs.contains).foreach { case (k, v) =>
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

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (newConfig.numIoThreads != oldConfig.numIoThreads)
      server.dataPlaneRequestHandlerPool.resizeThreadPool(newConfig.numIoThreads)
    if (newConfig.numNetworkThreads != oldConfig.numNetworkThreads)
      server.socketServer.resizeThreadPool(oldConfig.numNetworkThreads, newConfig.numNetworkThreads)
    if (newConfig.numReplicaFetchers != oldConfig.numReplicaFetchers)
      server.replicaManager.replicaFetcherManager.resizeThreadPool(newConfig.numReplicaFetchers)
    if (newConfig.numRecoveryThreadsPerDataDir != oldConfig.numRecoveryThreadsPerDataDir)
      server.getLogManager.resizeRecoveryThreadPool(newConfig.numRecoveryThreadsPerDataDir)
    if (newConfig.backgroundThreads != oldConfig.backgroundThreads)
      server.kafkaScheduler.resizeThreadPool(newConfig.backgroundThreads)
  }

  private def currentValue(name: String): Int = {
    name match {
      case KafkaConfig.NumIoThreadsProp => server.config.numIoThreads
      case KafkaConfig.NumNetworkThreadsProp => server.config.numNetworkThreads
      case KafkaConfig.NumReplicaFetchersProp => server.config.numReplicaFetchers
      case KafkaConfig.NumRecoveryThreadsPerDataDirProp => server.config.numRecoveryThreadsPerDataDir
      case KafkaConfig.BackgroundThreadsProp => server.config.backgroundThreads
      case n => throw new IllegalStateException(s"Unexpected config $n")
    }
  }
}

class DynamicMetricsReporters(brokerId: Int, server: KafkaServer) extends Reconfigurable {

  private val dynamicConfig = server.config.dynamicConfig
  private val metrics = server.metrics
  private val propsOverride = Map[String, AnyRef](KafkaConfig.BrokerIdProp -> brokerId.toString)
  private val currentReporters = mutable.Map[String, MetricsReporter]()

  createReporters(dynamicConfig.currentKafkaConfig.getList(KafkaConfig.MetricReporterClassesProp),
    Collections.emptyMap[String, Object])

  private[server] def currentMetricsReporters: List[MetricsReporter] = currentReporters.values.toList

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
    val deleted = currentReporters.keySet -- updatedMetricsReporters
    deleted.foreach(removeReporter)
    currentReporters.values.foreach {
      case reporter: Reconfigurable => dynamicConfig.maybeReconfigure(reporter, dynamicConfig.currentKafkaConfig, configs)
      case _ =>
    }
    val added = updatedMetricsReporters -- currentReporters.keySet
    createReporters(added.asJava, configs)
  }

  private def createReporters(reporterClasses: util.List[String],
                              updatedConfigs: util.Map[String, _]): Unit = {
    val props = new util.HashMap[String, AnyRef]
    updatedConfigs.asScala.foreach { case (k, v) => props.put(k, v.asInstanceOf[AnyRef]) }
    propsOverride.foreach { case (k, v) => props.put(k, v) }
    val reporters = dynamicConfig.currentKafkaConfig.getConfiguredInstances(reporterClasses, classOf[MetricsReporter], props)
    reporters.asScala.foreach { reporter =>
      metrics.addReporter(reporter)
      currentReporters += reporter.getClass.getName -> reporter
    }
    server.notifyClusterListeners(reporters.asScala)
  }

  private def removeReporter(className: String): Unit = {
    currentReporters.remove(className).foreach(metrics.removeReporter)
  }

  private def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] = {
    configs.get(KafkaConfig.MetricReporterClassesProp).asInstanceOf[util.List[String]].asScala
  }
}
object DynamicListenerConfig {

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
    KafkaConfig.SaslLoginRefreshBufferSecondsProp
  )
}

class DynamicClientQuotaCallback(brokerId: Int, server: KafkaServer) extends Reconfigurable {

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

class DynamicListenerConfig(server: KafkaServer) extends BrokerReconfigurable with Logging {

  override def reconfigurableConfigs: Set[String] = {
    DynamicListenerConfig.ReconfigurableConfigs
  }

  def validateReconfiguration(newConfig: KafkaConfig): Unit = {

    def immutableListenerConfigs(kafkaConfig: KafkaConfig, prefix: String): Map[String, AnyRef] = {
      newConfig.originals.asScala
        .filterKeys(_.startsWith(prefix))
        .filterKeys(k => !DynamicSecurityConfigs.contains(k))
    }

    val oldConfig = server.config
    val newListeners = listenersToMap(newConfig.listeners)
    val newAdvertisedListeners = listenersToMap(newConfig.advertisedListeners)
    val oldListeners = listenersToMap(oldConfig.listeners)
    if (!newAdvertisedListeners.keySet.subsetOf(newListeners.keySet))
      throw new ConfigException(s"Advertised listeners '$newAdvertisedListeners' must be a subset of listeners '$newListeners'")
    if (!newListeners.keySet.subsetOf(newConfig.listenerSecurityProtocolMap.keySet))
      throw new ConfigException(s"Listeners '$newListeners' must be subset of listener map '${newConfig.listenerSecurityProtocolMap}'")
    newListeners.keySet.intersect(oldListeners.keySet).foreach { listenerName =>
      val prefix = listenerName.configPrefix
      val newListenerProps = immutableListenerConfigs(newConfig, prefix)
      val oldListenerProps = immutableListenerConfigs(oldConfig, prefix)
      if (newListenerProps != oldListenerProps)
        throw new ConfigException(s"Configs cannot be updated dynamically for existing listener $listenerName, " +
          "restart broker or create a new listener for update")
      if (oldConfig.listenerSecurityProtocolMap(listenerName) != newConfig.listenerSecurityProtocolMap(listenerName))
        throw new ConfigException(s"Security protocol cannot be updated for existing listener $listenerName")
    }
    if (!newAdvertisedListeners.contains(newConfig.interBrokerListenerName))
      throw new ConfigException(s"Advertised listener must be specified for inter-broker listener ${newConfig.interBrokerListenerName}")
  }

  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val newListeners = newConfig.listeners
    val newListenerMap = listenersToMap(newListeners)
    val oldListeners = oldConfig.listeners
    val oldListenerMap = listenersToMap(oldListeners)
    val listenersRemoved = oldListeners.filterNot(e => newListenerMap.contains(e.listenerName))
    val listenersAdded = newListeners.filterNot(e => oldListenerMap.contains(e.listenerName))

    // Clear SASL login cache to force re-login
    if (listenersAdded.nonEmpty || listenersRemoved.nonEmpty)
      LoginManager.closeAll()

    server.socketServer.removeListeners(listenersRemoved)
    if (listenersAdded.nonEmpty)
      server.socketServer.addListeners(listenersAdded)

    server.kafkaController.updateBrokerInfo(server.createBrokerInfo)
  }

  private def listenersToMap(listeners: Seq[EndPoint]): Map[ListenerName, EndPoint] =
    listeners.map(e => (e.listenerName, e)).toMap

}

object DynamicConnectionQuota {
  val ReconfigurableConfigs = Set(KafkaConfig.MaxConnectionsPerIpProp, KafkaConfig.MaxConnectionsPerIpOverridesProp)
}

class DynamicConnectionQuota(server: KafkaServer) extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicConnectionQuota.ReconfigurableConfigs
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    server.socketServer.updateMaxConnectionsPerIpOverride(newConfig.maxConnectionsPerIpOverrides)

    if (newConfig.maxConnectionsPerIp != oldConfig.maxConnectionsPerIp)
      server.socketServer.updateMaxConnectionsPerIp(newConfig.maxConnectionsPerIp)
  }
}

