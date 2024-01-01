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

import java.{lang, util}
import java.util.{Collections, Optional, Properties}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.cluster.EndPoint
import kafka.log.LogManager
import kafka.network.DataPlaneAcceptor
import kafka.utils.{CoreUtils, Logging}
import kafka.utils.Implicits._
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{ConfigDef, ConfigException}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, MetricsReporter}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.network.{ListenerName, ListenerReconfigurable}
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.utils.{ConfigUtils, Utils}
import org.apache.kafka.network.SocketServer
import org.apache.kafka.server.config.{ConfigType, DynamicBrokerConfigBaseManager}
import org.apache.kafka.server.config.DynamicBrokerConfigManager.{BrokerReconfigurable, JDynamicListenerConfig, JDynamicLogConfig, JDynamicRemoteLogConfig, JDynamicThreadPool}
import org.apache.kafka.server.config.DynamicBrokerConfigBaseManager.{RECONFIGURABLE_CONFIGS, RELOADABLE_FILE_CONFIGS, brokerConfigSynonyms, isPasswordConfig, nonDynamicConfigs, perBrokerConfigs, securityConfigsWithoutListenerPrefix, validateConfigTypes, validateConfigs}
import org.apache.kafka.server.ReconfigurableServer
import org.apache.kafka.server.KafkaRaftServer.ProcessRole
import org.apache.kafka.server.config.KafkaConfig
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.ClientMetricsReceiverPlugin
import org.apache.kafka.server.telemetry.ClientTelemetry
import org.apache.kafka.storage.internals.log.{LogConfig, ProducerStateManagerConfig}
import org.apache.kafka.utils.PasswordEncoder
import org.apache.kafka.zk.KafkaZKClient

import scala.annotation.nowarn
import scala.collection._
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends DynamicBrokerConfigBaseManager with Logging {
  override def staticBrokerConfigs() = ConfigDef.convertToStringMapWithPasswordValues(kafkaConfig.originalsFromThisConfig)
  override def staticDefaultConfigs() = ConfigDef.convertToStringMapWithPasswordValues(KafkaConfig.defaultValues)
  private val dynamicBrokerConfigs = mutable.Map[String, String]()
  private val dynamicDefaultConfigs = mutable.Map[String, String]()

  // Use COWArrayList to prevent concurrent modification exception when an item is added by one thread to these
  // collections, while another thread is iterating over them.
  private val brokerReconfigurables = new CopyOnWriteArrayList[BrokerReconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var metricsReceiverPluginOpt: Option[ClientMetricsReceiverPlugin] = _
  private var currentConfig: KafkaConfig = _
  private val dynamicConfigPasswordEncoder = if (kafkaConfig.processRoles.isEmpty()) {
    maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderSecret.asScala)
  } else {
    Some(PasswordEncoder.NO_OP_PASSWORD_ENCODER)
  }

  override def initialize(zkClientOpt: Optional[KafkaZKClient], clientMetricsReceiverPluginOpt: Optional[ClientMetricsReceiverPlugin]): Unit = {
    currentConfig = KafkaConfigProvider.fromProps(kafkaConfig.props, false)
    metricsReceiverPluginOpt = clientMetricsReceiverPluginOpt.asScala

    zkClientOpt.asScala.foreach {
      case zkClient: KafkaZkClient =>
        val adminZkClient = new AdminZkClient(zkClient)
        updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.BROKER, ConfigEntityName.Default), false)
        val props = adminZkClient.fetchEntityConfig(ConfigType.BROKER, kafkaConfig.brokerId.toString)
        val brokerConfig = maybeReEncodePasswords(props, adminZkClient)
        updateBrokerConfig(kafkaConfig.brokerId, brokerConfig)
      case c =>
        logger.error(s"Unknown zk client ${c.name()}")
    }
  }

  /**
   * Clear all cached values. This is used to clear state on broker shutdown to avoid
   * exceptions in tests when broker is restarted. These fields are re-initialized when
   * broker starts up.
   */
  override def clear(): Unit = {
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

  override def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
    reconfigurables.add(reconfigurable)
  }

  override def addBrokerReconfigurable(reconfigurable: BrokerReconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    verifyReconfigurableConfigs(reconfigurable.reconfigurableConfigs.asScala)
    brokerReconfigurables.add(reconfigurable)
  }

  override def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables.remove(reconfigurable)
  }

  private def verifyReconfigurableConfigs(configNames: Set[String]): Unit = CoreUtils.inWriteLock(lock) {
    val nonDynamic = configNames.filter(DynamicConfig.Broker.nonDynamicProps.contains)
    require(nonDynamic.isEmpty, s"Reconfigurable contains non-dynamic configs $nonDynamic")
  }

  // Visibility for testing
  override def currentKafkaConfig: KafkaConfig = CoreUtils.inReadLock(lock) {
    currentConfig
  }

  override def currentDynamicBrokerConfigs: util.Map[String, String] = CoreUtils.inReadLock(lock) {
    dynamicBrokerConfigs.clone().asJava
  }

  override def currentDynamicDefaultConfigs: util.Map[String, String] = CoreUtils.inReadLock(lock) {
    dynamicDefaultConfigs.clone().asJava
  }

  override def clientMetricsReceiverPlugin(): Optional[ClientMetricsReceiverPlugin] = CoreUtils.inReadLock(lock) {
    metricsReceiverPluginOpt.asJava
  }

  override def updateBrokerConfig(brokerId: Int, persistentProps: Properties, doLog: lang.Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
    try {
      val props = fromPersistentProps(persistentProps, perBrokerConfig = true)
      dynamicBrokerConfigs.clear()
      dynamicBrokerConfigs ++= props.asScala
      updateCurrentConfig(doLog)
    } catch {
      case e: Exception => error(s"Per-broker configs of $brokerId could not be applied: ${persistentProps.keys()}", e)
    }
  }

  override def updateDefaultConfig(persistentProps: Properties, doLog: lang.Boolean = true): Unit = CoreUtils.inWriteLock(lock) {
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
  override def reloadUpdatedFilesWithoutConfigChange(newProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables.asScala
      .filter(reconfigurable => RELOADABLE_FILE_CONFIGS.asScala.exists(reconfigurable.reconfigurableConfigs.contains))
      .foreach {
        case reconfigurable: ListenerReconfigurable =>
          val kafkaProps = validatedKafkaProps(newProps, perBrokerConfig = true)
          val newConfig = KafkaConfigProvider.fromProps(kafkaProps.asJava, false)
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

  override def toPersistentProps(configProps: Properties, perBrokerConfig: lang.Boolean): Properties = {
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

  override def fromPersistentProps(persistentProps: Properties,
                                          perBrokerConfig: lang.Boolean): Properties = {
    val props = persistentProps.clone().asInstanceOf[Properties]

    // Remove all invalid configs from `props`
    removeInvalidConfigs(props, perBrokerConfig)
    def removeInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
      if (invalidPropNames.nonEmpty) {
        invalidPropNames.foreach(props.remove)
        error(s"$errorMessage: $invalidPropNames")
      }
    }
    removeInvalidProps(nonDynamicConfigs(props).asScala, "Non-dynamic configs configured in ZooKeeper will be ignored")
    removeInvalidProps(securityConfigsWithoutListenerPrefix(props).asScala,
      "Security configs can be dynamically updated only using listener prefix, base configs will be ignored")
    if (!perBrokerConfig)
      removeInvalidProps(perBrokerConfigs(props).asScala, "Per-broker configs defined at default cluster level will be ignored")

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
      maybeCreatePasswordEncoder(kafkaConfig.passwordEncoderOldSecret.asScala).foreach { passwordDecoder =>
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
    val propsResolved = DynamicBrokerConfigBaseManager.resolveVariableConfigs(propsOverride)
    validateConfigs(propsResolved, perBrokerConfig)
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs.asScala
    if (perBrokerConfig) {
      overrideProps(newProps, dynamicDefaultConfigs)
      overrideProps(newProps, propsResolved.asScala)
    } else {
      overrideProps(newProps, propsResolved.asScala)
      overrideProps(newProps, dynamicBrokerConfigs)
    }
    newProps
  }

  override def validate(props: Properties, perBrokerConfig: lang.Boolean): Unit = CoreUtils.inReadLock(lock) {
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

  override def maybeReconfigure(reconfigurable: Reconfigurable, oldConfig: KafkaConfig, newConfig: util.Map[String, _]): Unit = {
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
      brokerConfigSynonyms(k, false).asScala.foreach(props.remove)
      props.put(k, v)
    }
  }

  private def updateCurrentConfig(doLog: Boolean): Unit = {
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs.asScala
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
    val newConfig = KafkaConfigProvider.fromProps(newProps.asJava, doLog)
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
          if (needsReconfiguration(reconfigurable.reconfigurableConfigs, changeMap.keySet, deletedKeySet)) {
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
           s"custom configs: ${ConfigUtils.configMapToRedactedString(newCustomConfigs, KafkaConfig.configDef())}")
      reconfigurable.reconfigure(newConfigs)
    }
  }

  override def addReconfigurables(server: ReconfigurableServer): Unit = {
    server match {
      case kafkaBroker: KafkaBroker => addReconfigurables(kafkaBroker);
      case controllerServer: ControllerServer => addReconfigurables(controllerServer)
      case server =>
        logger.error(s"Can't reconfigure ${server.name()}")
    }
  }
}

class DynamicLogConfig(logManager: LogManager, server: KafkaBroker) extends JDynamicLogConfig with Logging {

  override def reconfigurableConfigs: util.Set[String] = {
    JDynamicLogConfig.getReconfigurableConfigs
  }

  def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    // For update of topic config overrides, only config names and types are validated
    // Names and types have already been validated. For consistency with topic config
    // validation, no additional validation is performed.

    def validateLogLocalRetentionMs(): Unit = {
      val logRetentionMs = newConfig.logRetentionTimeMillis
      val logLocalRetentionMs: java.lang.Long = newConfig.logLocalRetentionMs
      if (logRetentionMs != -1L && logLocalRetentionMs != -2L) {
        if (logLocalRetentionMs == -1L) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
            s"Value must not be -1 as ${KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP} value is set as $logRetentionMs.")
        }
        if (logLocalRetentionMs > logRetentionMs) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs,
            s"Value must not be more than ${KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP} property value: $logRetentionMs")
        }
      }
    }

    def validateLogLocalRetentionBytes(): Unit = {
      val logRetentionBytes = newConfig.logRetentionBytes
      val logLocalRetentionBytes: java.lang.Long = newConfig.logLocalRetentionBytes
      if (logRetentionBytes > -1 && logLocalRetentionBytes != -2) {
        if (logLocalRetentionBytes == -1) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
            s"Value must not be -1 as ${KafkaConfig.LOG_RETENTION_BYTES_PROP} value is set as $logRetentionBytes.")
        }
        if (logLocalRetentionBytes > logRetentionBytes) {
          throw new ConfigException(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes,
            s"Value must not be more than ${KafkaConfig.LOG_RETENTION_BYTES_PROP} property value: $logRetentionBytes")
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
    val newBrokerDefaults = new util.HashMap[String, Object](originalLogConfig.originals)
    newConfig.valuesFromThisConfig.forEach { (k, v) =>
      if (JDynamicLogConfig.getReconfigurableConfigs.contains(k)) {
        JDynamicLogConfig.KAFKA_CONFIG_TO_LOG_CONFIG_NAME.asScala.get(k).foreach { configName =>
          if (v == null)
             newBrokerDefaults.remove(configName)
          else
            newBrokerDefaults.put(configName, v.asInstanceOf[AnyRef])
        }
      }
    }

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

class ControllerDynamicThreadPool(controller: ControllerServer) extends BrokerReconfigurable {

  override def reconfigurableConfigs: util.Set[String] = {
    Collections.singleton(KafkaConfig.NUM_IO_THREADS_PROP)
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    JDynamicThreadPool.validateReconfiguration(controller.config, newConfig) // common validation
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    if (newConfig.numIoThreads != oldConfig.numIoThreads)
      controller.controllerApisHandlerPool.resizeThreadPool(newConfig.numIoThreads)
  }
}

class BrokerDynamicThreadPool(server: KafkaBroker) extends BrokerReconfigurable {

  override def reconfigurableConfigs: util.Set[String] = {
    JDynamicThreadPool.RECONFIGURABLE_CONFIGS
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    JDynamicThreadPool.validateReconfiguration(server.config, newConfig)
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
    configs.add(KafkaConfig.METRIC_REPORTER_CLASSES_PROP)
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
  private val propsOverride = Map[String, AnyRef](KafkaConfig.BROKER_ID_PROP -> brokerId.toString)
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
      val clientTelemetryReceiver = reporter match {
        case telemetry: ClientTelemetry => telemetry.clientReceiver()
        case _ => null
      }

      if (clientTelemetryReceiver != null) {
        dynamicConfig.clientMetricsReceiverPlugin.asScala match {
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

  @nowarn("cat=deprecation")
  private[server] def metricsReporterClasses(configs: util.Map[String, _]): mutable.Buffer[String] = {
    val reporters = mutable.Buffer[String]()
    reporters ++= configs.get(KafkaConfig.METRIC_REPORTER_CLASSES_PROP).asInstanceOf[util.List[String]].asScala
    if (configs.get(KafkaConfig.AUTO_INCLUDE_JMX_REPORTER_PROP).asInstanceOf[Boolean] &&
        !reporters.contains(classOf[JmxReporter].getName)) {
      reporters += classOf[JmxReporter].getName
    }
    reporters
  }
}

class DynamicClientQuotaCallback(
  quotaManagers: QuotaFactory.QuotaManagers,
  serverConfig: KafkaConfig
) extends Reconfigurable {

  override def configure(configs: util.Map[String, _]): Unit = {}

  override def reconfigurableConfigs(): util.Set[String] = {
    val configs = new util.HashSet[String]()
    quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable => configs.addAll(callback.reconfigurableConfigs)
      case _ =>
    }
    configs
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable => callback.validateReconfiguration(configs)
      case _ =>
    }
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    quotaManagers.clientQuotaCallback.foreach {
      case callback: Reconfigurable =>
        serverConfig.dynamicConfig.maybeReconfigure(callback, serverConfig.dynamicConfig.currentKafkaConfig, configs)
        true
      case _ => false
    }
  }
}

class DynamicListenerConfig(server: KafkaBroker) extends JDynamicListenerConfig with Logging {

  override def reconfigurableConfigs: util.Set[String] = {
    JDynamicListenerConfig.RECONFIGURABLE_CONFIGS
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
    val newListeners = listenersToMap(newConfig.listeners.asScala.map(EndPoint.fromJava))
    val newAdvertisedListeners = listenersToMap(newConfig.effectiveAdvertisedListeners.asScala.map(EndPoint.fromJava))
    val oldListeners = listenersToMap(oldConfig.listeners.asScala.map(EndPoint.fromJava))
    if (!newAdvertisedListeners.keySet.subsetOf(newListeners.keySet))
      throw new ConfigException(s"Advertised listeners '$newAdvertisedListeners' must be a subset of listeners '$newListeners'")
    if (!newListeners.keySet.subsetOf(newConfig.effectiveListenerSecurityProtocolMap.keySet.asScala))
      throw new ConfigException(s"Listeners '$newListeners' must be subset of listener map '${newConfig.effectiveListenerSecurityProtocolMap}'")
    newListeners.keySet.intersect(oldListeners.keySet).foreach { listenerName =>
      def immutableListenerConfigs(kafkaConfig: KafkaConfig, prefix: String): Map[String, AnyRef] = {
        kafkaConfig.originalsWithPrefix(prefix, true).asScala.filter { case (key, _) =>
          // skip the reconfigurable configs
          !RECONFIGURABLE_CONFIGS.contains(key) && !SocketServer.LISTENER_RECONFIGURABLE_CONFIGS.contains(key) && !DataPlaneAcceptor.ListenerReconfigurableConfigs.contains(key)
        }
      }
      if (immutableListenerConfigs(newConfig, listenerName.configPrefix) != immutableListenerConfigs(oldConfig, listenerName.configPrefix))
        throw new ConfigException(s"Configs cannot be updated dynamically for existing listener $listenerName, " +
          "restart broker or create a new listener for update")
      if (oldConfig.effectiveListenerSecurityProtocolMap.get(listenerName) != newConfig.effectiveListenerSecurityProtocolMap.get(listenerName))
        throw new ConfigException(s"Security protocol cannot be updated for existing listener $listenerName")
    }
    if (!newAdvertisedListeners.contains(newConfig.interBrokerListenerName))
      throw new ConfigException(s"Advertised listener must be specified for inter-broker listener ${newConfig.interBrokerListenerName}")

    // Currently, we do not support adding or removing listeners when in KRaft mode.
    // However, we support changing other listener configurations (max connections, etc.)
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedListeners.asScala.map(EndPoint.fromJava)),
        listenersToMap(newConfig.effectiveAdvertisedListeners.asScala.map(EndPoint.fromJava)))) {
      verifyListenerRegistrationAlterationSupported()
    }
  }

  def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val newListeners = newConfig.listeners.asScala.map(EndPoint.fromJava)
    val newListenerMap = listenersToMap(newListeners)
    val oldListeners = oldConfig.listeners.asScala.map(EndPoint.fromJava)
    val oldListenerMap = listenersToMap(oldListeners)
    val listenersRemoved = oldListeners.filterNot(e => newListenerMap.contains(e.listenerName))
    val listenersAdded = newListeners.filterNot(e => oldListenerMap.contains(e.listenerName))
    if (listenersRemoved.nonEmpty || listenersAdded.nonEmpty) {
      LoginManager.closeAll() // Clear SASL login cache to force re-login
      if (listenersRemoved.nonEmpty) server.socketServer.removeListeners(listenersRemoved)
      if (listenersAdded.nonEmpty) server.socketServer.addListeners(listenersAdded)
    }
    if (listenerRegistrationsAltered(listenersToMap(oldConfig.effectiveAdvertisedListeners.asScala.map(EndPoint.fromJava)),
        listenersToMap(newConfig.effectiveAdvertisedListeners.asScala.map(EndPoint.fromJava)))) {
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
    if (producerStateManagerConfig.producerIdExpirationMs != newConfig.producerIdExpirationMs) {
      info(s"Reconfigure ${KafkaConfig.PRODUCER_ID_EXPIRATION_MS_PROP} from ${producerStateManagerConfig.producerIdExpirationMs} to ${newConfig.producerIdExpirationMs}")
      producerStateManagerConfig.setProducerIdExpirationMs(newConfig.producerIdExpirationMs)
    }
    if (producerStateManagerConfig.transactionVerificationEnabled != newConfig.transactionPartitionVerificationEnable) {
      info(s"Reconfigure ${KafkaConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_PROP} from ${producerStateManagerConfig.transactionVerificationEnabled} to ${newConfig.transactionPartitionVerificationEnable}")
      producerStateManagerConfig.setTransactionVerificationEnabled(newConfig.transactionPartitionVerificationEnable)
    }
  }

  def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    if (newConfig.producerIdExpirationMs < 0)
      throw new ConfigException(s"${KafkaConfig.PRODUCER_ID_EXPIRATION_MS_PROP} cannot be less than 0, current value is ${producerStateManagerConfig.producerIdExpirationMs}, and new value is ${newConfig.producerIdExpirationMs}")
  }

  override def reconfigurableConfigs: util.Set[String] = ProducerStateManagerConfig.RECONFIGURABLE_CONFIGS

}

class DynamicRemoteLogConfig(server: KafkaBroker) extends JDynamicRemoteLogConfig with Logging {
  override def reconfigurableConfigs: util.Set[String] = {
    JDynamicRemoteLogConfig.RECONFIGURABLE_CONFIGS
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    newConfig.values.forEach { (k, v) =>
      if (reconfigurableConfigs.contains(k)) {
        if (k.equals(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)) {
          val newValue = v.asInstanceOf[Long]
          val oldValue = getValue(server.config, k)
          if (newValue != oldValue && newValue <= 0) {
            val errorMsg = s"Dynamic remote log manager config update validation failed for $k=$v"
            throw new ConfigException(s"$errorMsg, value should be at least 1")
          }
        }
      }
    }
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    val oldValue = oldConfig.getLong(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)
    val newValue = newConfig.getLong(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)
    if (oldValue != newValue) {
      val remoteLogManager = server.remoteLogManagerOpt
      if (remoteLogManager.nonEmpty) {
        remoteLogManager.get.resizeCacheSize(newValue)
        info(s"Dynamic remote log manager config: ${RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP} updated, " +
          s"old value: $oldValue, new value: $newValue")
      }
    }
  }

  private def getValue(config: KafkaConfig, name: String): Long = {
    name match {
      case RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP =>
        config.getLong(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP)
      case n => throw new IllegalStateException(s"Unexpected dynamic remote log manager config $n")
    }
  }
}