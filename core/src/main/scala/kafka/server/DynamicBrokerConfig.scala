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

import java.nio.charset.StandardCharsets
import java.util
import java.util.Properties
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.server.DynamicBrokerConfig._
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.{ConfigDef, ConfigException, SslConfigs}
import org.apache.kafka.common.network.ListenerReconfigurable
import org.apache.kafka.common.utils.Base64

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

  private val DynamicPasswordConfigs = Set(
    SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
    SslConfigs.SSL_KEY_PASSWORD_CONFIG
  )
  private val DynamicSecurityConfigs = SslConfigs.RECONFIGURABLE_CONFIGS.asScala

  val AllDynamicConfigs = mutable.Set[String]()
  AllDynamicConfigs ++= DynamicSecurityConfigs

  private val PerBrokerConfigs = DynamicSecurityConfigs

  val ListenerConfigRegex = """listener\.name\.[^.]*\.(.*)""".r


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
      case ListenerConfigRegex(baseName) if matchListenerOverride => List(name, baseName)
      case _ => List(name)
    }
  }

  private[server] def addDynamicConfigs(configDef: ConfigDef): Unit = {
    KafkaConfig.configKeys.filterKeys(AllDynamicConfigs.contains).values.foreach { config =>
      configDef.define(config.name, config.`type`, config.defaultValue, config.validator,
        config.importance, config.documentation, config.group, config.orderInGroup, config.width,
        config.displayName, config.dependents, config.recommender)
    }
  }
}

class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging {

  private[server] val staticBrokerConfigs = ConfigDef.convertToStringMapWithPasswordValues(kafkaConfig.originalsFromThisConfig).asScala
  private[server] val staticDefaultConfigs = ConfigDef.convertToStringMapWithPasswordValues(KafkaConfig.defaultValues.asJava).asScala
  private val dynamicBrokerConfigs = mutable.Map[String, String]()
  private val dynamicDefaultConfigs = mutable.Map[String, String]()
  private val brokerId = kafkaConfig.brokerId
  private val reconfigurables = mutable.Buffer[Reconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var currentConfig = kafkaConfig

  private[server] def initialize(zkClient: KafkaZkClient): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)
    updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default))
    updateBrokerConfig(brokerId, adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.toString))
  }

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    require(reconfigurable.reconfigurableConfigs.asScala.forall(AllDynamicConfigs.contains))
    reconfigurables += reconfigurable
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables -= reconfigurable
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

  private[server] def toPersistentProps(configProps: Properties, perBrokerConfig: Boolean): Properties = {
    val props = configProps.clone().asInstanceOf[Properties]
    // TODO (KAFKA-6246): encrypt passwords
    def encodePassword(configName: String): Unit = {
      val value = props.getProperty(configName)
      if (value != null) {
        if (!perBrokerConfig)
          throw new ConfigException("Password config can be defined only at broker level")
        props.setProperty(configName, Base64.encoder.encodeToString(value.getBytes(StandardCharsets.UTF_8)))
      }
    }
    DynamicPasswordConfigs.foreach(encodePassword)
    props
  }

  private[server] def fromPersistentProps(persistentProps: Properties, perBrokerConfig: Boolean): Properties = {
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

    // TODO (KAFKA-6246): encrypt passwords
    def decodePassword(configName: String): Unit = {
      val value = props.getProperty(configName)
      if (value != null) {
        props.setProperty(configName, new String(Base64.decoder.decode(value), StandardCharsets.UTF_8))
      }
    }
    DynamicPasswordConfigs.foreach(decodePassword)
    props
  }

  private[server] def validate(props: Properties, perBrokerConfig: Boolean): Unit = CoreUtils.inReadLock(lock) {
    def checkInvalidProps(invalidPropNames: Set[String], errorMessage: String): Unit = {
      if (invalidPropNames.nonEmpty)
        throw new ConfigException(s"$errorMessage: $invalidPropNames")
    }
    checkInvalidProps(nonDynamicConfigs(props), "Cannot update these configs dynamically")
    checkInvalidProps(securityConfigsWithoutListenerPrefix(props),
      "These security configs can be dynamically updated only per-listener using the listener prefix")
    validateConfigTypes(props)
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    if (perBrokerConfig) {
      overrideProps(newProps, dynamicDefaultConfigs)
      overrideProps(newProps, props.asScala)
    } else {
      checkInvalidProps(perBrokerConfigs(props),
        "Cannot update these configs at default cluster level, broker id must be specified")
      overrideProps(newProps, props.asScala)
      overrideProps(newProps, dynamicBrokerConfigs)
    }
    processReconfiguration(newProps, validateOnly = true)
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
    val newConfig = processReconfiguration(newProps, validateOnly = false)
    if (newConfig ne currentConfig) {
      currentConfig = newConfig
      kafkaConfig.updateCurrentConfig(currentConfig)
    }
  }

  private def processReconfiguration(newProps: Map[String, String], validateOnly: Boolean): KafkaConfig = {
    val newConfig = new KafkaConfig(newProps.asJava, !validateOnly, None)
    val updatedMap = updatedConfigs(newConfig.originalsFromThisConfig, currentConfig.originals)
    if (updatedMap.nonEmpty) {
      try {
        val customConfigs = new util.HashMap[String, Object](newConfig.originalsFromThisConfig) // non-Kafka configs
        newConfig.valuesFromThisConfig.keySet.asScala.foreach(customConfigs.remove)
        reconfigurables.foreach {
          case listenerReconfigurable: ListenerReconfigurable =>
            val listenerName = listenerReconfigurable.listenerName
            val oldValues = currentConfig.valuesWithPrefixOverride(listenerName.configPrefix)
            val newValues = newConfig.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
            val updatedKeys = updatedConfigs(newValues, oldValues).keySet
            processReconfigurable(listenerReconfigurable, updatedKeys, newValues, customConfigs, validateOnly)
          case reconfigurable =>
            processReconfigurable(reconfigurable, updatedMap.keySet, newConfig.valuesFromThisConfig, customConfigs, validateOnly)
        }
        newConfig
      } catch {
        case e: Exception =>
          if (!validateOnly)
            error(s"Failed to update broker configuration with configs : ${newConfig.originalsFromThisConfig}", e)
          throw new ConfigException("Invalid dynamic configuration", e)
      }
    }
    else
      currentConfig
  }

  private def processReconfigurable(reconfigurable: Reconfigurable, updatedKeys: Set[String],
                                    allNewConfigs: util.Map[String, _], newCustomConfigs: util.Map[String, Object],
                                    validateOnly: Boolean): Unit = {
    if (reconfigurable.reconfigurableConfigs.asScala.intersect(updatedKeys).nonEmpty) {
      val newConfigs = new util.HashMap[String, Object]
      allNewConfigs.asScala.foreach { case (k, v) => newConfigs.put(k, v.asInstanceOf[AnyRef]) }
      newConfigs.putAll(newCustomConfigs)
      if (validateOnly) {
        if (!reconfigurable.validateReconfiguration(newConfigs))
          throw new ConfigException("Validation of dynamic config update failed")
      } else
        reconfigurable.reconfigure(newConfigs)
    }
  }
}
