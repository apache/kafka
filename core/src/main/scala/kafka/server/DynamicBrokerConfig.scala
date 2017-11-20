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

import kafka.log.LogConfig
import kafka.server.DynamicBrokerConfig._
import kafka.utils.{CoreUtils, Logging}
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigDef, ConfigException, SslConfigs}
import org.apache.kafka.common.network.ChannelBuilder
import org.apache.kafka.common.utils.Base64

import scala.collection._
import scala.collection.JavaConverters._

object DynamicBrokerConfig {

  // Map topic config to the broker config with highest priority. Some of these have additional synonyms
  // that can be obtained using `brokerConfigSynonyms`
  val topicConfigSynonyms = Map(
    LogConfig.SegmentBytesProp -> KafkaConfig.LogSegmentBytesProp,
    LogConfig.SegmentMsProp -> KafkaConfig.LogRollTimeMillisProp,
    LogConfig.SegmentJitterMsProp -> KafkaConfig.LogRollTimeJitterMillisProp,
    LogConfig.SegmentIndexBytesProp -> KafkaConfig.LogIndexSizeMaxBytesProp,
    LogConfig.FlushMessagesProp -> KafkaConfig.LogFlushIntervalMessagesProp,
    LogConfig.FlushMsProp -> KafkaConfig.LogFlushIntervalMsProp,
    LogConfig.RetentionBytesProp -> KafkaConfig.LogRetentionBytesProp,
    LogConfig.RetentionMsProp -> KafkaConfig.LogRetentionTimeMillisProp,
    LogConfig.MaxMessageBytesProp -> KafkaConfig.MessageMaxBytesProp,
    LogConfig.IndexIntervalBytesProp -> KafkaConfig.LogIndexIntervalBytesProp,
    LogConfig.DeleteRetentionMsProp -> KafkaConfig.LogCleanerDeleteRetentionMsProp,
    LogConfig.MinCompactionLagMsProp -> KafkaConfig.LogCleanerMinCompactionLagMsProp,
    LogConfig.FileDeleteDelayMsProp -> KafkaConfig.LogDeleteDelayMsProp,
    LogConfig.MinCleanableDirtyRatioProp -> KafkaConfig.LogCleanerMinCleanRatioProp,
    LogConfig.CleanupPolicyProp -> KafkaConfig.LogCleanupPolicyProp,
    LogConfig.UncleanLeaderElectionEnableProp -> KafkaConfig.UncleanLeaderElectionEnableProp,
    LogConfig.MinInSyncReplicasProp -> KafkaConfig.MinInSyncReplicasProp,
    LogConfig.CompressionTypeProp -> KafkaConfig.CompressionTypeProp,
    LogConfig.PreAllocateEnableProp -> KafkaConfig.LogPreAllocateProp,
    LogConfig.MessageFormatVersionProp -> KafkaConfig.LogMessageFormatVersionProp,
    LogConfig.MessageTimestampTypeProp -> KafkaConfig.LogMessageTimestampTypeProp,
    LogConfig.MessageTimestampDifferenceMaxMsProp -> KafkaConfig.LogMessageTimestampDifferenceMaxMsProp
  )

  private val dynamicPasswordConfigs = Set(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
    SslConfigs.SSL_KEY_PASSWORD_CONFIG)
  private val dynamicSecurityConfigs = SslConfigs.RECONFIGURABLE_CONFIGS.asScala

  val allDynamicConfigs = mutable.Set[String]()
  allDynamicConfigs ++= dynamicSecurityConfigs

  val listenerConfigRegex = """listener\.name\.[^.]*\.(.*)""".r

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
      case listenerConfigRegex(baseName) if matchListenerOverride => List(name, baseName)
      case n => List(n)
    }
  }

  private[server] def addDynamicConfigs(configDef: ConfigDef): Unit = {
    KafkaConfig.configKeys.filterKeys(allDynamicConfigs.contains).values.foreach { config =>
      configDef.define(config.name, config.`type`, config.defaultValue, config.validator,
        config.importance, config.documentation, config.group, config.orderInGroup, config.width,
        config.displayName, config.dependents, config.recommender)
    }
  }
}

class DynamicBrokerConfig(private val kafkaConfig: KafkaConfig) extends Logging {

  private[server] val staticBrokerConfigs = configToMap(kafkaConfig.originalsFromThisConfig.asScala)
  private[server] val staticDefaultConfigs = configToMap(KafkaConfig.defaultValues)
  private[server] val dynamicBrokerConfigs = mutable.Map[String, String]()
  private[server] val dynamicDefaultConfigs = mutable.Map[String, String]()
  private val brokerId = kafkaConfig.brokerId
  private val reconfigurables = mutable.Buffer[Reconfigurable]()
  private val lock = new ReentrantReadWriteLock
  private var currentConfig = kafkaConfig

  def initialize(zkClient: KafkaZkClient): Unit = {
    val adminZkClient = new AdminZkClient(zkClient)
    updateDefaultConfig(adminZkClient.fetchEntityConfig(ConfigType.Broker, ConfigEntityName.Default))
    updateBrokerConfig(brokerId, adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.toString))
    updateCurrentConfig()
  }

  def config: KafkaConfig = CoreUtils.inReadLock(lock) {
    currentConfig
  }

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    require(reconfigurable.reconfigurableConfigs.asScala.forall(allDynamicConfigs.contains))
    reconfigurables += reconfigurable
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = CoreUtils.inWriteLock(lock) {
    reconfigurables -= reconfigurable
  }

  private[server] def updateBrokerConfig(brokerId: Int, persistentProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    val props = fromPersistentProps(persistentProps, perBrokerConfig = true)
    validateConfigTypes(Some(brokerId), props, logError = true)
    dynamicBrokerConfigs.clear()
    dynamicBrokerConfigs ++= props.asScala
    updateCurrentConfig()
  }

  private[server] def updateDefaultConfig(persistentProps: Properties): Unit = CoreUtils.inWriteLock(lock) {
    val props = fromPersistentProps(persistentProps, perBrokerConfig = false)
    validateConfigTypes(None, props, logError = true)
    dynamicDefaultConfigs.clear()
    dynamicDefaultConfigs ++= props.asScala
    updateCurrentConfig()
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
    dynamicPasswordConfigs.foreach(encodePassword)
    props
  }

  private[server] def fromPersistentProps(persistentProps: Properties, perBrokerConfig: Boolean): Properties = {
    val props = persistentProps.clone().asInstanceOf[Properties]
    // TODO (KAFKA-6246): encrypt passwords
    def decodePassword(configName: String): Unit = {
      val value = props.getProperty(configName)
      if (value != null) {
        if (!perBrokerConfig)
          warn(s"Password config $configName defined at default cluster level will be ignored")
        props.setProperty(configName, new String(Base64.decoder.decode(value), StandardCharsets.UTF_8))
      }
    }
    dynamicPasswordConfigs.foreach(decodePassword)
    props
  }

  private[server] def validateAndConvertForPersistence(brokerId: Option[Int], props: Properties): Properties = CoreUtils.inReadLock(lock) {
    validateConfigTypes(brokerId, props, logError = false)
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    brokerId match {
      case Some(_) =>
        overrideProps(newProps, dynamicDefaultConfigs)
        overrideProps(newProps, props.asScala)
      case None =>
        overrideProps(newProps, props.asScala)
        overrideProps(newProps, dynamicBrokerConfigs)
    }
    processConfig(newProps, validateOnly = true)
    toPersistentProps(props, brokerId.nonEmpty)
  }

  private def validateConfigTypes(brokerId: Option[Int], props: Properties, logError: Boolean): Unit = {
    try {
      val securityConfigsWithoutPrefix = dynamicSecurityConfigs.filter(props.containsKey)
      if (securityConfigsWithoutPrefix.nonEmpty)
          throw new ConfigException(s"Invalid configs $securityConfigsWithoutPrefix: security configs can be dynamically updated only using listener prefix")
      val baseProps = new Properties
      props.asScala.foreach {
        case (listenerConfigRegex(baseName), v) => baseProps.put(baseName, v)
        case (k, v) => baseProps.put(k, v)
      }
      DynamicConfig.Broker.validate(baseProps)
    } catch {
      case e: Exception =>
        if (logError)
          error(s"Dynamic default broker config is invalid: $props, these configs will be ignored", e)
        throw e
    }
  }

  private def configToMap(config: Map[String, _]): immutable.Map[String, String] = {
    config.filter(_._2 != null).map {
      case (k, v: String) => (k, v)
      case (k, v: Password) => (k, v.value)
      case (k, v: java.util.List[_]) => (k, ConfigDef.convertToString(v, ConfigDef.Type.LIST))
      case (k, v: Class[_]) => (k, ConfigDef.convertToString(v, ConfigDef.Type.CLASS))
      case (k, v) => (k, String.valueOf(v))
    }.toMap
  }

  private def updatedConfigs(newProps: java.util.Map[String, _], currentProps: java.util.Map[_, _]): mutable.Map[String, _] = {
    newProps.asScala.filter {
      case (k, v) => v != currentProps.get(k)
    }
  }

  private def overrideProps(props: mutable.Map[String, String], propsOverride: mutable.Map[String, String]): Unit = {
    propsOverride.foreach { case (k, v) =>
      brokerConfigSynonyms(k, matchListenerOverride = false).foreach(props.remove)
      props.put(k, v)
    }
  }

  private def updateCurrentConfig(): Unit = {
    val newProps = mutable.Map[String, String]()
    newProps ++= staticBrokerConfigs
    overrideProps(newProps, dynamicDefaultConfigs)
    overrideProps(newProps, dynamicBrokerConfigs)
    currentConfig = processConfig(newProps, validateOnly = false)
  }

  private def processConfig(newProps: mutable.Map[String, String], validateOnly: Boolean): KafkaConfig = {
    val newConfig = new KafkaConfig(newProps.asJava, true, None)
    val updatedMap = updatedConfigs(newConfig.originalsFromThisConfig, currentConfig.originals)
    if (updatedMap.nonEmpty) {
      try {
        val customConfigs = new util.HashMap[String, Object](newConfig.originalsFromThisConfig) // non-Kafka configs
        newConfig.valuesFromThisConfig.keySet.asScala.foreach(customConfigs.remove)
        reconfigurables.foreach {
          case channelBuilder: ChannelBuilder =>
            val listenerName = channelBuilder.listenerName
            val oldValues = currentConfig.valuesWithPrefixOverride(listenerName.configPrefix)
            val newValues = newConfig.valuesFromThisConfigWithPrefixOverride(listenerName.configPrefix)
            val updatedKeys = updatedConfigs(newValues, oldValues).keySet
            processReconfigurable(channelBuilder, updatedKeys, newValues, customConfigs, validateOnly)
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
      if (validateOnly)
        reconfigurable.validate(newConfigs)
      else
        reconfigurable.reconfigure(newConfigs)
    }
  }
}
