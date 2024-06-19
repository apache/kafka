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

import kafka.utils.Implicits._
import kafka.utils.Logging
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.ConfigDef.ConfigKey
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigDef, ConfigException, ConfigResource}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config._
import org.apache.kafka.storage.internals.log.LogConfig
import org.apache.zookeeper.client.ZKClientConfig

import java.util
import java.util.{Collections, Properties}
import scala.annotation.nowarn
import scala.collection.{Map, Seq}
import scala.compat.java8.OptionConverters._
import scala.jdk.CollectionConverters._

object KafkaConfig {

  def main(args: Array[String]): Unit = {
    System.out.println(configDef.toHtml(4, (config: String) => "brokerconfigs_" + config,
      DynamicBrokerConfig.dynamicConfigUpdateModes))
  }

  private[kafka] def zooKeeperClientProperty(clientConfig: ZKClientConfig, kafkaPropName: String): Option[String] = {
    Option(clientConfig.getProperty(ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaPropName)))
  }

  private[kafka] def setZooKeeperClientProperty(clientConfig: ZKClientConfig, kafkaPropName: String, kafkaPropValue: Any): Unit = {
    clientConfig.setProperty(ZkConfigs.ZK_SSL_CONFIG_TO_SYSTEM_PROPERTY_MAP.get(kafkaPropName),
      kafkaPropName match {
        case ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG => (kafkaPropValue.toString.toUpperCase == "HTTPS").toString
        case ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG | ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG => kafkaPropValue match {
          case list: java.util.List[_] => list.asScala.mkString(",")
          case _ => kafkaPropValue.toString
        }
        case _ => kafkaPropValue.toString
    })
  }

  // For ZooKeeper TLS client authentication to be enabled the client must (at a minimum) configure itself as using TLS
  // with both a client connection socket and a key store location explicitly set.
  private[kafka] def zkTlsClientAuthEnabled(zkClientConfig: ZKClientConfig): Boolean = {
    zooKeeperClientProperty(zkClientConfig, ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG).contains("true") &&
      zooKeeperClientProperty(zkClientConfig, ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG).isDefined &&
      zooKeeperClientProperty(zkClientConfig, ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG).isDefined
  }

  /** ********* Remote Log Management Configuration *********/
  val configDef = AbstractKafkaConfig.CONFIG_DEF

  def configNames: Seq[String] = configDef.names.asScala.toBuffer.sorted
  private[server] def defaultValues: Map[String, _] = configDef.defaultValues.asScala
  private[server] def configKeys: Map[String, ConfigKey] = configDef.configKeys.asScala

  def fromProps(props: Properties): KafkaConfig =
    fromProps(props, true)

  def fromProps(props: Properties, doLog: Boolean): KafkaConfig =
    new KafkaConfig(props, doLog)

  def fromProps(defaults: Properties, overrides: Properties): KafkaConfig =
    fromProps(defaults, overrides, true)

  def fromProps(defaults: Properties, overrides: Properties, doLog: Boolean): KafkaConfig = {
    val props = new Properties()
    props ++= defaults
    props ++= overrides
    fromProps(props, doLog)
  }

  def apply(props: java.util.Map[_, _], doLog: Boolean = true): KafkaConfig = new KafkaConfig(props, doLog)

  private def typeOf(name: String): Option[ConfigDef.Type] = Option(configDef.configKeys.get(name)).map(_.`type`)

  def configType(configName: String): Option[ConfigDef.Type] = {
    val configType = configTypeExact(configName)
    if (configType.isDefined) {
      return configType
    }
    typeOf(configName) match {
      case Some(t) => Some(t)
      case None =>
        DynamicBrokerConfig.brokerConfigSynonyms(configName, matchListenerOverride = true).flatMap(typeOf).headOption
    }
  }

  private def configTypeExact(exactName: String): Option[ConfigDef.Type] = {
    val configType = typeOf(exactName).orNull
    if (configType != null) {
      Some(configType)
    } else {
      val configKey = DynamicConfig.Broker.configKeys.get(exactName)
      if (configKey != null) {
        Some(configKey.`type`)
      } else {
        None
      }
    }
  }

  def maybeSensitive(configType: Option[ConfigDef.Type]): Boolean = {
    // If we can't determine the config entry type, treat it as a sensitive config to be safe
    configType.isEmpty || configType.contains(ConfigDef.Type.PASSWORD)
  }

  def loggableValue(resourceType: ConfigResource.Type, name: String, value: String): String = {
    val maybeSensitive = resourceType match {
      case ConfigResource.Type.BROKER => KafkaConfig.maybeSensitive(KafkaConfig.configType(name))
      case ConfigResource.Type.TOPIC => KafkaConfig.maybeSensitive(LogConfig.configType(name).asScala)
      case ConfigResource.Type.BROKER_LOGGER => false
      case ConfigResource.Type.CLIENT_METRICS => false
      case _ => true
    }
    if (maybeSensitive) Password.HIDDEN else value
  }

  /**
   * Copy a configuration map, populating some keys that we want to treat as synonyms.
   */
  def populateSynonyms(input: util.Map[_, _]): util.Map[Any, Any] = {
    val output = new util.HashMap[Any, Any](input)
    val brokerId = output.get(ServerConfigs.BROKER_ID_CONFIG)
    val nodeId = output.get(KRaftConfigs.NODE_ID_CONFIG)
    if (brokerId == null && nodeId != null) {
      output.put(ServerConfigs.BROKER_ID_CONFIG, nodeId)
    } else if (brokerId != null && nodeId == null) {
      output.put(KRaftConfigs.NODE_ID_CONFIG, brokerId)
    }
    output
  }
}

/**
 * The class extend {@link AbstractKafkaConfig} which will be the future KafkaConfig.
 * When add any new methods if it doesn't depend on anything in Core, then move it to org.apache.kafka.server.config.KafkaConfig instead of here.
 * Any code depends on kafka.server.KafkaConfig will keep for using kafka.server.KafkaConfig for the time being until we move it out of core
 * For more details check KAFKA-15853
 */
class KafkaConfig private(doLog: Boolean, val props: java.util.Map[_, _], dynamicConfigOverride: Option[DynamicBrokerConfig])
  extends AbstractKafkaConfig(KafkaConfig.configDef, props, Utils.castToStringObjectMap(props), doLog) with Logging {

  def this(props: java.util.Map[_, _]) = this(true, KafkaConfig.populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean) = this(doLog, KafkaConfig.populateSynonyms(props), None)
  def this(props: java.util.Map[_, _], doLog: Boolean, dynamicConfigOverride: Option[DynamicBrokerConfig]) =
    this(doLog, KafkaConfig.populateSynonyms(props), dynamicConfigOverride)

  // Cache the current config to avoid acquiring read lock to access from dynamicConfig
  private[server] val dynamicConfig = dynamicConfigOverride.getOrElse(new DynamicBrokerConfig(this))

  private[server] def updateCurrentConfig(newConfig: KafkaConfig): Unit = {
    this.currentConfig = newConfig
  }

  // The following captures any system properties impacting ZooKeeper TLS configuration
  // and defines the default values this instance will use if no explicit config is given.
  // We make it part of each instance rather than the object to facilitate testing.
  private val zkClientConfigViaSystemProperties = new ZKClientConfig()

  /** ********* Zookeeper Configuration ********** */
  val zkConnect: String = getString(ZkConfigs.ZK_CONNECT_CONFIG)
  val zkSessionTimeoutMs: Int = getInt(ZkConfigs.ZK_SESSION_TIMEOUT_MS_CONFIG)
  val zkConnectionTimeoutMs: Int =
    Option(getInt(ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_CONFIG)).map(_.toInt).getOrElse(getInt(ZkConfigs.ZK_SESSION_TIMEOUT_MS_CONFIG))
  val zkEnableSecureAcls: Boolean = getBoolean(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG)
  val zkMaxInFlightRequests: Int = getInt(ZkConfigs.ZK_MAX_IN_FLIGHT_REQUESTS_CONFIG)

  private def zkBooleanConfigOrSystemPropertyWithDefaultValue(propKey: String): Boolean = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false (String) to true/false (Boolean)
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getBoolean(propKey) else {
      val sysPropValue = KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
      sysPropValue match {
        case Some("true") => true
        case Some(_) => false
        case _ => getBoolean(propKey) // not specified so use the default value
      }
    }
  }

  private def zkStringConfigOrSystemPropertyWithDefaultValue(propKey: String): String = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    val actuallyProvided = originals.containsKey(propKey)
    if (actuallyProvided) getString(propKey) else {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey) match {
        case Some(v) => v
        case _ => getString(propKey) // not specified so use the default value
      }
    }
  }

  private def zkOptionalStringConfigOrSystemProperty(propKey: String): Option[String] = {
    Option(getString(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey)
    }
  }
  private def zkPasswordConfigOrSystemProperty(propKey: String): Option[Password] = {
    Option(getPassword(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map(new Password(_))
    }
  }
  private def zkListConfigOrSystemProperty(propKey: String): Option[util.List[String]] = {
    Option(getList(propKey)).orElse {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, propKey).map { sysProp =>
        sysProp.split("\\s*,\\s*").toBuffer.asJava
      }
    }
  }

  val zkSslClientEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG)
  val zkClientCnxnSocketClassName = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_CLIENT_CNXN_SOCKET_CONFIG)
  val zkSslKeyStoreLocation = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG)
  val zkSslKeyStorePassword = zkPasswordConfigOrSystemProperty(ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_CONFIG)
  val zkSslKeyStoreType = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_KEY_STORE_TYPE_CONFIG)
  val zkSslTrustStoreLocation = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_CONFIG)
  val zkSslTrustStorePassword = zkPasswordConfigOrSystemProperty(ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_CONFIG)
  val zkSslTrustStoreType = zkOptionalStringConfigOrSystemProperty(ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_CONFIG)
  val ZkSslProtocol = zkStringConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_PROTOCOL_CONFIG)
  val ZkSslEnabledProtocols = zkListConfigOrSystemProperty(ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG)
  val ZkSslCipherSuites = zkListConfigOrSystemProperty(ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG)
  val ZkSslEndpointIdentificationAlgorithm = {
    // Use the system property if it exists and the Kafka config value was defaulted rather than actually provided
    // Need to translate any system property value from true/false to HTTPS/<blank>
    val kafkaProp = ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
    val actuallyProvided = originals.containsKey(kafkaProp)
    if (actuallyProvided)
      getString(kafkaProp)
    else {
      KafkaConfig.zooKeeperClientProperty(zkClientConfigViaSystemProperties, kafkaProp) match {
        case Some("true") => "HTTPS"
        case Some(_) => ""
        case None => getString(kafkaProp) // not specified so use the default value
      }
    }
  }
  val ZkSslCrlEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG)
  val ZkSslOcspEnable = zkBooleanConfigOrSystemPropertyWithDefaultValue(ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG)

  def addReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.addReconfigurable(reconfigurable)
  }

  def removeReconfigurable(reconfigurable: Reconfigurable): Unit = {
    dynamicConfig.removeReconfigurable(reconfigurable)
  }

  validateValues()

  /**
   * This method is kept here for now to reduce the conflict between the work of moving KafkaConfig out and removing ZK.
   * Any validations related to ZK mode will stay here until we cleanup the ZK code.
   * Other validations will be defined in {@link org.apache.kafka.server.config.KafkaConfigValidator}
   */
  @nowarn("cat=deprecation")
  private def validateValues(): Unit = {
    configValidator.validateNodeAndBrokerId()

    if (requiresZookeeper) {
      if (zkConnect == null) {
        throw new ConfigException(s"Missing required configuration `${ZkConfigs.ZK_CONNECT_CONFIG}` which has no default value.")
      }
      if (brokerIdGenerationEnable) {
        require(brokerId >= -1 && brokerId <= maxReservedBrokerId, "broker.id must be greater than or equal to -1 and not greater than reserved.broker.max.id")
      } else {
        require(brokerId >= 0, "broker.id must be greater than or equal to 0")
      }
    } else {
      // KRaft-based metadata quorum
      if (nodeId < 0) {
        throw new ConfigException(s"Missing configuration `${KRaftConfigs.NODE_ID_CONFIG}` which is required " +
          s"when `process.roles` is defined (i.e. when running in KRaft mode).")
      }
      if (migrationEnabled) {
        if (zkConnect == null) {
          throw new ConfigException(s"If using `${KRaftConfigs.MIGRATION_ENABLED_CONFIG}` in KRaft mode, `${ZkConfigs.ZK_CONNECT_CONFIG}` must also be set.")
        }
      }
    }

    configValidator.validateLogConfig()
    configValidator.validateReplicaFetchConfigs()
    configValidator.validateOffsetCommitAcks()

    // validate KRaft-related configs
    if (processRoles == Collections.singleton(ProcessRole.BrokerRole)) {
      configValidator.validateKraftBrokerConfig()
    } else if (processRoles == Collections.singleton(ProcessRole.ControllerRole)) {
      configValidator.validateKraftControllerConfig()
    } else if (isKRaftCombinedMode) {
      configValidator.validateKraftCombinedModeConfig()
    } else {
      // ZK-based
      if (migrationEnabled) {
        configValidator.validateNonEmptyQuorumVotersForMigration()
        require(controllerListenerNames.asScala.nonEmpty,
          s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must not be empty when running in ZooKeeper migration mode: ${controllerListenerNames}")
        require(interBrokerProtocolVersion.isMigrationSupported, s"Cannot enable ZooKeeper migration without setting " +
          s"'${ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG}' to 3.4 or higher")
        if (logDirs.size > 1) {
          require(interBrokerProtocolVersion.isDirectoryAssignmentSupported,
            s"Cannot enable ZooKeeper migration with multiple log directories (aka JBOD) without setting " +
            s"'${ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG}' to ${MetadataVersion.IBP_3_7_IV2} or higher")
        }
      } else {
        // controller listener names must be empty when not in KRaft mode
        require(controllerListenerNames.isEmpty,
          s"${KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG} must be empty when not running in KRaft mode: ${controllerListenerNames}")
      }
      configValidator.validateAdvertisedListenersNonEmptyForBroker()
    }

    configValidator.validateListenerNames()
    configValidator.validateMessageFormatConfigs()
    configValidator.validateCompressionConfig()
    configValidator.validateInterBrokerSecurityConfig()
    configValidator.validateQueueMaxByte()
    configValidator.validateConnectionConfigs()
    configValidator.validateNewGroupCoordinatorConfigs()
    configValidator.validateSharedGroupConfigs
  }
}
