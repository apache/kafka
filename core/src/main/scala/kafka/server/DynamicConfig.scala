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

import java.util.Properties

import kafka.log.LogConfig
import kafka.security.CredentialProvider
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance._
import org.apache.kafka.common.config.ConfigDef.Range._
import org.apache.kafka.common.config.ConfigDef.Type._
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.CommonClientConfigs;

import scala.jdk.CollectionConverters._

/**
  * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
  * and can only be set dynamically.
  */
object DynamicConfig {

  object Broker {
    // Properties
    val LeaderReplicationThrottledRateProp = "leader.replication.throttled.rate"
    val FollowerReplicationThrottledRateProp = "follower.replication.throttled.rate"
    val ReplicaAlterLogDirsIoMaxBytesPerSecondProp = "replica.alter.log.dirs.io.max.bytes.per.second"

    // Defaults
    val DefaultReplicationThrottledRate = ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault

    // Documentation
    val LeaderReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
      s"property ${LogConfig.LeaderReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
      s"limit be kept above 1MB/s for accurate behaviour."
    val FollowerReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
      s"property ${LogConfig.FollowerReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
      s"limit be kept above 1MB/s for accurate behaviour."
    val ReplicaAlterLogDirsIoMaxBytesPerSecondDoc = "A long representing the upper bound (bytes/sec) on disk IO used for moving replica between log directories on the same broker. " +
      s"This property can be only set dynamically. It is suggested that the limit be kept above 1MB/s for accurate behaviour."

    // Definitions
    val brokerConfigDef = new ConfigDef()
      // Round minimum value down, to make it easier for users.
      .define(LeaderReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, LeaderReplicationThrottledRateDoc)
      .define(FollowerReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, FollowerReplicationThrottledRateDoc)
      .define(ReplicaAlterLogDirsIoMaxBytesPerSecondProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, ReplicaAlterLogDirsIoMaxBytesPerSecondDoc)
    DynamicBrokerConfig.addDynamicConfigs(brokerConfigDef)
    val nonDynamicProps = KafkaConfig.configNames.toSet -- brokerConfigDef.names.asScala

    def names = brokerConfigDef.names

    def validate(props: Properties) = DynamicConfig.validate(brokerConfigDef, props, customPropsAllowed = true)
  }

  object QuotaConfigs {
    val ProducerByteRateOverrideProp = "producer_byte_rate"
    val ConsumerByteRateOverrideProp = "consumer_byte_rate"
    val RequestPercentageOverrideProp = "request_percentage"
    val ControllerMutationOverrideProp = "controller_mutation_rate"
    private val configNames = Set(ProducerByteRateOverrideProp, ConsumerByteRateOverrideProp,
      RequestPercentageOverrideProp, ControllerMutationOverrideProp)

    def isQuotaConfig(name: String): Boolean = configNames.contains(name)
  }

  object ClientConfigs {
    val AcksOverrideProp = ProducerConfig.ACKS_CONFIG
    val SessionTimeoutOverrideProp = CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG
    val HeartbeatIntervalOverrideProp = CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG
    val SupportedConfigsProp = "supported.configs"

    private val configNames = Set(AcksOverrideProp,
      SessionTimeoutOverrideProp, HeartbeatIntervalOverrideProp, SupportedConfigsProp)

    def isClientConfig(name: String): Boolean = configNames.contains(name)
    def isClientConfigAndNotRegistration(name: String): Boolean = (configNames - SupportedConfigsProp).contains(name)
  }

  object Client {
    // Properties
    val ProducerByteRateOverrideProp = QuotaConfigs.ProducerByteRateOverrideProp
    val ConsumerByteRateOverrideProp = QuotaConfigs.ConsumerByteRateOverrideProp
    val RequestPercentageOverrideProp = QuotaConfigs.RequestPercentageOverrideProp
    val ControllerMutationOverrideProp = QuotaConfigs.ControllerMutationOverrideProp
    val AcksOverrideProp = ClientConfigs.AcksOverrideProp
    val SessionTimeoutOverrideProp = ClientConfigs.SessionTimeoutOverrideProp
    val HeartbeatIntervalOverrideProp = ClientConfigs.HeartbeatIntervalOverrideProp
    val SupportedConfigsProp = ClientConfigs.SupportedConfigsProp

    // Defaults
    val DefaultProducerOverride = ClientQuotaManagerConfig.QuotaDefault
    val DefaultConsumerOverride = ClientQuotaManagerConfig.QuotaDefault
    val DefaultRequestOverride = ClientRequestQuotaManager.QuotaRequestPercentDefault
    val DefaultControllerMutationOverride = ClientQuotaManagerConfig.QuotaDefault
    val DefaultAcksOverride = "1"
    val DefaultSessionTimeoutOverride = 10000
    val DefaultHeartbeatIntervalOverride = 3000
    val DefaultSupportedConfigs = ""

    // Documentation
    val ProducerOverrideDoc = "A rate representing the upper bound (bytes/sec) for producer traffic."
    val ConsumerOverrideDoc = "A rate representing the upper bound (bytes/sec) for consumer traffic."
    val RequestOverrideDoc = "A percentage representing the upper bound of time spent for processing requests."
    val AcksOverrideDoc = "Number of acknowlegments to be received from replica assignments"
    val SessionTimeoutOverrideDoc = "Consumer group session timeout"
    val HeartbeatIntervalOverrideDoc = "Consumer group heartbeat interval"
    val ControllerMutationOverrideDoc = "The rate at which mutations are accepted for the create topics request, " +
      "the create partitions request and the delete topics request. The rate is accumulated by the number of partitions created or deleted."
    val SupportedConfigsDoc = "Configs supported for each connection"

    // Definitions
    private val clientConfigs = new ConfigDef()
      .define(ProducerByteRateOverrideProp, LONG, DefaultProducerOverride, MEDIUM, ProducerOverrideDoc)
      .define(ConsumerByteRateOverrideProp, LONG, DefaultConsumerOverride, MEDIUM, ConsumerOverrideDoc)
      .define(RequestPercentageOverrideProp, DOUBLE, DefaultRequestOverride, MEDIUM, RequestOverrideDoc)
      .define(ControllerMutationOverrideProp, LONG, DefaultConsumerOverride, MEDIUM, ControllerMutationOverrideDoc)
      .define(AcksOverrideProp, STRING, DefaultAcksOverride, ConfigDef.ValidString.in("all", "-1", "0", "1"), HIGH, AcksOverrideDoc)
      .define(
        SessionTimeoutOverrideProp, 
        INT, 
        DefaultSessionTimeoutOverride, 
        ConfigDef.Range.between(Defaults.GroupMinSessionTimeoutMs, Defaults.GroupMaxSessionTimeoutMs), 
        HIGH, 
        SessionTimeoutOverrideDoc)
      .define(HeartbeatIntervalOverrideProp, INT, DefaultHeartbeatIntervalOverride, HIGH, HeartbeatIntervalOverrideDoc)
      .define(SupportedConfigsProp, LIST, DefaultSupportedConfigs, MEDIUM, SupportedConfigsDoc)

    def configKeys = clientConfigs.configKeys

    def names = clientConfigs.names

    def isQuotaOrDynamicConfig(name: String): Boolean = names.contains(name)

    def typeOf(name: String): Option[ConfigDef.Type] = Option(configKeys.get(name)).map(_.`type`)

    def validate(props: Properties) = DynamicConfig.validate(clientConfigs, props, customPropsAllowed = false)
  }

  object User {
    // Definitions
    private val userConfigs = CredentialProvider.userCredentialConfigs
      .define(Client.ProducerByteRateOverrideProp, LONG, Client.DefaultProducerOverride, MEDIUM, Client.ProducerOverrideDoc)
      .define(Client.ConsumerByteRateOverrideProp, LONG, Client.DefaultConsumerOverride, MEDIUM, Client.ConsumerOverrideDoc)
      .define(Client.RequestPercentageOverrideProp, DOUBLE, Client.DefaultRequestOverride, MEDIUM, Client.RequestOverrideDoc)
      .define(Client.ControllerMutationOverrideProp, DOUBLE, Client.DefaultControllerMutationOverride, MEDIUM, Client.ControllerMutationOverrideDoc)
      .define(Client.AcksOverrideProp, STRING, Client.DefaultAcksOverride, ConfigDef.ValidString.in("all", "-1", "0", "1"), HIGH, Client.AcksOverrideDoc)
      .define(
        Client.SessionTimeoutOverrideProp, 
        INT, 
        Client.DefaultSessionTimeoutOverride, 
        ConfigDef.Range.between(Defaults.GroupMinSessionTimeoutMs, Defaults.GroupMaxSessionTimeoutMs), 
        HIGH, 
        Client.SessionTimeoutOverrideDoc)
      .define(Client.HeartbeatIntervalOverrideProp, INT, Client.DefaultHeartbeatIntervalOverride, HIGH, Client.HeartbeatIntervalOverrideDoc)
      .define(Client.SupportedConfigsProp, LIST, Client.DefaultSupportedConfigs, MEDIUM, Client.SupportedConfigsDoc)

    def configKeys = userConfigs.configKeys

    def names = userConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(userConfigs, props, customPropsAllowed = false)
  }

  private def validate(configDef: ConfigDef, props: Properties, customPropsAllowed: Boolean) = {
    // Validate Names
    val names = configDef.names()
    val propKeys = props.keySet.asScala.map(_.asInstanceOf[String])
    if (!customPropsAllowed) {
      val unknownKeys = propKeys.filter(!names.contains(_))
      require(unknownKeys.isEmpty, s"Unknown Dynamic Configuration: $unknownKeys.")
    }
    val propResolved = DynamicBrokerConfig.resolveVariableConfigs(props)
    // ValidateValues
    configDef.parse(propResolved)
  }
}
