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

import java.net.{InetAddress, UnknownHostException}
import java.util.Properties

import kafka.log.LogConfig
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance._
import org.apache.kafka.common.config.ConfigDef.Range._
import org.apache.kafka.common.config.ConfigDef.Type._

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
    def isClientOrUserQuotaConfig(name: String): Boolean = org.apache.kafka.common.config.internals.QuotaConfigs.isClientOrUserConfig(name)
  }

  object Client {
    private val clientConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.clientConfigs()

    def configKeys = clientConfigs.configKeys

    def names = clientConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(clientConfigs, props, customPropsAllowed = false)
  }

  object User {
    private val userConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.userConfigs()

    def configKeys = userConfigs.configKeys

    def names = userConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(userConfigs, props, customPropsAllowed = false)
  }

  object Ip {
    private val ipConfigs = org.apache.kafka.common.config.internals.QuotaConfigs.ipConfigs()

    def configKeys = ipConfigs.configKeys

    def names = ipConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(ipConfigs, props, customPropsAllowed = false)

    def isValidIpEntity(ip: String): Boolean = {
      if (ip != ConfigEntityName.Default) {
        try {
          InetAddress.getByName(ip)
        } catch {
          case _: UnknownHostException => return false
        }
      }
      true
    }
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
