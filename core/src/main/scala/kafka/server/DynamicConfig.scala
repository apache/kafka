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

import kafka.server.DynamicBrokerConfig.AllDynamicConfigs

import java.net.{InetAddress, UnknownHostException}
import java.util.Properties
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.coordinator.group.GroupConfig
import org.apache.kafka.server.config.{QuotaConfig, ZooKeeperInternals}

import java.util
import scala.jdk.CollectionConverters._

/**
  * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
  * and can only be set dynamically.
  */
object DynamicConfig {
    object Broker {
      private val brokerConfigs = {
        val configs = QuotaConfig.brokerQuotaConfigs()

        // Filter and define all dynamic configurations
        KafkaConfig.configKeys
          .filter { case (configName, _) => AllDynamicConfigs.contains(configName) }
          .foreach { case (_, config) => configs.define(config) }
        configs
      }

    // In order to avoid circular reference, all DynamicBrokerConfig's variables which are initialized by `DynamicConfig.Broker` should be moved to `DynamicConfig.Broker`.
    // Otherwise, those variables of DynamicBrokerConfig will see intermediate state of `DynamicConfig.Broker`, because `brokerConfigs` is created by `DynamicBrokerConfig.AllDynamicConfigs`
    val nonDynamicProps: Set[String] = KafkaConfig.configNames.toSet -- brokerConfigs.names.asScala

    def configKeys: util.Map[String, ConfigDef.ConfigKey] = brokerConfigs.configKeys

    def names: util.Set[String] = brokerConfigs.names

    def validate(props: Properties): util.Map[String, AnyRef] = DynamicConfig.validate(brokerConfigs, props, customPropsAllowed = true)
  }

  object Client {
    private val clientConfigs = QuotaConfig.userAndClientQuotaConfigs()

    def configKeys: util.Map[String, ConfigDef.ConfigKey] = clientConfigs.configKeys

    def names: util.Set[String] = clientConfigs.names

    def validate(props: Properties): util.Map[String, AnyRef] = DynamicConfig.validate(clientConfigs, props, customPropsAllowed = false)
  }

  object User {
    private val userConfigs = QuotaConfig.scramMechanismsPlusUserAndClientQuotaConfigs()

    def configKeys: util.Map[String, ConfigDef.ConfigKey] = userConfigs.configKeys

    def names: util.Set[String] = userConfigs.names

    def validate(props: Properties): util.Map[String, AnyRef] = DynamicConfig.validate(userConfigs, props, customPropsAllowed = false)
  }

  object Ip {
    private val ipConfigs = QuotaConfig.ipConfigs

    def configKeys: util.Map[String, ConfigDef.ConfigKey] = ipConfigs.configKeys

    def names: util.Set[String] = ipConfigs.names

    def validate(props: Properties): util.Map[String, AnyRef] = DynamicConfig.validate(ipConfigs, props, customPropsAllowed = false)

    def isValidIpEntity(ip: String): Boolean = {
      if (ip != ZooKeeperInternals.DEFAULT_STRING) {
        try {
          InetAddress.getByName(ip)
        } catch {
          case _: UnknownHostException => return false
        }
      }
      true
    }
  }

  object ClientMetrics {
    private val clientConfigs = org.apache.kafka.server.metrics.ClientMetricsConfigs.configDef()

    def names: util.Set[String] = clientConfigs.names
  }

  object Group {
    private val groupConfigs = GroupConfig.configDef()

    def names: util.Set[String] = groupConfigs.names
  }

  private def validate(configDef: ConfigDef, props: Properties, customPropsAllowed: Boolean) = {
    // Validate Names
    val names = configDef.names
    val propKeys = props.keySet.asScala.map(_.asInstanceOf[String])
    if (!customPropsAllowed) {
      val unknownKeys = propKeys.filterNot(names.contains(_))
      require(unknownKeys.isEmpty, s"Unknown Dynamic Configuration: $unknownKeys.")
    }
    val propResolved = DynamicBrokerConfig.resolveVariableConfigs(props)
    // ValidateValues
    configDef.parse(propResolved)
  }
}
