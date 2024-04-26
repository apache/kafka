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
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance._
import org.apache.kafka.common.config.ConfigDef.Range._
import org.apache.kafka.common.config.ConfigDef.Type._
import org.apache.kafka.server.config.{QuotaConfigs, ZooKeeperInternals}

import java.util
import scala.jdk.CollectionConverters._

/**
  * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
  * and can only be set dynamically.
  */
object DynamicConfig {

  object Broker {
    // Definitions
    val brokerConfigDef = new ConfigDef()
      // Round minimum value down, to make it easier for users.
      .define(QuotaConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, LONG, QuotaConfigs.QUOTA_BYTES_PER_SECOND_DEFAULT, atLeast(0), MEDIUM, QuotaConfigs.LEADER_REPLICATION_THROTTLED_RATE_DOC)
      .define(QuotaConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, LONG, QuotaConfigs.QUOTA_BYTES_PER_SECOND_DEFAULT, atLeast(0), MEDIUM, QuotaConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_DOC)
      .define(QuotaConfigs.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_CONFIG, LONG, QuotaConfigs.QUOTA_BYTES_PER_SECOND_DEFAULT, atLeast(0), MEDIUM, QuotaConfigs.REPLICA_ALTER_LOG_DIRS_IO_MAX_BYTES_PER_SECOND_DOC)
    DynamicBrokerConfig.addDynamicConfigs(brokerConfigDef)
    val nonDynamicProps = KafkaConfig.configNames.toSet -- brokerConfigDef.names.asScala

    def names = brokerConfigDef.names

    def validate(props: Properties) = DynamicConfig.validate(brokerConfigDef, props, customPropsAllowed = true)
  }

  object Client {
    private val clientConfigs = QuotaConfigs.userAndClientQuotaConfigs()

    def configKeys = clientConfigs.configKeys

    def names = clientConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(clientConfigs, props, customPropsAllowed = false)
  }

  object User {
    private val userConfigs = QuotaConfigs.scramMechanismsPlusUserAndClientQuotaConfigs()

    def configKeys = userConfigs.configKeys

    def names = userConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(userConfigs, props, customPropsAllowed = false)
  }

  object Ip {
    private val ipConfigs = QuotaConfigs.ipConfigs()

    def configKeys = ipConfigs.configKeys

    def names = ipConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(ipConfigs, props, customPropsAllowed = false)

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

  private def validate(configDef: ConfigDef, props: Properties, customPropsAllowed: Boolean) = {
    // Validate Names
    val names = configDef.names()
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
