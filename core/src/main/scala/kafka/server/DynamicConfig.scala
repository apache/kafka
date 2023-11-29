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
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.server.DynamicConfig.Broker._
import org.apache.kafka.server.DynamicConfig.Client._
import org.apache.kafka.server.DynamicConfig.User._
import org.apache.kafka.server.DynamicConfig.Ip._

import scala.jdk.CollectionConverters._

/**
  * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
  * and can only be set dynamically.
  */
object DynamicConfig {

  object Broker {
    DynamicBrokerConfig.addDynamicConfigs(BROKER_CONFIG_DEF)
    val nonDynamicProps = KafkaConfig.configNames.toSet -- BROKER_CONFIG_DEF.names.asScala

    def validate(props: Properties) = DynamicConfig.validate(BROKER_CONFIG_DEF, props, customPropsAllowed = true)
  }

  object Client {
    def validate(props: Properties) = DynamicConfig.validate(CLIENT_CONFIGS, props, customPropsAllowed = false)
  }

  object User {
    def validate(props: Properties) = DynamicConfig.validate(USER_CONFIGS, props, customPropsAllowed = false)
  }

  object Ip {
    def validate(props: Properties) = DynamicConfig.validate(IP_CONFIGS, props, customPropsAllowed = false)
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
