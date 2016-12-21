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
import scala.collection.JavaConverters._

/**
  * Class used to hold dynamic configs. These are configs which have no physical manifestation in the server.properties
  * and can only be set dynamically.
  */
object DynamicConfig {

  object Broker {
    //Properties
    val LeaderReplicationThrottledRateProp = "leader.replication.throttled.rate"
    val FollowerReplicationThrottledRateProp = "follower.replication.throttled.rate"

    //Defaults
    val DefaultReplicationThrottledRate = ReplicationQuotaManagerConfig.QuotaBytesPerSecondDefault

    //Documentation
    val LeaderReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for leaders enumerated in the " +
      s"property ${LogConfig.LeaderReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
      s"limit be kept above 1MB/s for accurate behaviour."
    val FollowerReplicationThrottledRateDoc = "A long representing the upper bound (bytes/sec) on replication traffic for followers enumerated in the " +
      s"property ${LogConfig.FollowerReplicationThrottledReplicasProp} (for each topic). This property can be only set dynamically. It is suggested that the " +
      s"limit be kept above 1MB/s for accurate behaviour."

    //Definitions
    private val brokerConfigDef = new ConfigDef()
      //round minimum value down, to make it easier for users.
      .define(LeaderReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, LeaderReplicationThrottledRateDoc)
      .define(FollowerReplicationThrottledRateProp, LONG, DefaultReplicationThrottledRate, atLeast(0), MEDIUM, FollowerReplicationThrottledRateDoc)

    def names = brokerConfigDef.names

    def validate(props: Properties) = DynamicConfig.validate(brokerConfigDef, props)
  }

  object Client {
    //Properties
    val ProducerByteRateOverrideProp = "producer_byte_rate"
    val ConsumerByteRateOverrideProp = "consumer_byte_rate"

    //Defaults
    val DefaultProducerOverride = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault
    val DefaultConsumerOverride = ClientQuotaManagerConfig.QuotaBytesPerSecondDefault

    //Documentation
    val ProducerOverrideDoc = "A rate representing the upper bound (bytes/sec) for producer traffic."
    val ConsumerOverrideDoc = "A rate representing the upper bound (bytes/sec) for consumer traffic."

    //Definitions
    private val clientConfigs = new ConfigDef()
      .define(ProducerByteRateOverrideProp, LONG, DefaultProducerOverride, MEDIUM, ProducerOverrideDoc)
      .define(ConsumerByteRateOverrideProp, LONG, DefaultConsumerOverride, MEDIUM, ConsumerOverrideDoc)

    def names = clientConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(clientConfigs, props)
  }

  object User {

    //Definitions
    private val userConfigs = CredentialProvider.userCredentialConfigs
      .define(Client.ProducerByteRateOverrideProp, LONG, Client.DefaultProducerOverride, MEDIUM, Client.ProducerOverrideDoc)
      .define(Client.ConsumerByteRateOverrideProp, LONG, Client.DefaultConsumerOverride, MEDIUM, Client.ConsumerOverrideDoc)

    def names = userConfigs.names

    def validate(props: Properties) = DynamicConfig.validate(userConfigs, props)
  }

  private def validate(configDef: ConfigDef, props: Properties) = {
    //Validate Names
    val names = configDef.names()
    props.keys.asScala.foreach { name =>
      require(names.contains(name), s"Unknown Dynamic Configuration '$name'.")
    }
    //ValidateValues
    configDef.parse(props)
  }
}
