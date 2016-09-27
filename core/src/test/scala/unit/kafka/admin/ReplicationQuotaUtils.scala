/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.admin

import kafka.log.LogConfig
import kafka.server.{DynamicConfig, KafkaConfig, ConfigType, KafkaServer}
import kafka.utils.TestUtils

import scala.collection.Seq

object ReplicationQuotaUtils {

  def checkThrottleConfigRemovedFromZK(topic: String, servers: Seq[KafkaServer]): Boolean = {
    TestUtils.waitUntilTrue(() => {
      val brokerReset = servers.forall { server =>
        val brokerConfig = AdminUtils.fetchEntityConfig(server.zkUtils, ConfigType.Broker, server.config.brokerId.toString)
        !brokerConfig.contains(DynamicConfig.Broker.ThrottledReplicationRateLimitProp)
      }
      val topicConfig = AdminUtils.fetchEntityConfig(servers(0).zkUtils, ConfigType.Topic, topic)
      val topicReset = !(topicConfig.contains(LogConfig.LeaderThrottledReplicasListProp)
        || topicConfig.contains(LogConfig.FollowerThrottledReplicasListProp))
      brokerReset && topicReset
    }, "Throttle limit/replicas was not unset")
  }

  def checkThrottleConfigAddedToZK(expectedThrottleRate: Long, servers: Seq[KafkaServer], topic: String, throttledLeaders: String, throttledFollowers: String): Boolean = {
    TestUtils.waitUntilTrue(() => {
      //Check for limit in ZK
      val brokerConfigAvailable = servers.forall { server =>
        val configInZk = AdminUtils.fetchEntityConfig(server.zkUtils, ConfigType.Broker, server.config.brokerId.toString)
        val zkThrottleRate = configInZk.getProperty(DynamicConfig.Broker.ThrottledReplicationRateLimitProp)
        zkThrottleRate != null && expectedThrottleRate == zkThrottleRate.toLong
      }
      //Check replicas assigned
      val topicConfig = AdminUtils.fetchEntityConfig(servers(0).zkUtils, ConfigType.Topic, topic)
      val leader = topicConfig.getProperty(LogConfig.LeaderThrottledReplicasListProp)
      val follower = topicConfig.getProperty(LogConfig.FollowerThrottledReplicasListProp)
      val topicConfigAvailable = (leader == throttledLeaders && follower == throttledFollowers)
      brokerConfigAvailable && topicConfigAvailable
    }, "throttle limit/replicas was not set")
  }
}