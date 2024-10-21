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

import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.AdminZkClient
import org.apache.kafka.server.config.{ConfigType, QuotaConfig}

import scala.collection.Seq

object ReplicationQuotaUtils {

  def checkThrottleConfigRemovedFromZK(adminZkClient: AdminZkClient, topic: String, servers: Seq[KafkaServer]): Unit = {
    TestUtils.waitUntilTrue(() => {
      val hasRateProp = servers.forall { server =>
        val brokerConfig = adminZkClient.fetchEntityConfig(ConfigType.BROKER, server.config.brokerId.toString)
        brokerConfig.contains(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG) ||
          brokerConfig.contains(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)
      }
      val topicConfig = adminZkClient.fetchEntityConfig(ConfigType.TOPIC, topic)
      val hasReplicasProp = topicConfig.contains(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG) ||
        topicConfig.contains(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG)
      !hasRateProp && !hasReplicasProp
    }, "Throttle limit/replicas was not unset")
  }

  def checkThrottleConfigAddedToZK(adminZkClient: AdminZkClient, expectedThrottleRate: Long, servers: Seq[KafkaServer], topic: String, throttledLeaders: Set[String], throttledFollowers: Set[String]): Unit = {
    TestUtils.waitUntilTrue(() => {
      //Check for limit in ZK
      val brokerConfigAvailable = servers.forall { server =>
        val configInZk = adminZkClient.fetchEntityConfig(ConfigType.BROKER, server.config.brokerId.toString)
        val zkLeaderRate = configInZk.getProperty(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG)
        val zkFollowerRate = configInZk.getProperty(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG)
        zkLeaderRate != null && expectedThrottleRate == zkLeaderRate.toLong &&
          zkFollowerRate != null && expectedThrottleRate == zkFollowerRate.toLong
      }
      //Check replicas assigned
      val topicConfig = adminZkClient.fetchEntityConfig(ConfigType.TOPIC, topic)
      val leader = topicConfig.getProperty(QuotaConfig.LEADER_REPLICATION_THROTTLED_REPLICAS_CONFIG).split(",").toSet
      val follower = topicConfig.getProperty(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_REPLICAS_CONFIG).split(",").toSet
      val topicConfigAvailable = leader == throttledLeaders && follower == throttledFollowers
      brokerConfigAvailable && topicConfigAvailable
    }, "throttle limit/replicas was not set")
  }
}
