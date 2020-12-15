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

import kafka.admin.AdminOperationException
import kafka.utils.CoreUtils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.config._
import org.junit.Test

class DynamicConfigTest extends ZooKeeperTestHarness {
  private final val nonExistentConfig: String = "some.config.that.does.not.exist"
  private final val someValue: String = "some interesting value"

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailWhenChangingClientIdUnknownConfig(): Unit = {
    adminZkClient.changeClientIdConfig("ClientId", propsWith(nonExistentConfig, someValue))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def shouldFailWhenChangingUserUnknownConfig(): Unit = {
    adminZkClient.changeUserOrUserClientIdConfig("UserId", propsWith(nonExistentConfig, someValue))
  }

  @Test(expected = classOf[ConfigException])
  def shouldFailLeaderConfigsWithInvalidValues(): Unit = {
    adminZkClient.changeBrokerConfig(Seq(0),
      propsWith(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, "-100"))
  }

  @Test(expected = classOf[ConfigException])
  def shouldFailFollowerConfigsWithInvalidValues(): Unit = {
    adminZkClient.changeBrokerConfig(Seq(0),
      propsWith(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, "-100"))
  }

  @Test(expected = classOf[ConfigException])
  def shouldFailIpConfigsWithInvalidValues(): Unit = {
    adminZkClient.changeIpConfig("1.2.3.4", propsWith(DynamicConfig.Ip.IpConnectionRateOverrideProp, "-1"))
  }

  @Test(expected = classOf[AdminOperationException])
  def shouldFailIpConfigsWithInvalidIpv4Entity(): Unit = {
    adminZkClient.changeIpConfig("1,1.1.1", propsWith(DynamicConfig.Ip.IpConnectionRateOverrideProp, "2"));
  }

  @Test(expected = classOf[AdminOperationException])
  def shouldFailIpConfigsWithBadHost(): Unit = {
    adminZkClient.changeIpConfig("ip", propsWith(DynamicConfig.Ip.IpConnectionRateOverrideProp, "2"));
  }
}
