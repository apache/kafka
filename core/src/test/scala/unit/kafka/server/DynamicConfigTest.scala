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

import kafka.utils.CoreUtils._
import org.apache.kafka.common.config._
import org.apache.kafka.server.common.AdminOperationException
import org.apache.kafka.server.config.QuotaConfigs
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class DynamicConfigTest extends QuorumTestHarness {
  private final val nonExistentConfig: String = "some.config.that.does.not.exist"
  private final val someValue: String = "some interesting value"

  @Test
  def shouldFailWhenChangingClientIdUnknownConfig(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => adminZkClient.changeClientIdConfig("ClientId",
      propsWith(nonExistentConfig, someValue)))
  }

  @Test
  def shouldFailWhenChangingUserUnknownConfig(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => adminZkClient.changeUserOrUserClientIdConfig("UserId",
      propsWith(nonExistentConfig, someValue)))
  }

  @Test
  def shouldFailLeaderConfigsWithInvalidValues(): Unit = {
    assertThrows(classOf[ConfigException], () => adminZkClient.changeBrokerConfig(Seq(0),
      propsWith(QuotaConfigs.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, "-100")))
  }

  @Test
  def shouldFailFollowerConfigsWithInvalidValues(): Unit = {
    assertThrows(classOf[ConfigException], () => adminZkClient.changeBrokerConfig(Seq(0),
      propsWith(QuotaConfigs.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, "-100")))
  }

  @Test
  def shouldFailIpConfigsWithInvalidValues(): Unit = {
    assertThrows(classOf[ConfigException], () => adminZkClient.changeIpConfig("1.2.3.4",
      propsWith(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "-1")))
  }

  @Test
  def shouldFailIpConfigsWithInvalidIpv4Entity(): Unit = {
    assertThrows(classOf[AdminOperationException], () => adminZkClient.changeIpConfig("1,1.1.1",
      propsWith(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "2")))
  }

  @Test
  def shouldFailIpConfigsWithBadHost(): Unit = {
    assertThrows(classOf[AdminOperationException], () => adminZkClient.changeIpConfig("RFC2606.invalid",
      propsWith(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, "2")))
  }
}
