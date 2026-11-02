/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.admin

import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.errors.InvalidRequestException
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.nio.file.Files

@Tag("integration")
class PlaintextAlterConfigsIntegrationTest extends BaseAlterConfigsIntegrationTest {

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProviders(quorum: String): Unit = {
    checkAlterConfigs(Map(), classOf[InvalidRequestException], "token", placeholder = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersAndValidatorRejection(quorum: String): Unit = {
    Files.write(file, ("key=" + Long.MaxValue).getBytes(StandardCharsets.UTF_8))
    checkAlterConfigs(Map(), classOf[InvalidRequestException], String.valueOf(Long.MaxValue), placeholder = true)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsRespectsConfigProviderAllowlist(quorum: String): Unit = {
    val previous = System.getProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY)
    try {
      System.setProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY, "none")
      checkAlterConfigs(Map(), classOf[InvalidRequestException], "token", false)
    } finally {
      if (previous == null) System.clearProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY)
      else System.setProperty(AbstractConfig.AUTOMATIC_CONFIG_PROVIDERS_PROPERTY, previous)
    }
  }
}
