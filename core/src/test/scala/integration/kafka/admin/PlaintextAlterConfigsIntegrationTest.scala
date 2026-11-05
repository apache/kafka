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

import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.config.{AbstractConfig, ConfigResource}
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.server.config.{ServerConfigs, ServerLogConfigs}
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters._

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

  // Non-STRING configs resolved through a config provider must be applied by the broker, not only accepted
  // by the request validation (KAFKA-20973).

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersForPerBrokerIntConfig(quorum: String): Unit = {
    checkAlterConfigsApplied(brokerResource, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, ServerConfigs.NUM_IO_THREADS_CONFIG, "10")
    assertEquals(10, brokerConfig.getInt(ServerConfigs.NUM_IO_THREADS_CONFIG))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersForClusterWideIntConfig(quorum: String): Unit = {
    checkAlterConfigsApplied(new ConfigResource(ConfigResource.Type.BROKER, ""), ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG, ServerConfigs.NUM_IO_THREADS_CONFIG, "10")
    assertEquals(10, brokerConfig.getInt(ServerConfigs.NUM_IO_THREADS_CONFIG))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersForLongConfig(quorum: String): Unit = {
    checkAlterConfigsApplied(brokerResource, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "86400000")
    assertEquals(86400000L, brokerConfig.getLong(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersForDoubleConfig(quorum: String): Unit = {
    checkAlterConfigsApplied(brokerResource, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP, "0.75")
    assertEquals(0.75, brokerConfig.getDouble(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersForBooleanConfig(quorum: String): Unit = {
    checkAlterConfigsApplied(brokerResource, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG, "true")
    assertTrue(brokerConfig.getBoolean(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testAlterConfigsWithConfigProvidersForListConfig(quorum: String): Unit = {
    checkAlterConfigsApplied(brokerResource, ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG, ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, "compact,delete")
    assertEquals(List("compact", "delete").asJava, brokerConfig.getList(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG))
  }
}
