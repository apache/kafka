/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigException, SslConfigs}
import org.junit.Assert._
import org.junit.Test

class DynamicBrokerConfigTest {

  @Test
  def testConfigUpdate(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val oldKeystore = "oldKs.jks"
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, oldKeystore)
    val config = KafkaConfig(props)
    val dynamicConfig = config.dynamicConfig
    assertSame(config, dynamicConfig.config)
    assertSame(config, config.currentConfig)
    assertEquals(oldKeystore, config.sslKeystoreLocation)
    assertEquals(oldKeystore,
      config.valuesFromThisConfigWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    assertEquals(oldKeystore, config.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

    (1 to 2).foreach { i =>
      val props1 = new Properties
      val newKeystore = s"ks$i.jks"
      props1.put(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}", newKeystore)
      dynamicConfig.updateBrokerConfig(0, props1)
      assertNotSame(config, config.currentConfig)

      assertEquals(newKeystore,
        config.valuesWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.originalsWithPrefix("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.currentConfig.valuesWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.currentConfig.currentConfig.originalsWithPrefix("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

      assertEquals(oldKeystore, config.sslKeystoreLocation)
      assertEquals(oldKeystore, config.originals.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.values.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.originalsStrings.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.currentConfig.sslKeystoreLocation)
      assertEquals(oldKeystore, config.currentConfig.originals.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.currentConfig.values.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.currentConfig.originalsStrings.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

      assertEquals(oldKeystore,
        config.valuesFromThisConfigWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.valuesFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.currentConfig.valuesFromThisConfigWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.currentConfig.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.currentConfig.valuesFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    }
  }

  @Test
  def testSecurityConfigs(): Unit = {
    def verifyUpdate(name: String, value: Object, invalidValue: Boolean): Unit = {
      verifyConfigUpdate(name, value, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, expectFailure = invalidValue)
    }

    verifyUpdate(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ks.jks", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, 1.asInstanceOf[Integer], invalidValue = true)
    verifyUpdate(SslConfigs.SSL_KEY_PASSWORD_CONFIG, 1.asInstanceOf[Integer], invalidValue = true)
  }

  private def verifyConfigUpdate(name: String, value: Object, expectFailure: Boolean) {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val config = KafkaConfig(props)
    val props1 = new Properties
    props1.put(name, value)
    try {
      config.dynamicConfig.updateBrokerConfig(0, props1)
      assertFalse("Invalid update did not fail", expectFailure)
    } catch {
      case e: Exception => assertTrue(s"Unexpected exception $e", expectFailure)
    }
  }
}
