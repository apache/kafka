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
    assertSame(config, dynamicConfig.currentKafkaConfig)
    assertEquals(oldKeystore, config.sslKeystoreLocation)
    assertEquals(oldKeystore,
      config.valuesFromThisConfigWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    assertEquals(oldKeystore, config.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

    (1 to 2).foreach { i =>
      val props1 = new Properties
      val newKeystore = s"ks$i.jks"
      props1.put(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}", newKeystore)
      dynamicConfig.updateBrokerConfig(0, props1)
      assertNotSame(config, dynamicConfig.currentKafkaConfig)

      assertEquals(newKeystore,
        config.valuesWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.originalsWithPrefix("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.valuesWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.originalsWithPrefix("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

      assertEquals(oldKeystore, config.sslKeystoreLocation)
      assertEquals(oldKeystore, config.originals.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.values.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.originalsStrings.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

      assertEquals(oldKeystore,
        config.valuesFromThisConfigWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.valuesFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(oldKeystore, config.valuesFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    }
  }

  @Test
  def testConfigUpdateWithSomeInvalidConfigs(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
    val config = KafkaConfig(origProps)

    def verifyConfigUpdateWithInvalidConfig(validProps: Map[String, String], invalidProps: Map[String, String]): Unit = {
      val props = new Properties
      validProps.foreach { case (k, v) => props.put(k, v) }
      invalidProps.foreach { case (k, v) => props.put(k, v) }

      // DynamicBrokerConfig#validate is used by AdminClient to validate the configs provided in
      // in an AlterConfigs request. Validation should fail with an exception if any of the configs are invalid.
      try {
        config.dynamicConfig.validate(props, perBrokerConfig = true)
        fail("Invalid config did not fail validation")
      } catch {
        case e: ConfigException => // expected exception
      }

      // DynamicBrokerConfig#updateBrokerConfig is used to update configs from ZooKeeper during
      // startup and when configs are updated in ZK. Update should apply valid configs and ignore
      // invalid ones.
      config.dynamicConfig.updateBrokerConfig(0, props)
      validProps.foreach { case (name, value) => assertEquals(value, config.originals.get(name)) }
      invalidProps.keySet.foreach { name =>
        assertEquals(origProps.get(name), config.originals.get(name))
      }
    }

    val validProps = Map(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}" ->"ks.p12")
    val securityPropsWithoutListenerPrefix = Map(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG -> "PKCS12")
    verifyConfigUpdateWithInvalidConfig(validProps, securityPropsWithoutListenerPrefix)
    val nonDynamicProps = Map(KafkaConfig.ZkConnectProp -> "somehost:2181")
    verifyConfigUpdateWithInvalidConfig(validProps, nonDynamicProps)
  }

  @Test
  def testSecurityConfigs(): Unit = {
    def verifyUpdate(name: String, value: Object, invalidValue: Boolean): Unit = {
      verifyConfigUpdate(name, value, perBrokerConfig = true, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, perBrokerConfig = true, expectFailure = invalidValue)
      verifyConfigUpdate(name, value, perBrokerConfig = false, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, perBrokerConfig = false, expectFailure = true)
    }

    verifyUpdate(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ks.jks", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password", invalidValue = false)
    verifyUpdate(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, 1.asInstanceOf[Integer], invalidValue = true)
    verifyUpdate(SslConfigs.SSL_KEY_PASSWORD_CONFIG, 1.asInstanceOf[Integer], invalidValue = true)
  }

  private def verifyConfigUpdate(name: String, value: Object, perBrokerConfig: Boolean, expectFailure: Boolean) {
    val config = KafkaConfig(TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181))
    val props = new Properties
    props.put(name, value)
    val oldValue = config.originals.get(name)

    def updateConfig() = {
      if (perBrokerConfig)
        config.dynamicConfig.updateBrokerConfig(0, props)
      else
        config.dynamicConfig.updateDefaultConfig(props)
    }
    if (!expectFailure) {
      config.dynamicConfig.validate(props, perBrokerConfig)
      updateConfig()
      assertEquals(value, config.originals.get(name))
    } else {
      try {
        config.dynamicConfig.validate(props, perBrokerConfig)
        fail("Invalid config did not fail validation")
      } catch {
        case e: Exception => // expected exception
      }
      updateConfig()
      assertEquals(oldValue, config.originals.get(name))
    }
  }
}
