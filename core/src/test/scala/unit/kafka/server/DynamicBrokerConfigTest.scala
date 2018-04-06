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

import java.util
import java.util.Properties

import kafka.utils.TestUtils
import org.apache.kafka.common.Reconfigurable
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigException, SslConfigs}
import org.easymock.EasyMock
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._
import scala.collection.Set

class DynamicBrokerConfigTest extends JUnitSuite {

  @Test
  def testConfigUpdate(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val oldKeystore = "oldKs.jks"
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, oldKeystore)
    val config = KafkaConfig(props)
    val dynamicConfig = config.dynamicConfig
    assertSame(config, dynamicConfig.currentKafkaConfig)
    assertEquals(oldKeystore, config.values.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
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

      assertEquals(oldKeystore, config.getString(KafkaConfig.SslKeystoreLocationProp))
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

    val validProps = Map(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}" -> "ks.p12")

    val securityPropsWithoutListenerPrefix = Map(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG -> "PKCS12")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, securityPropsWithoutListenerPrefix)
    val nonDynamicProps = Map(KafkaConfig.ZkConnectProp -> "somehost:2181")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, nonDynamicProps)

    // Test update of configs with invalid type
    val invalidProps = Map(KafkaConfig.LogCleanerThreadsProp -> "invalid")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
  }

  @Test
  def testConfigUpdateWithReconfigurableValidationFailure(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(KafkaConfig.LogCleanerDedupeBufferSizeProp, "100000000")
    val config = KafkaConfig(origProps)
    val validProps = Map.empty[String, String]
    val invalidProps = Map(KafkaConfig.LogCleanerThreadsProp -> "20")

    def validateLogCleanerConfig(configs: util.Map[String, _]): Unit = {
      val cleanerThreads = configs.get(KafkaConfig.LogCleanerThreadsProp).toString.toInt
      if (cleanerThreads <=0 || cleanerThreads >= 5)
        throw new ConfigException(s"Invalid cleaner threads $cleanerThreads")
    }
    val reconfigurable = new Reconfigurable {
      override def configure(configs: util.Map[String, _]): Unit = {}
      override def reconfigurableConfigs(): util.Set[String] = Set(KafkaConfig.LogCleanerThreadsProp).asJava
      override def validateReconfiguration(configs: util.Map[String, _]): Unit = validateLogCleanerConfig(configs)
      override def reconfigure(configs: util.Map[String, _]): Unit = {}
    }
    config.dynamicConfig.addReconfigurable(reconfigurable)
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
    config.dynamicConfig.removeReconfigurable(reconfigurable)

    val brokerReconfigurable = new BrokerReconfigurable {
      override def reconfigurableConfigs: collection.Set[String] = Set(KafkaConfig.LogCleanerThreadsProp)
      override def validateReconfiguration(newConfig: KafkaConfig): Unit = validateLogCleanerConfig(newConfig.originals)
      override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {}
    }
    config.dynamicConfig.addBrokerReconfigurable(brokerReconfigurable)
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
  }

  @Test
  def testReconfigurableValidation(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val config = KafkaConfig(origProps)
    val invalidReconfigurableProps = Set(KafkaConfig.LogCleanerThreadsProp, KafkaConfig.BrokerIdProp, "some.prop")
    val validReconfigurableProps = Set(KafkaConfig.LogCleanerThreadsProp, KafkaConfig.LogCleanerDedupeBufferSizeProp, "some.prop")

    def createReconfigurable(configs: Set[String]) = new Reconfigurable {
      override def configure(configs: util.Map[String, _]): Unit = {}
      override def reconfigurableConfigs(): util.Set[String] = configs.asJava
      override def validateReconfiguration(configs: util.Map[String, _]): Unit = {}
      override def reconfigure(configs: util.Map[String, _]): Unit = {}
    }
    intercept[IllegalArgumentException] {
      config.dynamicConfig.addReconfigurable(createReconfigurable(invalidReconfigurableProps))
    }
    config.dynamicConfig.addReconfigurable(createReconfigurable(validReconfigurableProps))

    def createBrokerReconfigurable(configs: Set[String]) = new BrokerReconfigurable {
      override def reconfigurableConfigs: collection.Set[String] = configs
      override def validateReconfiguration(newConfig: KafkaConfig): Unit = {}
      override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {}
    }
    intercept[IllegalArgumentException] {
      config.dynamicConfig.addBrokerReconfigurable(createBrokerReconfigurable(invalidReconfigurableProps))
    }
    config.dynamicConfig.addBrokerReconfigurable(createBrokerReconfigurable(validReconfigurableProps))
  }

  @Test
  def testSecurityConfigs(): Unit = {
    def verifyUpdate(name: String, value: Object): Unit = {
      verifyConfigUpdate(name, value, perBrokerConfig = true, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, perBrokerConfig = true, expectFailure = false)
      verifyConfigUpdate(name, value, perBrokerConfig = false, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, perBrokerConfig = false, expectFailure = true)
    }

    verifyUpdate(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ks.jks")
    verifyUpdate(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
    verifyUpdate(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password")
    verifyUpdate(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password")
  }

  private def verifyConfigUpdate(name: String, value: Object, perBrokerConfig: Boolean, expectFailure: Boolean) {
    val configProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    configProps.put(KafkaConfig.PasswordEncoderSecretProp, "broker.secret")
    val config = KafkaConfig(configProps)
    val props = new Properties
    props.put(name, value)
    val oldValue = config.originals.get(name)

    def updateConfig() = {
      if (perBrokerConfig)
        config.dynamicConfig.updateBrokerConfig(0, config.dynamicConfig.toPersistentProps(props, perBrokerConfig))
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

  private def verifyConfigUpdateWithInvalidConfig(config: KafkaConfig,
                                                  origProps: Properties,
                                                  validProps: Map[String, String],
                                                  invalidProps: Map[String, String]): Unit = {
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

  @Test
  def testPasswordConfigEncryption(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val configWithoutSecret = KafkaConfig(props)
    props.put(KafkaConfig.PasswordEncoderSecretProp, "config-encoder-secret")
    val configWithSecret = KafkaConfig(props)
    val dynamicProps = new Properties
    dynamicProps.put(KafkaConfig.SaslJaasConfigProp, "myLoginModule required;")

    try {
      configWithoutSecret.dynamicConfig.toPersistentProps(dynamicProps, perBrokerConfig = true)
    } catch {
      case e: ConfigException => // expected exception
    }
    val persistedProps = configWithSecret.dynamicConfig.toPersistentProps(dynamicProps, perBrokerConfig = true)
    assertFalse("Password not encoded",
      persistedProps.getProperty(KafkaConfig.SaslJaasConfigProp).contains("myLoginModule"))
    val decodedProps = configWithSecret.dynamicConfig.fromPersistentProps(persistedProps, perBrokerConfig = true)
    assertEquals("myLoginModule required;", decodedProps.getProperty(KafkaConfig.SaslJaasConfigProp))
  }

  @Test
  def testPasswordConfigEncoderSecretChange(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.SaslJaasConfigProp, "staticLoginModule required;")
    props.put(KafkaConfig.PasswordEncoderSecretProp, "config-encoder-secret")
    val config = KafkaConfig(props)
    val dynamicProps = new Properties
    dynamicProps.put(KafkaConfig.SaslJaasConfigProp, "dynamicLoginModule required;")

    val persistedProps = config.dynamicConfig.toPersistentProps(dynamicProps, perBrokerConfig = true)
    assertFalse("Password not encoded",
      persistedProps.getProperty(KafkaConfig.SaslJaasConfigProp).contains("LoginModule"))
    config.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("dynamicLoginModule required;", config.values.get(KafkaConfig.SaslJaasConfigProp).asInstanceOf[Password].value)

    // New config with same secret should use the dynamic password config
    val newConfigWithSameSecret = KafkaConfig(props)
    newConfigWithSameSecret.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("dynamicLoginModule required;", newConfigWithSameSecret.values.get(KafkaConfig.SaslJaasConfigProp).asInstanceOf[Password].value)

    // New config with new secret should use the dynamic password config if new and old secrets are configured in KafkaConfig
    props.put(KafkaConfig.PasswordEncoderSecretProp, "new-encoder-secret")
    props.put(KafkaConfig.PasswordEncoderOldSecretProp, "config-encoder-secret")
    val newConfigWithNewAndOldSecret = KafkaConfig(props)
    newConfigWithNewAndOldSecret.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("dynamicLoginModule required;", newConfigWithSameSecret.values.get(KafkaConfig.SaslJaasConfigProp).asInstanceOf[Password].value)

    // New config with new secret alone should revert to static password config since dynamic config cannot be decoded
    props.put(KafkaConfig.PasswordEncoderSecretProp, "another-new-encoder-secret")
    val newConfigWithNewSecret = KafkaConfig(props)
    newConfigWithNewSecret.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("staticLoginModule required;", newConfigWithNewSecret.values.get(KafkaConfig.SaslJaasConfigProp).asInstanceOf[Password].value)
  }

  @Test
  def testDynamicListenerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    val oldConfig =  KafkaConfig.fromProps(props)
    val kafkaServer = EasyMock.createMock(classOf[kafka.server.KafkaServer])
    EasyMock.expect(kafkaServer.config).andReturn(oldConfig).anyTimes()
    EasyMock.replay(kafkaServer)

    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://hostname:9092,SASL_PLAINTEXT://hostname:9093")
    val newConfig = KafkaConfig(props)

    val dynamicListenerConfig = new DynamicListenerConfig(kafkaServer)
    dynamicListenerConfig.validateReconfiguration(newConfig)
  }

  @Test
  def testSynonyms(): Unit = {
    assertEquals(List("listener.name.secure.ssl.keystore.type", "ssl.keystore.type"),
      DynamicBrokerConfig.brokerConfigSynonyms("listener.name.secure.ssl.keystore.type", matchListenerOverride = true))
    assertEquals(List("listener.name.sasl_ssl.plain.sasl.jaas.config", "sasl.jaas.config"),
      DynamicBrokerConfig.brokerConfigSynonyms("listener.name.sasl_ssl.plain.sasl.jaas.config", matchListenerOverride = true))
    assertEquals(List("some.config"),
      DynamicBrokerConfig.brokerConfigSynonyms("some.config", matchListenerOverride = true))
    assertEquals(List(KafkaConfig.LogRollTimeMillisProp, KafkaConfig.LogRollTimeHoursProp),
      DynamicBrokerConfig.brokerConfigSynonyms(KafkaConfig.LogRollTimeMillisProp, matchListenerOverride = true))
  }

  @Test
  def testDynamicConfigInitializationWithoutConfigsInZK(): Unit = {
    val zkClient = EasyMock.createMock(classOf[kafka.zk.KafkaZkClient])
    EasyMock.expect(zkClient.getEntityConfigs(EasyMock.anyString(), EasyMock.anyString())).andReturn(new java.util.Properties()).anyTimes()
    EasyMock.replay(zkClient)

    val oldConfig =  KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092))
    val dynamicBrokerConfig = new DynamicBrokerConfig(oldConfig)
    dynamicBrokerConfig.initialize(zkClient)
    dynamicBrokerConfig.addBrokerReconfigurable(new TestDynamicThreadPool)

    val newprops = new Properties()
    newprops.put(KafkaConfig.NumIoThreadsProp, "10")
    newprops.put(KafkaConfig.BackgroundThreadsProp, "100")
    dynamicBrokerConfig.updateBrokerConfig(0, newprops)
  }
}

class TestDynamicThreadPool() extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicThreadPool.ReconfigurableConfigs
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    assertEquals(Defaults.NumIoThreads, oldConfig.numIoThreads)
    assertEquals(Defaults.BackgroundThreads, oldConfig.backgroundThreads)

    assertEquals(10, newConfig.numIoThreads)
    assertEquals(100, newConfig.backgroundThreads)
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    assertEquals(10, newConfig.numIoThreads)
    assertEquals(100, newConfig.backgroundThreads)
  }
}