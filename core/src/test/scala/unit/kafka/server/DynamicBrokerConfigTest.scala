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

import java.{lang, util}
import java.util.Properties
import java.util.concurrent.CompletionStage

import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{Endpoint, Reconfigurable}
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigException, SslConfigs}
import org.apache.kafka.server.authorizer._
import org.easymock.EasyMock
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test

import scala.jdk.CollectionConverters._
import scala.collection.Set

class DynamicBrokerConfigTest {

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
    assertThrows(classOf[IllegalArgumentException], () => config.dynamicConfig.addReconfigurable(createReconfigurable(invalidReconfigurableProps)))
    config.dynamicConfig.addReconfigurable(createReconfigurable(validReconfigurableProps))

    def createBrokerReconfigurable(configs: Set[String]) = new BrokerReconfigurable {
      override def reconfigurableConfigs: collection.Set[String] = configs
      override def validateReconfiguration(newConfig: KafkaConfig): Unit = {}
      override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {}
    }
    assertThrows(classOf[IllegalArgumentException], () => config.dynamicConfig.addBrokerReconfigurable(createBrokerReconfigurable(invalidReconfigurableProps)))
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

  @Test
  def testConnectionQuota(): Unit = {
    verifyConfigUpdate(KafkaConfig.MaxConnectionsPerIpProp, "100", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MaxConnectionsPerIpProp, "100", perBrokerConfig = false, expectFailure = false)
    //MaxConnectionsPerIpProp can be set to zero only if MaxConnectionsPerIpOverridesProp property is set
    verifyConfigUpdate(KafkaConfig.MaxConnectionsPerIpProp, "0", perBrokerConfig = false, expectFailure = true)

    verifyConfigUpdate(KafkaConfig.MaxConnectionsPerIpOverridesProp, "hostName1:100,hostName2:0", perBrokerConfig = true,
      expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MaxConnectionsPerIpOverridesProp, "hostName1:100,hostName2:0", perBrokerConfig = false,
      expectFailure = false)
    //test invalid address
    verifyConfigUpdate(KafkaConfig.MaxConnectionsPerIpOverridesProp, "hostName#:100", perBrokerConfig = true,
      expectFailure = true)

    verifyConfigUpdate(KafkaConfig.MaxConnectionsProp, "100", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MaxConnectionsProp, "100", perBrokerConfig = false, expectFailure = false)
    val listenerMaxConnectionsProp = s"listener.name.external.${KafkaConfig.MaxConnectionsProp}"
    verifyConfigUpdate(listenerMaxConnectionsProp, "10", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(listenerMaxConnectionsProp, "10", perBrokerConfig = false, expectFailure = false)
  }

  @Test
  def testConnectionRateQuota(): Unit = {
    verifyConfigUpdate(KafkaConfig.MaxConnectionCreationRateProp, "110", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MaxConnectionCreationRateProp, "120", perBrokerConfig = false, expectFailure = false)
    val listenerMaxConnectionsProp = s"listener.name.external.${KafkaConfig.MaxConnectionCreationRateProp}"
    verifyConfigUpdate(listenerMaxConnectionsProp, "20", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(listenerMaxConnectionsProp, "30", perBrokerConfig = false, expectFailure = false)
  }

  private def verifyConfigUpdate(name: String, value: Object, perBrokerConfig: Boolean, expectFailure: Boolean): Unit = {
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
      assertThrows(classOf[Exception], () => config.dynamicConfig.validate(props, perBrokerConfig))
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
    assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(props, perBrokerConfig = true))

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
    assertFalse(persistedProps.getProperty(KafkaConfig.SaslJaasConfigProp).contains("myLoginModule"),
      "Password not encoded")
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
    assertFalse(persistedProps.getProperty(KafkaConfig.SaslJaasConfigProp).contains("LoginModule"),
      "Password not encoded")
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
    val kafkaServer: KafkaServer = EasyMock.createMock(classOf[kafka.server.KafkaServer])
    EasyMock.expect(kafkaServer.config).andReturn(oldConfig).anyTimes()
    EasyMock.replay(kafkaServer)

    props.put(KafkaConfig.ListenersProp, "PLAINTEXT://hostname:9092,SASL_PLAINTEXT://hostname:9093")
    new DynamicListenerConfig(kafkaServer).validateReconfiguration(KafkaConfig(props))

    // it is illegal to update non-reconfiguable configs of existent listeners
    props.put("listener.name.plaintext.you.should.not.pass", "failure")
    val dynamicListenerConfig = new DynamicListenerConfig(kafkaServer)
    assertThrows(classOf[ConfigException], () => dynamicListenerConfig.validateReconfiguration(KafkaConfig(props)))
  }

  @Test
  def testAuthorizerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    val oldConfig =  KafkaConfig.fromProps(props)
    val kafkaServer: KafkaServer = EasyMock.createMock(classOf[kafka.server.KafkaServer])

    class TestAuthorizer extends Authorizer with Reconfigurable {
      @volatile var superUsers = ""
      override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = Map.empty.asJava
      override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = null
      override def createAcls(requestContext: AuthorizableRequestContext, aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = null
      override def deleteAcls(requestContext: AuthorizableRequestContext, aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = null
      override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = null
      override def close(): Unit = {}
      override def configure(configs: util.Map[String, _]): Unit = {}
      override def reconfigurableConfigs(): util.Set[String] = Set("super.users").asJava
      override def validateReconfiguration(configs: util.Map[String, _]): Unit = {}
      override def reconfigure(configs: util.Map[String, _]): Unit = {
        superUsers = configs.get("super.users").toString
      }
    }

    val authorizer = new TestAuthorizer
    EasyMock.expect(kafkaServer.config).andReturn(oldConfig).anyTimes()
    EasyMock.expect(kafkaServer.authorizer).andReturn(Some(authorizer)).anyTimes()
    EasyMock.replay(kafkaServer)
    // We are only testing authorizer reconfiguration, ignore any exceptions due to incomplete mock
    assertThrows(classOf[Throwable], () => kafkaServer.config.dynamicConfig.addReconfigurables(kafkaServer))
    props.put("super.users", "User:admin")
    kafkaServer.config.dynamicConfig.updateBrokerConfig(0, props)
    assertEquals("User:admin", authorizer.superUsers)
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
    val zkClient: KafkaZkClient = EasyMock.createMock(classOf[KafkaZkClient])
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

  @Test
  def testImproperConfigsAreRemoved(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    val configs = KafkaConfig(props)

    assertEquals(Defaults.MaxConnections, configs.maxConnections)
    assertEquals(Defaults.MessageMaxBytes, configs.messageMaxBytes)

    var newProps = new Properties()
    newProps.put(KafkaConfig.MaxConnectionsProp, "9999")
    newProps.put(KafkaConfig.MessageMaxBytesProp, "2222")

    configs.dynamicConfig.updateDefaultConfig(newProps)
    assertEquals(9999, configs.maxConnections)
    assertEquals(2222, configs.messageMaxBytes)

    newProps = new Properties()
    newProps.put(KafkaConfig.MaxConnectionsProp, "INVALID_INT")
    newProps.put(KafkaConfig.MessageMaxBytesProp, "1111")

    configs.dynamicConfig.updateDefaultConfig(newProps)
    // Invalid value should be skipped and reassigned as default value
    assertEquals(Defaults.MaxConnections, configs.maxConnections)
    // Even if One property is invalid, the below should get correctly updated.
    assertEquals(1111, configs.messageMaxBytes)
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
