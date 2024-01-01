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
import java.util.{Collections, Optional, Properties, Map => JMap}
import java.util.concurrent.CompletionStage
import java.util.concurrent.atomic.AtomicReference
import kafka.controller.KafkaController
import kafka.log.LogManager
import kafka.log.remote.RemoteLogManager
import kafka.network.{DataPlaneAcceptor, SocketServer}
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.{Endpoint, Reconfigurable}
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.{ConfigException, SslConfigs}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.network.{SocketServer => JSocketServer}
import org.apache.kafka.server.config.DynamicBrokerConfigManager.{BrokerReconfigurable, JDynamicThreadPool}
import org.apache.kafka.server.config.{Defaults, KafkaConfig}
import org.apache.kafka.storage.internals.log.{LogConfig, ProducerStateManagerConfig}
import org.apache.kafka.test.MockMetricsReporter
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.{mock, when}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.Set

class DynamicBrokerConfigTest {

  @Test
  def testConfigUpdate(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val oldKeystore = "oldKs.jks"
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, oldKeystore)
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")
    val config = KafkaConfigProvider.fromProps(props)
    val dynamicConfig = config.dynamicConfig
    dynamicConfig.initialize(Optional.empty(), Optional.empty())

    assertEquals(config, dynamicConfig.currentKafkaConfig)
    assertEquals(oldKeystore, config.values.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    assertEquals(oldKeystore,
      config.valuesFromThisConfigWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    assertEquals(oldKeystore, config.originalsFromThisConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

    (1 to 2).foreach { i =>
      val props1 = new Properties
      val newKeystore = s"ks$i.jks"
      props1.put(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}", newKeystore)
      dynamicConfig.updateBrokerConfig(0, props1, true)
      assertNotSame(config, dynamicConfig.currentKafkaConfig)

      assertEquals(newKeystore,
        config.valuesWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.originalsWithPrefix("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.valuesWithPrefixOverride("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      assertEquals(newKeystore,
        config.originalsWithPrefix("listener.name.external.").get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))

      assertEquals(oldKeystore, config.getString(KafkaConfig.SSL_KEYSTORE_LOCATION_PROP))
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
  def testEnableDefaultUncleanLeaderElection(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(KafkaConfig.UNCLEAN_LEADER_ELECTION_ENABLE_PROP, "false")
    origProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")

    val config = KafkaConfigProvider.fromProps(origProps)
    val serverMock = Mockito.mock(classOf[KafkaServer])
    val controllerMock = Mockito.mock(classOf[KafkaController])
    val logManagerMock = Mockito.mock(classOf[LogManager])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.kafkaController).thenReturn(controllerMock)
    Mockito.when(serverMock.logManager).thenReturn(logManagerMock)
    Mockito.when(logManagerMock.allLogs).thenReturn(Iterable.empty)

    val currentDefaultLogConfig = new AtomicReference(new LogConfig(new Properties))
    Mockito.when(logManagerMock.currentDefaultConfig).thenAnswer(_ => currentDefaultLogConfig.get())
    Mockito.when(logManagerMock.reconfigureDefaultLogConfig(ArgumentMatchers.any(classOf[LogConfig])))
      .thenAnswer(invocation => currentDefaultLogConfig.set(invocation.getArgument(0)))

    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    config.dynamicConfig.addBrokerReconfigurable(new DynamicLogConfig(logManagerMock, serverMock))

    val props = new Properties()

    props.put(KafkaConfig.UNCLEAN_LEADER_ELECTION_ENABLE_PROP, "true")
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertTrue(config.uncleanLeaderElectionEnable)
    Mockito.verify(controllerMock).enableDefaultUncleanLeaderElection()
  }

  @Test
  def testUpdateDynamicThreadPool(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(KafkaConfig.NUM_IO_THREADS_PROP, "4")
    origProps.put(KafkaConfig.NUM_NETWORK_THREADS_PROP, "2")
    origProps.put(KafkaConfig.NUM_REPLICA_FETCHERS_PROP, "1")
    origProps.put(KafkaConfig.NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP, "1")
    origProps.put(KafkaConfig.BACKGROUND_THREADS_PROP, "3")
    origProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")

    val config = KafkaConfigProvider.fromProps(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val acceptorMock = Mockito.mock(classOf[DataPlaneAcceptor])
    val handlerPoolMock = Mockito.mock(classOf[KafkaRequestHandlerPool])
    val socketServerMock = Mockito.mock(classOf[SocketServer])
    val replicaManagerMock = Mockito.mock(classOf[ReplicaManager])
    val logManagerMock = Mockito.mock(classOf[LogManager])
    val schedulerMock = Mockito.mock(classOf[KafkaScheduler])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.dataPlaneRequestHandlerPool).thenReturn(handlerPoolMock)
    Mockito.when(acceptorMock.listenerName()).thenReturn(new ListenerName("plaintext"))
    Mockito.when(acceptorMock.reconfigurableConfigs()).thenCallRealMethod()
    Mockito.when(serverMock.socketServer).thenReturn(socketServerMock)
    Mockito.when(socketServerMock.dataPlaneAcceptor(anyString())).thenReturn(Some(acceptorMock))
    Mockito.when(serverMock.replicaManager).thenReturn(replicaManagerMock)
    Mockito.when(serverMock.logManager).thenReturn(logManagerMock)
    Mockito.when(serverMock.kafkaScheduler).thenReturn(schedulerMock)

    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    config.dynamicConfig.addBrokerReconfigurable(new BrokerDynamicThreadPool(serverMock))
    config.dynamicConfig.addReconfigurable(acceptorMock)

    val props = new Properties()
    props.put(KafkaConfig.NUM_IO_THREADS_PROP, "8")
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")

    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(8, config.numIoThreads)
    Mockito.verify(handlerPoolMock).resizeThreadPool(newSize = 8)

    props.put(KafkaConfig.NUM_NETWORK_THREADS_PROP, "4")
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(4, config.numNetworkThreads)
    val captor: ArgumentCaptor[JMap[String, String]] = ArgumentCaptor.forClass(classOf[JMap[String, String]])
    Mockito.verify(acceptorMock).reconfigure(captor.capture())
    assertTrue(captor.getValue.containsKey(KafkaConfig.NUM_NETWORK_THREADS_PROP))
    assertEquals(4, captor.getValue.get(KafkaConfig.NUM_NETWORK_THREADS_PROP))

    props.put(KafkaConfig.NUM_REPLICA_FETCHERS_PROP, "2")
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(2, config.numReplicaFetchers)
    Mockito.verify(replicaManagerMock).resizeFetcherThreadPool(newSize = 2)

    props.put(KafkaConfig.NUM_RECOVERY_THREADS_PER_DATA_DIR_PROP, "2")
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(2, config.numRecoveryThreadsPerDataDir)
    Mockito.verify(logManagerMock).resizeRecoveryThreadPool(newSize = 2)

    props.put(KafkaConfig.BACKGROUND_THREADS_PROP, "6")
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(6, config.backgroundThreads)
    Mockito.verify(schedulerMock).resizeThreadPool(6)

    Mockito.verifyNoMoreInteractions(
      handlerPoolMock,
      socketServerMock,
      replicaManagerMock,
      logManagerMock,
      schedulerMock
    )
  }

  @nowarn("cat=deprecation")
  @Test
  def testConfigUpdateWithSomeInvalidConfigs(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
    origProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")

    val config = KafkaConfigProvider.fromProps(origProps)
    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())

    val validProps = Map(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}" -> "ks.p12")

    val securityPropsWithoutListenerPrefix = Map(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG -> "PKCS12")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, securityPropsWithoutListenerPrefix)
    val nonDynamicProps = Map(KafkaConfig.ZK_CONNECT_PROP -> "somehost:2181")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, nonDynamicProps)

    // Test update of configs with invalid type
    val invalidProps = Map(KafkaConfig.LOG_CLEANER_THREADS_PROP -> "invalid")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)

    val excludedTopicConfig = Map(KafkaConfig.LOG_MESSAGE_FORMAT_VERSION_PROP -> "0.10.2")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, excludedTopicConfig)
  }

  @Test
  def testConfigUpdateWithReconfigurableValidationFailure(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "100000000")
    val config = KafkaConfigProvider.fromProps(origProps)
    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())

    val validProps = Map.empty[String, String]
    val invalidProps = Map(KafkaConfig.LOG_CLEANER_THREADS_PROP -> "20")

    def validateLogCleanerConfig(configs: util.Map[String, _]): Unit = {
      val cleanerThreads = configs.get(KafkaConfig.LOG_CLEANER_THREADS_PROP).toString.toInt
      if (cleanerThreads <=0 || cleanerThreads >= 5)
        throw new ConfigException(s"Invalid cleaner threads $cleanerThreads")
    }
    val reconfigurable = new Reconfigurable {
      override def configure(configs: util.Map[String, _]): Unit = {}
      override def reconfigurableConfigs(): util.Set[String] = Set(KafkaConfig.LOG_CLEANER_THREADS_PROP).asJava
      override def validateReconfiguration(configs: util.Map[String, _]): Unit = validateLogCleanerConfig(configs)
      override def reconfigure(configs: util.Map[String, _]): Unit = {}
    }
    config.dynamicConfig.addReconfigurable(reconfigurable)
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
    config.dynamicConfig.removeReconfigurable(reconfigurable)

    val brokerReconfigurable = new BrokerReconfigurable {
      override def reconfigurableConfigs: util.Set[String] = Collections.singleton(KafkaConfig.LOG_CLEANER_THREADS_PROP)
      override def validateReconfiguration(newConfig: KafkaConfig): Unit = validateLogCleanerConfig(newConfig.originals)
      override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {}
    }
    config.dynamicConfig.addBrokerReconfigurable(brokerReconfigurable)
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
  }

  @Test
  def testReconfigurableValidation(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val config = KafkaConfigProvider.fromProps(origProps)
    val invalidReconfigurableProps = Set(KafkaConfig.LOG_CLEANER_THREADS_PROP, KafkaConfig.BROKER_ID_PROP, "some.prop")
    val validReconfigurableProps = Set(KafkaConfig.LOG_CLEANER_THREADS_PROP, KafkaConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "some.prop")

    def createReconfigurable(configs: Set[String]) = new Reconfigurable {
      override def configure(configs: util.Map[String, _]): Unit = {}
      override def reconfigurableConfigs(): util.Set[String] = configs.asJava
      override def validateReconfiguration(configs: util.Map[String, _]): Unit = {}
      override def reconfigure(configs: util.Map[String, _]): Unit = {}
    }
    assertThrows(classOf[IllegalArgumentException], () => config.dynamicConfig.addReconfigurable(createReconfigurable(invalidReconfigurableProps)))
    config.dynamicConfig.addReconfigurable(createReconfigurable(validReconfigurableProps))

    def createBrokerReconfigurable(configs: util.Set[String]) = new BrokerReconfigurable {
      override def reconfigurableConfigs: util.Set[String] = configs
      override def validateReconfiguration(newConfig: KafkaConfig): Unit = {}
      override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {}
    }
    assertThrows(classOf[IllegalArgumentException], () => config.dynamicConfig.addBrokerReconfigurable(createBrokerReconfigurable(invalidReconfigurableProps.asJava)))
    config.dynamicConfig.addBrokerReconfigurable(createBrokerReconfigurable(validReconfigurableProps.asJava))
  }

  @Test
  def testSecurityConfigs(): Unit = {
    def verifyUpdate(name: String, value: Object): Unit = {
      verifyConfigUpdate(name, value, true, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, true, expectFailure = false)
      verifyConfigUpdate(name, value, false, expectFailure = true)
      verifyConfigUpdate(s"listener.name.external.$name", value, false, expectFailure = true)
    }

    verifyUpdate(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "ks.jks")
    verifyUpdate(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
    verifyUpdate(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "password")
    verifyUpdate(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "password")
  }

  @Test
  def testConnectionQuota(): Unit = {
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "100", true, expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "100", false, expectFailure = false)
    //MAX_CONNECTIONS_PER_IP_PROP can be set to zero only if MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP property is set
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "0", false, expectFailure = true)

    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, "hostName1:100,hostName2:0", true,
      expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, "hostName1:100,hostName2:0", false,
      expectFailure = false)
    //test invalid address
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_OVERRIDES_PROP, "hostName#:100", true,
      expectFailure = true)

    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "100", true, expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "100", false, expectFailure = false)
    val listenerMaxConnectionsProp = s"listener.name.external.${KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP}"
    verifyConfigUpdate(listenerMaxConnectionsProp, "10", true, expectFailure = false)
    verifyConfigUpdate(listenerMaxConnectionsProp, "10", false, expectFailure = false)
  }

  @Test
  def testConnectionRateQuota(): Unit = {
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTION_CREATION_RATE_PROP, "110", true, expectFailure = false)
    verifyConfigUpdate(KafkaConfig.MAX_CONNECTION_CREATION_RATE_PROP, "120", false, expectFailure = false)
    val listenerMaxConnectionsProp = s"listener.name.external.${KafkaConfig.MAX_CONNECTION_CREATION_RATE_PROP}"
    verifyConfigUpdate(listenerMaxConnectionsProp, "20", true, expectFailure = false)
    verifyConfigUpdate(listenerMaxConnectionsProp, "30", false, expectFailure = false)
  }

  private def verifyConfigUpdate(name: String, value: Object, perBrokerConfig: Boolean, expectFailure: Boolean): Unit = {
    val configProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    configProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")
    val config = KafkaConfigProvider.fromProps(configProps)
    config.dynamicConfig().initialize(Optional.empty(), Optional.empty())

    val props = new Properties
    props.put(name, value)
    val oldValue = config.originals.get(name)

    def updateConfig(): Unit = {
      if (perBrokerConfig)
        config.dynamicConfig.updateBrokerConfig(0, config.dynamicConfig.toPersistentProps(props, perBrokerConfig), true)
      else
        config.dynamicConfig.updateDefaultConfig(props, true)
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
    assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(props, true))

    // DynamicBrokerConfig#updateBrokerConfig is used to update configs from ZooKeeper during
    // startup and when configs are updated in ZK. Update should apply valid configs and ignore
    // invalid ones.
    config.dynamicConfig.updateBrokerConfig(0, props, true)
    validProps.foreach { case (name, value) => assertEquals(value, config.originals.get(name)) }
    invalidProps.keySet.foreach { name =>
      assertEquals(origProps.get(name), config.originals.get(name))
    }
  }

  @Test
  def testPasswordConfigEncryption(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val configWithoutSecret = KafkaConfigProvider.fromProps(props)
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "config-encoder-secret")
    val configWithSecret = KafkaConfigProvider.fromProps(props)
    val dynamicProps = new Properties
    dynamicProps.put(KafkaConfig.SASL_JAAS_CONFIG_PROP, "myLoginModule required;")

    try {
      configWithoutSecret.dynamicConfig.toPersistentProps(dynamicProps, true)
    } catch {
      case _: ConfigException => // expected exception
    }
    val persistedProps = configWithSecret.dynamicConfig.toPersistentProps(dynamicProps, true)
    assertFalse(persistedProps.getProperty(KafkaConfig.SASL_JAAS_CONFIG_PROP).contains("myLoginModule"),
      "Password not encoded")
    val decodedProps = configWithSecret.dynamicConfig.fromPersistentProps(persistedProps, true)
    assertEquals("myLoginModule required;", decodedProps.getProperty(KafkaConfig.SASL_JAAS_CONFIG_PROP))
  }

  @Test
  def testPasswordConfigEncoderSecretChange(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.SASL_JAAS_CONFIG_PROP, "staticLoginModule required;")
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "config-encoder-secret")
    val config = KafkaConfigProvider.fromProps(props)
    config.dynamicConfig().initialize(Optional.empty(), Optional.empty())
    val dynamicProps = new Properties
    dynamicProps.put(KafkaConfig.SASL_JAAS_CONFIG_PROP, "dynamicLoginModule required;")

    val persistedProps = config.dynamicConfig.toPersistentProps(dynamicProps, true)
    assertFalse(persistedProps.getProperty(KafkaConfig.SASL_JAAS_CONFIG_PROP).contains("LoginModule"),
      "Password not encoded")
    config.dynamicConfig().updateBrokerConfig(0, persistedProps, true)
    assertEquals("dynamicLoginModule required;", config.values.get(KafkaConfig.SASL_JAAS_CONFIG_PROP).asInstanceOf[Password].value)

    // New config with same secret should use the dynamic password config
    val newConfigWithSameSecret = KafkaConfigProvider.fromProps(props)
    newConfigWithSameSecret.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    newConfigWithSameSecret.dynamicConfig.updateBrokerConfig(0, persistedProps, true)
    assertEquals("dynamicLoginModule required;", newConfigWithSameSecret.values.get(KafkaConfig.SASL_JAAS_CONFIG_PROP).asInstanceOf[Password].value)

    // New config with new secret should use the dynamic password config if new and old secrets are configured in KafkaConfig
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "new-encoder-secret")
    props.put(KafkaConfig.PASSWORD_ENCODER_OLD_SECRET_PROP, "config-encoder-secret")
    val newConfigWithNewAndOldSecret = KafkaConfigProvider.fromProps(props)
    newConfigWithNewAndOldSecret.dynamicConfig.updateBrokerConfig(0, persistedProps, true)
    assertEquals("dynamicLoginModule required;", newConfigWithSameSecret.values.get(KafkaConfig.SASL_JAAS_CONFIG_PROP).asInstanceOf[Password].value)

    // New config with new secret alone should revert to static password config since dynamic config cannot be decoded
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "another-new-encoder-secret")
    val newConfigWithNewSecret = KafkaConfigProvider.fromProps(props)
    newConfigWithNewSecret.dynamicConfig.updateBrokerConfig(0, persistedProps, true)
    assertEquals("staticLoginModule required;", newConfigWithNewSecret.values.get(KafkaConfig.SASL_JAAS_CONFIG_PROP).asInstanceOf[Password].value)
  }

  @Test
  def testDynamicListenerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    val oldConfig =  KafkaConfigProvider.fromProps(props)
    val kafkaServer: KafkaServer = mock(classOf[kafka.server.KafkaServer])
    when(kafkaServer.config).thenReturn(oldConfig)

    props.put(KafkaConfig.LISTENERS_PROP, "PLAINTEXT://hostname:9092,SASL_PLAINTEXT://hostname:9093")
    new DynamicListenerConfig(kafkaServer).validateReconfiguration(KafkaConfigProvider.fromProps(props))

    // it is illegal to update non-reconfiguable configs of existent listeners
    props.put("listener.name.plaintext.you.should.not.pass", "failure")
    val dynamicListenerConfig = new DynamicListenerConfig(kafkaServer)
    assertThrows(classOf[ConfigException], () => dynamicListenerConfig.validateReconfiguration(KafkaConfigProvider.fromProps(props)))
  }

  class TestAuthorizer extends Authorizer with Reconfigurable {
    @volatile var superUsers = ""

    override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = Collections.emptyMap()

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

  @Test
  def testAuthorizerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    val oldConfig =  KafkaConfigProvider.fromProps(props)
    oldConfig.dynamicConfig.initialize(Optional.empty(), Optional.empty())

    val kafkaServer: KafkaServer = mock(classOf[kafka.server.KafkaServer])
    when(kafkaServer.config).thenReturn(oldConfig)
    when(kafkaServer.kafkaYammerMetrics).thenReturn(KafkaYammerMetrics.INSTANCE)
    val metrics: Metrics = mock(classOf[Metrics])
    when(kafkaServer.metrics).thenReturn(metrics)
    val quotaManagers: QuotaFactory.QuotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    when(quotaManagers.clientQuotaCallback).thenReturn(None)
    when(kafkaServer.quotaManagers).thenReturn(quotaManagers)
    val socketServer: SocketServer = mock(classOf[SocketServer])
    when(socketServer.reconfigurableConfigs).thenReturn(JSocketServer.RECONFIGURABLE_CONFIGS)
    when(kafkaServer.socketServer).thenReturn(socketServer)
    val logManager: LogManager = mock(classOf[LogManager])
    val producerStateManagerConfig: ProducerStateManagerConfig = mock(classOf[ProducerStateManagerConfig])
    when(logManager.producerStateManagerConfig).thenReturn(producerStateManagerConfig)
    when(kafkaServer.logManager).thenReturn(logManager)

    val authorizer = new TestAuthorizer
    when(kafkaServer.authorizer).thenReturn(Some(authorizer))

    kafkaServer.config.dynamicConfig().addReconfigurables(kafkaServer)
    props.put("super.users", "User:admin")
    kafkaServer.config.dynamicConfig().updateBrokerConfig(0, props, true)
    assertEquals("User:admin", authorizer.superUsers)
  }

  private def createCombinedControllerConfig(
    nodeId: Int,
    port: Int
  ): Properties = {
    val retval = TestUtils.createBrokerConfig(nodeId,
      zkConnect = null,
      enableControlledShutdown = true,
      enableDeleteTopic = true,
      port)
    retval.put(KafkaConfig.PROCESS_ROLES_PROP, "broker,controller")
    retval.put(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER")
    retval.put(KafkaConfig.LISTENERS_PROP, s"${retval.get(KafkaConfig.LISTENERS_PROP)},CONTROLLER://localhost:0")
    retval.put(KafkaConfig.QUORUM_VOTERS_PROP, s"${nodeId}@localhost:0")
    retval
  }

  @Test
  def testCombinedControllerAuthorizerConfig(): Unit = {
    val props = createCombinedControllerConfig(0, 9092)
    val oldConfig = KafkaConfigProvider.fromProps(props)
    oldConfig.dynamicConfig.initialize(Optional.empty(), Optional.empty())

    val controllerServer: ControllerServer = mock(classOf[kafka.server.ControllerServer])
    when(controllerServer.config).thenReturn(oldConfig)
    when(controllerServer.kafkaYammerMetrics).thenReturn(KafkaYammerMetrics.INSTANCE)
    val metrics: Metrics = mock(classOf[Metrics])
    when(controllerServer.metrics).thenReturn(metrics)
    val quotaManagers: QuotaFactory.QuotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    when(quotaManagers.clientQuotaCallback).thenReturn(None)
    when(controllerServer.quotaManagers).thenReturn(quotaManagers)
    val socketServer: SocketServer = mock(classOf[SocketServer])
    when(socketServer.reconfigurableConfigs).thenReturn(JSocketServer.RECONFIGURABLE_CONFIGS)
    when(controllerServer.socketServer).thenReturn(socketServer)

    val authorizer = new TestAuthorizer
    when(controllerServer.authorizer).thenReturn(Some(authorizer))

    controllerServer.config.dynamicConfig.addReconfigurables(controllerServer)
    props.put("super.users", "User:admin")
    controllerServer.config.dynamicConfig.updateBrokerConfig(0, props, true)
    assertEquals("User:admin", authorizer.superUsers)
  }

  private def createIsolatedControllerConfig(
    nodeId: Int,
    port: Int
  ): Properties = {
    val retval = TestUtils.createBrokerConfig(nodeId,
      zkConnect = null,
      enableControlledShutdown = true,
      enableDeleteTopic = true,
      port
    )
    retval.put(KafkaConfig.PROCESS_ROLES_PROP, "controller")
    retval.remove(KafkaConfig.ADVERTISED_LISTENERS_PROP)

    retval.put(KafkaConfig.CONTROLLER_LISTENER_NAMES_PROP, "CONTROLLER")
    retval.put(KafkaConfig.LISTENERS_PROP, "CONTROLLER://localhost:0")
    retval.put(KafkaConfig.QUORUM_VOTERS_PROP, s"${nodeId}@localhost:0")
    retval
  }

  @Test
  def testIsolatedControllerAuthorizerConfig(): Unit = {
    val props = createIsolatedControllerConfig(0, port = 9092)
    val oldConfig = KafkaConfigProvider.fromProps(props)
    oldConfig.dynamicConfig.initialize(Optional.empty(), Optional.empty())

    val controllerServer: ControllerServer = mock(classOf[kafka.server.ControllerServer])
    when(controllerServer.config).thenReturn(oldConfig)
    when(controllerServer.kafkaYammerMetrics).thenReturn(KafkaYammerMetrics.INSTANCE)
    val metrics: Metrics = mock(classOf[Metrics])
    when(controllerServer.metrics).thenReturn(metrics)
    val quotaManagers: QuotaFactory.QuotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    when(quotaManagers.clientQuotaCallback).thenReturn(None)
    when(controllerServer.quotaManagers).thenReturn(quotaManagers)
    val socketServer: SocketServer = mock(classOf[SocketServer])
    when(socketServer.reconfigurableConfigs).thenReturn(JSocketServer.RECONFIGURABLE_CONFIGS)
    when(controllerServer.socketServer).thenReturn(socketServer)

    val authorizer = new TestAuthorizer
    when(controllerServer.authorizer).thenReturn(Some(authorizer))

    controllerServer.config.dynamicConfig.addReconfigurables(controllerServer)
    props.put("super.users", "User:admin")
    controllerServer.config.dynamicConfig.updateBrokerConfig(0, props, true)
    assertEquals("User:admin", authorizer.superUsers)
  }

  @Test
  def testDynamicConfigInitializationWithoutConfigsInZK(): Unit = {
    val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getEntityConfigs(anyString(), anyString())).thenReturn(new java.util.Properties())

    val initialProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    initialProps.remove(KafkaConfig.BACKGROUND_THREADS_PROP)
    val oldConfig =  KafkaConfigProvider.fromProps(initialProps)
    val dynamicBrokerConfig = new DynamicBrokerConfig(oldConfig)
    dynamicBrokerConfig.initialize(Optional.of(zkClient), Optional.empty())
    dynamicBrokerConfig.addBrokerReconfigurable(new TestDynamicThreadPool)

    val newprops = new Properties()
    newprops.put(KafkaConfig.NUM_IO_THREADS_PROP, "10")
    newprops.put(KafkaConfig.BACKGROUND_THREADS_PROP, "100")
    dynamicBrokerConfig.updateBrokerConfig(0, newprops)
  }

  @Test
  def testImproperConfigsAreRemoved(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect)
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")
    val config = KafkaConfigProvider.fromProps(props)
    config.dynamicConfig().initialize(Optional.empty(), Optional.empty())

    assertEquals(Defaults.MAX_CONNECTIONS, config.maxConnections)
    assertEquals(LogConfig.DEFAULT_MAX_MESSAGE_BYTES, config.messageMaxBytes)

    var newProps = new Properties()
    newProps.put(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "9999")
    newProps.put(KafkaConfig.MESSAGE_MAX_BYTES_PROP, "2222")

    config.dynamicConfig.updateDefaultConfig(newProps, true)
    assertEquals(9999, config.maxConnections)
    assertEquals(2222, config.messageMaxBytes)

    newProps = new Properties()
    newProps.put(KafkaConfig.MAX_CONNECTIONS_PER_IP_PROP, "INVALID_INT")
    newProps.put(KafkaConfig.MESSAGE_MAX_BYTES_PROP, "1111")

    config.dynamicConfig.updateDefaultConfig(newProps, true)
    // Invalid value should be skipped and reassigned as default value
    assertEquals(Defaults.MAX_CONNECTIONS, config.maxConnections)
    // Even if One property is invalid, the below should get correctly updated.
    assertEquals(1111, config.messageMaxBytes)
  }

  @Test
  def testUpdateMetricReporters(): Unit = {
    val brokerId = 0
    val origProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect, port = 8181)
    origProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")

    val config = KafkaConfigProvider.fromProps(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val metrics = Mockito.mock(classOf[Metrics])

    Mockito.when(serverMock.config).thenReturn(config)

    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    val m = new DynamicMetricsReporters(brokerId, config, metrics, "clusterId")
    config.dynamicConfig.addReconfigurable(m)
    assertEquals(1, m.currentReporters.size)
    assertEquals(classOf[JmxReporter].getName, m.currentReporters.keySet.head)

    val props = new Properties()
    props.put(KafkaConfig.METRIC_REPORTER_CLASSES_PROP, classOf[MockMetricsReporter].getName)
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(2, m.currentReporters.size)
    assertEquals(Set(classOf[JmxReporter].getName, classOf[MockMetricsReporter].getName), m.currentReporters.keySet)
  }

  @Test
  @nowarn("cat=deprecation")
  def testUpdateMetricReportersNoJmxReporter(): Unit = {
    val brokerId = 0
    val origProps = TestUtils.createBrokerConfig(brokerId, TestUtils.MockZkConnect, port = 8181)
    origProps.put(KafkaConfig.AUTO_INCLUDE_JMX_REPORTER_PROP, "false")
    origProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")
    val config = KafkaConfigProvider.fromProps(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val metrics = Mockito.mock(classOf[Metrics])

    Mockito.when(serverMock.config).thenReturn(config)

    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    val m = new DynamicMetricsReporters(brokerId, config, metrics, "clusterId")
    config.dynamicConfig.addReconfigurable(m)
    assertTrue(m.currentReporters.isEmpty)

    val props = new Properties()
    props.put(KafkaConfig.METRIC_REPORTER_CLASSES_PROP, classOf[MockMetricsReporter].getName)
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertEquals(1, m.currentReporters.size)
    assertEquals(classOf[MockMetricsReporter].getName, m.currentReporters.keySet.head)

    props.remove(KafkaConfig.METRIC_REPORTER_CLASSES_PROP)
    config.dynamicConfig.updateDefaultConfig(props, true)
    assertTrue(m.currentReporters.isEmpty)
  }

  @Test
  def testNonInternalValuesDoesNotExposeInternalConfigs(): Unit = {
    val props = new Properties()
    props.put(KafkaConfig.ZK_CONNECT_PROP, "localhost:2181")
    props.put(KafkaConfig.METADATA_LOG_SEGMENT_MIN_BYTES_PROP, "1024")
    val config = KafkaConfigProvider.fromProps(props)
    assertFalse(config.nonInternalValues.containsKey(KafkaConfig.METADATA_LOG_SEGMENT_MIN_BYTES_PROP))
    config.updateCurrentConfig(KafkaConfigProvider.fromProps(props))
    assertFalse(config.nonInternalValues.containsKey(KafkaConfig.METADATA_LOG_SEGMENT_MIN_BYTES_PROP))
  }

  @Test
  def testDynamicLogLocalRetentionMsConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP, "2592000000")
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")
    val config = KafkaConfigProvider.fromProps(props)
    val dynamicLogConfig = new DynamicLogConfig(mock(classOf[LogManager]), mock(classOf[KafkaServer]))
    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    config.dynamicConfig.addBrokerReconfigurable(dynamicLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2160000000")
    // update default config
    config.dynamicConfig.validate(newProps, false)
    config.dynamicConfig.updateDefaultConfig(newProps, true)
    assertEquals(2160000000L, config.logLocalRetentionMs)

    // update per broker config
    config.dynamicConfig.validate(newProps, true)
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2150000000")
    config.dynamicConfig.updateBrokerConfig(0, newProps, true)
    assertEquals(2150000000L, config.logLocalRetentionMs)
  }

  @Test
  def testDynamicLogLocalRetentionSizeConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LOG_RETENTION_BYTES_PROP, "4294967296")
    props.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")
    val config = KafkaConfigProvider.fromProps(props)
    val dynamicLogConfig = new DynamicLogConfig(mock(classOf[LogManager]), mock(classOf[KafkaServer]))
    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    config.dynamicConfig.addBrokerReconfigurable(dynamicLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967295")
    // update default config
    config.dynamicConfig.validate(newProps, false)
    config.dynamicConfig.updateDefaultConfig(newProps, true)
    assertEquals(4294967295L, config.logLocalRetentionBytes)

    // update per broker config
    config.dynamicConfig.validate(newProps, true)
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967294")
    config.dynamicConfig.updateBrokerConfig(0, newProps, true)
    assertEquals(4294967294L, config.logLocalRetentionBytes)
  }

  @Test
  def testDynamicLogLocalRetentionSkipsOnInvalidConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1000")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1024")
    val config = KafkaConfigProvider.fromProps(props)
    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())

    // Check for invalid localRetentionMs < -2
    verifyConfigUpdateWithInvalidConfig(config, props, Map.empty, Map(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP -> "-3"))
    // Check for invalid localRetentionBytes < -2
    verifyConfigUpdateWithInvalidConfig(config, props, Map.empty, Map(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP -> "-3"))
  }

  @Test
  def testDynamicLogLocalRetentionThrowsOnIncorrectConfig(): Unit = {
    // Check for incorrect case of logLocalRetentionMs > retentionMs
    verifyIncorrectLogLocalRetentionProps(2000L, 1000L, 2, 100)
    // Check for incorrect case of logLocalRetentionBytes > retentionBytes
    verifyIncorrectLogLocalRetentionProps(500L, 1000L, 200, 100)
    // Check for incorrect case of logLocalRetentionMs (-1 viz unlimited) > retentionMs,
    verifyIncorrectLogLocalRetentionProps(-1, 1000L, 200, 100)
    // Check for incorrect case of logLocalRetentionBytes(-1 viz unlimited) > retentionBytes
    verifyIncorrectLogLocalRetentionProps(2000L, 1000L, -1, 100)
  }

  @Test
  def testUpdateDynamicRemoteLogManagerConfig(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    origProps.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, "2")
    origProps.put(KafkaConfig.PASSWORD_ENCODER_SECRET_PROP, "broker.secret")

    val config = KafkaConfigProvider.fromProps(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val remoteLogManagerMockOpt = Option(Mockito.mock(classOf[RemoteLogManager]))

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.remoteLogManagerOpt).thenReturn(remoteLogManagerMockOpt)

    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    val props = new Properties()

    props.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, "4")
    config.dynamicConfig().updateDefaultConfig(props, true)
    assertEquals(4L, config.getLong(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP))
    Mockito.verify(remoteLogManagerMockOpt.get).resizeCacheSize(4)

    Mockito.verifyNoMoreInteractions(remoteLogManagerMockOpt.get)
  }

  def verifyIncorrectLogLocalRetentionProps(logLocalRetentionMs: Long,
                                            retentionMs: Long,
                                            logLocalRetentionBytes: Long,
                                            retentionBytes: Long): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(KafkaConfig.LOG_RETENTION_TIME_MILLIS_PROP, retentionMs.toString)
    props.put(KafkaConfig.LOG_RETENTION_BYTES_PROP, retentionBytes.toString)
    val config = KafkaConfigProvider.fromProps(props)
    val dynamicLogConfig = new DynamicLogConfig(mock(classOf[LogManager]), mock(classOf[KafkaServer]))
    config.dynamicConfig.initialize(Optional.empty(), Optional.empty())
    config.dynamicConfig.addBrokerReconfigurable(dynamicLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs.toString)
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes.toString)
    // validate default config
    assertThrows(classOf[ConfigException], () =>  config.dynamicConfig.validate(newProps, false))
    // validate per broker config
    assertThrows(classOf[ConfigException], () =>  config.dynamicConfig.validate(newProps, true))
  }
}

class TestDynamicThreadPool() extends BrokerReconfigurable {

  override def reconfigurableConfigs: util.Set[String] = {
    JDynamicThreadPool.RECONFIGURABLE_CONFIGS
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    assertEquals(Defaults.NUM_IO_THREADS, oldConfig.numIoThreads)
    assertEquals(Defaults.BACKGROUND_THREADS, oldConfig.backgroundThreads)

    assertEquals(10, newConfig.numIoThreads)
    assertEquals(100, newConfig.backgroundThreads)
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    assertEquals(10, newConfig.numIoThreads)
    assertEquals(100, newConfig.backgroundThreads)
  }
}
