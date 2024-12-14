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
import java.util.{Optional, Properties, Map => JMap}
import java.util.concurrent.{CompletionStage, TimeUnit}
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
import org.apache.kafka.common.config.{ConfigException, SaslConfigs, SslConfigs}
import org.apache.kafka.common.metrics.{JmxReporter, Metrics}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.PasswordEncoderConfigs
import org.apache.kafka.server.authorizer._
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs, ZkConfigs}
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig
import org.apache.kafka.server.metrics.{KafkaYammerMetrics, MetricConfigs}
import org.apache.kafka.server.util.KafkaScheduler
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig, ProducerStateManagerConfig}
import org.apache.kafka.test.MockMetricsReporter
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}
import org.mockito.Mockito.{mock, verify, verifyNoMoreInteractions, when}

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.collection.Set

class DynamicBrokerConfigTest {

  @Test
  def testConfigUpdate(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    val oldKeystore = "oldKs.jks"
    props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, oldKeystore)
    val config = KafkaConfig(props)
    val dynamicConfig = config.dynamicConfig
    dynamicConfig.initialize(None, None)

    assertEquals(config, dynamicConfig.currentKafkaConfig)
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

      assertEquals(oldKeystore, config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
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
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")

    val config = KafkaConfig(origProps)
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

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicLogConfig(logManagerMock, serverMock))

    val props = new Properties()

    props.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    config.dynamicConfig.updateDefaultConfig(props)
    assertTrue(config.uncleanLeaderElectionEnable)
    Mockito.verify(controllerMock).enableDefaultUncleanLeaderElection()
  }

  @Test
  def testUpdateDynamicThreadPool(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(ServerConfigs.NUM_IO_THREADS_CONFIG, "4")
    origProps.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2")
    origProps.put(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, "1")
    origProps.put(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG, "1")
    origProps.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "3")

    val config = KafkaConfig(origProps)
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

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new BrokerDynamicThreadPool(serverMock))
    config.dynamicConfig.addReconfigurable(acceptorMock)

    val props = new Properties()

    props.put(ServerConfigs.NUM_IO_THREADS_CONFIG, "8")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(8, config.numIoThreads)
    Mockito.verify(handlerPoolMock).resizeThreadPool(newSize = 8)

    props.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "4")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(4, config.numNetworkThreads)
    val captor: ArgumentCaptor[JMap[String, String]] = ArgumentCaptor.forClass(classOf[JMap[String, String]])
    Mockito.verify(acceptorMock).reconfigure(captor.capture())
    assertTrue(captor.getValue.containsKey(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG))
    assertEquals(4, captor.getValue.get(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG))

    props.put(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, "2")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(2, config.numReplicaFetchers)
    Mockito.verify(replicaManagerMock).resizeFetcherThreadPool(newSize = 2)

    props.put(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG, "2")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(2, config.numRecoveryThreadsPerDataDir)
    Mockito.verify(logManagerMock).resizeRecoveryThreadPool(newSize = 2)

    props.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "6")
    config.dynamicConfig.updateDefaultConfig(props)
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

  @Test
  def testUpdateRemoteLogManagerDynamicThreadPool(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val config = KafkaConfig(origProps)
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE, config.remoteLogManagerConfig.remoteLogManagerCopierThreadPoolSize())
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE, config.remoteLogManagerConfig.remoteLogManagerExpirationThreadPoolSize())
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_READER_THREADS, config.remoteLogManagerConfig.remoteLogReaderThreads())

    val serverMock = mock(classOf[KafkaBroker])
    val remoteLogManager = mock(classOf[RemoteLogManager])
    when(serverMock.config).thenReturn(config)
    when(serverMock.remoteLogManagerOpt).thenReturn(Some(remoteLogManager))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    // Test dynamic update with valid values
    val props = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP, "8")
    config.dynamicConfig.validate(props, perBrokerConfig = true)
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(8, config.remoteLogManagerConfig.remoteLogManagerCopierThreadPoolSize())
    verify(remoteLogManager).resizeCopierThreadPool(8)

    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP, "7")
    config.dynamicConfig.validate(props, perBrokerConfig = false)
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(7, config.remoteLogManagerConfig.remoteLogManagerExpirationThreadPoolSize())
    verify(remoteLogManager).resizeExpirationThreadPool(7)

    props.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, "6")
    config.dynamicConfig.validate(props, perBrokerConfig = true)
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(6, config.remoteLogManagerConfig.remoteLogReaderThreads())
    verify(remoteLogManager).resizeReaderThreadPool(6)
    props.clear()
    verifyNoMoreInteractions(remoteLogManager)
  }

  @Test
  def testRemoteLogDynamicThreadPoolWithInvalidValues(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    val config = KafkaConfig(origProps)

    val serverMock = mock(classOf[KafkaBroker])
    val remoteLogManager = mock(classOf[RemoteLogManager])
    when(serverMock.config).thenReturn(config)
    when(serverMock.remoteLogManagerOpt).thenReturn(Some(remoteLogManager))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    // Test dynamic update with invalid values
    val props = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPIER_THREAD_POOL_SIZE_PROP, "0")
    val err = assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(props, perBrokerConfig = true))
    assertTrue(err.getMessage.contains("Value must be at least 1"))

    val props1 = new Properties()
    props1.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_EXPIRATION_THREAD_POOL_SIZE_PROP, "-1")
    val err1 = assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(props1, perBrokerConfig = false))
    assertTrue(err1.getMessage.contains("Value must be at least 1"))

    val props2 = new Properties()
    props2.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, "2")
    val err2 = assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(props2, perBrokerConfig = false))
    assertTrue(err2.getMessage.contains("value should be at least half the current value"))

    val props3 = new Properties()
    props3.put(RemoteLogManagerConfig.REMOTE_LOG_READER_THREADS_PROP, "-1")
    val err3 = assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(props, perBrokerConfig = true))
    assertTrue(err3.getMessage.contains("Value must be at least 1"))
    verifyNoMoreInteractions(remoteLogManager)
  }

  @nowarn("cat=deprecation")
  @Test
  def testConfigUpdateWithSomeInvalidConfigs(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS")
    val config = KafkaConfig(origProps)
    config.dynamicConfig.initialize(None, None)

    val validProps = Map(s"listener.name.external.${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG}" -> "ks.p12")

    val securityPropsWithoutListenerPrefix = Map(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG -> "PKCS12")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, securityPropsWithoutListenerPrefix)
    val nonDynamicProps = Map(ZkConfigs.ZK_CONNECT_CONFIG -> "somehost:2181")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, nonDynamicProps)

    // Test update of configs with invalid type
    val invalidProps = Map(CleanerConfig.LOG_CLEANER_THREADS_PROP -> "invalid")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)

    val excludedTopicConfig = Map(ServerLogConfigs.LOG_MESSAGE_FORMAT_VERSION_CONFIG -> "0.10.2")
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, excludedTopicConfig)
  }

  @Test
  def testConfigUpdateWithReconfigurableValidationFailure(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "100000000")
    val config = KafkaConfig(origProps)
    config.dynamicConfig.initialize(None, None)

    val validProps = Map.empty[String, String]
    val invalidProps = Map(CleanerConfig.LOG_CLEANER_THREADS_PROP -> "20")

    def validateLogCleanerConfig(configs: util.Map[String, _]): Unit = {
      val cleanerThreads = configs.get(CleanerConfig.LOG_CLEANER_THREADS_PROP).toString.toInt
      if (cleanerThreads <=0 || cleanerThreads >= 5)
        throw new ConfigException(s"Invalid cleaner threads $cleanerThreads")
    }
    val reconfigurable = new Reconfigurable {
      override def configure(configs: util.Map[String, _]): Unit = {}
      override def reconfigurableConfigs(): util.Set[String] = Set(CleanerConfig.LOG_CLEANER_THREADS_PROP).asJava
      override def validateReconfiguration(configs: util.Map[String, _]): Unit = validateLogCleanerConfig(configs)
      override def reconfigure(configs: util.Map[String, _]): Unit = {}
    }
    config.dynamicConfig.addReconfigurable(reconfigurable)
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
    config.dynamicConfig.removeReconfigurable(reconfigurable)

    val brokerReconfigurable = new BrokerReconfigurable {
      override def reconfigurableConfigs: collection.Set[String] = Set(CleanerConfig.LOG_CLEANER_THREADS_PROP)
      override def validateReconfiguration(newConfig: KafkaConfig): Unit = validateLogCleanerConfig(newConfig.originals)
      override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {}
    }
    config.dynamicConfig.addBrokerReconfigurable(brokerReconfigurable)
    verifyConfigUpdateWithInvalidConfig(config, origProps, validProps, invalidProps)
  }

  @Test
  def testReconfigurableValidation(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    val config = KafkaConfig(origProps)
    val invalidReconfigurableProps = Set(CleanerConfig.LOG_CLEANER_THREADS_PROP, ServerConfigs.BROKER_ID_CONFIG, "some.prop")
    val validReconfigurableProps = Set(CleanerConfig.LOG_CLEANER_THREADS_PROP, CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "some.prop")

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
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, "100", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, "100", perBrokerConfig = false, expectFailure = false)
    //MaxConnectionsPerIpProp can be set to zero only if MaxConnectionsPerIpOverridesProp property is set
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_CONFIG, "0", perBrokerConfig = false, expectFailure = true)

    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, "hostName1:100,hostName2:0", perBrokerConfig = true,
      expectFailure = false)
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, "hostName1:100,hostName2:0", perBrokerConfig = false,
      expectFailure = false)
    //test invalid address
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_PER_IP_OVERRIDES_CONFIG, "hostName#:100", perBrokerConfig = true,
      expectFailure = true)

    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, "100", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, "100", perBrokerConfig = false, expectFailure = false)
    val listenerMaxConnectionsProp = s"listener.name.external.${SocketServerConfigs.MAX_CONNECTIONS_CONFIG}"
    verifyConfigUpdate(listenerMaxConnectionsProp, "10", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(listenerMaxConnectionsProp, "10", perBrokerConfig = false, expectFailure = false)
  }

  @Test
  def testConnectionRateQuota(): Unit = {
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG, "110", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG, "120", perBrokerConfig = false, expectFailure = false)
    val listenerMaxConnectionsProp = s"listener.name.external.${SocketServerConfigs.MAX_CONNECTION_CREATION_RATE_CONFIG}"
    verifyConfigUpdate(listenerMaxConnectionsProp, "20", perBrokerConfig = true, expectFailure = false)
    verifyConfigUpdate(listenerMaxConnectionsProp, "30", perBrokerConfig = false, expectFailure = false)
  }

  private def verifyConfigUpdate(name: String, value: Object, perBrokerConfig: Boolean, expectFailure: Boolean): Unit = {
    val configProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    configProps.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "broker.secret")
    val config = KafkaConfig(configProps)
    config.dynamicConfig.initialize(None, None)

    val props = new Properties
    props.put(name, value)
    val oldValue = config.originals.get(name)

    def updateConfig(): Unit = {
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
    props.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "config-encoder-secret")
    val configWithSecret = KafkaConfig(props)
    val dynamicProps = new Properties
    dynamicProps.put(SaslConfigs.SASL_JAAS_CONFIG, "myLoginModule required;")

    try {
      configWithoutSecret.dynamicConfig.toPersistentProps(dynamicProps, perBrokerConfig = true)
    } catch {
      case _: ConfigException => // expected exception
    }
    val persistedProps = configWithSecret.dynamicConfig.toPersistentProps(dynamicProps, perBrokerConfig = true)
    assertFalse(persistedProps.getProperty(SaslConfigs.SASL_JAAS_CONFIG).contains("myLoginModule"),
      "Password not encoded")
    val decodedProps = configWithSecret.dynamicConfig.fromPersistentProps(persistedProps, perBrokerConfig = true)
    assertEquals("myLoginModule required;", decodedProps.getProperty(SaslConfigs.SASL_JAAS_CONFIG))
  }

  @Test
  def testPasswordConfigEncoderSecretChange(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 8181)
    props.put(SaslConfigs.SASL_JAAS_CONFIG, "staticLoginModule required;")
    props.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "config-encoder-secret")
    val config = KafkaConfig(props)
    config.dynamicConfig.initialize(None, None)
    val dynamicProps = new Properties
    dynamicProps.put(SaslConfigs.SASL_JAAS_CONFIG, "dynamicLoginModule required;")

    val persistedProps = config.dynamicConfig.toPersistentProps(dynamicProps, perBrokerConfig = true)
    assertFalse(persistedProps.getProperty(SaslConfigs.SASL_JAAS_CONFIG).contains("LoginModule"),
      "Password not encoded")
    config.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("dynamicLoginModule required;", config.values.get(SaslConfigs.SASL_JAAS_CONFIG).asInstanceOf[Password].value)

    // New config with same secret should use the dynamic password config
    val newConfigWithSameSecret = KafkaConfig(props)
    newConfigWithSameSecret.dynamicConfig.initialize(None, None)
    newConfigWithSameSecret.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("dynamicLoginModule required;", newConfigWithSameSecret.values.get(SaslConfigs.SASL_JAAS_CONFIG).asInstanceOf[Password].value)

    // New config with new secret should use the dynamic password config if new and old secrets are configured in KafkaConfig
    props.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "new-encoder-secret")
    props.put(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG, "config-encoder-secret")
    val newConfigWithNewAndOldSecret = KafkaConfig(props)
    newConfigWithNewAndOldSecret.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("dynamicLoginModule required;", newConfigWithSameSecret.values.get(SaslConfigs.SASL_JAAS_CONFIG).asInstanceOf[Password].value)

    // New config with new secret alone should revert to static password config since dynamic config cannot be decoded
    props.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "another-new-encoder-secret")
    val newConfigWithNewSecret = KafkaConfig(props)
    newConfigWithNewSecret.dynamicConfig.updateBrokerConfig(0, persistedProps)
    assertEquals("staticLoginModule required;", newConfigWithNewSecret.values.get(SaslConfigs.SASL_JAAS_CONFIG).asInstanceOf[Password].value)
  }

  @Test
  def testDynamicListenerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    val oldConfig =  KafkaConfig.fromProps(props)
    val kafkaServer: KafkaBroker = mock(classOf[kafka.server.KafkaBroker])
    when(kafkaServer.config).thenReturn(oldConfig)

    props.put(SocketServerConfigs.LISTENERS_CONFIG, "PLAINTEXT://hostname:9092,SASL_PLAINTEXT://hostname:9093")
    new DynamicListenerConfig(kafkaServer).validateReconfiguration(KafkaConfig(props))

    // it is illegal to update non-reconfiguable configs of existent listeners
    props.put("listener.name.plaintext.you.should.not.pass", "failure")
    val dynamicListenerConfig = new DynamicListenerConfig(kafkaServer)
    assertThrows(classOf[ConfigException], () => dynamicListenerConfig.validateReconfiguration(KafkaConfig(props)))
  }

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

  @Test
  def testAuthorizerConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 9092)
    val oldConfig =  KafkaConfig.fromProps(props)
    oldConfig.dynamicConfig.initialize(None, None)

    val kafkaServer: KafkaBroker = mock(classOf[kafka.server.KafkaBroker])
    when(kafkaServer.config).thenReturn(oldConfig)
    when(kafkaServer.kafkaYammerMetrics).thenReturn(KafkaYammerMetrics.INSTANCE)
    val metrics: Metrics = mock(classOf[Metrics])
    when(kafkaServer.metrics).thenReturn(metrics)
    val quotaManagers: QuotaFactory.QuotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    when(quotaManagers.clientQuotaCallback).thenReturn(Optional.empty())
    when(kafkaServer.quotaManagers).thenReturn(quotaManagers)
    val socketServer: SocketServer = mock(classOf[SocketServer])
    when(socketServer.reconfigurableConfigs).thenReturn(SocketServer.ReconfigurableConfigs)
    when(kafkaServer.socketServer).thenReturn(socketServer)
    val logManager: LogManager = mock(classOf[LogManager])
    val producerStateManagerConfig: ProducerStateManagerConfig = mock(classOf[ProducerStateManagerConfig])
    when(logManager.producerStateManagerConfig).thenReturn(producerStateManagerConfig)
    when(kafkaServer.logManager).thenReturn(logManager)

    val authorizer = new TestAuthorizer
    when(kafkaServer.authorizer).thenReturn(Some(authorizer))

    kafkaServer.config.dynamicConfig.addReconfigurables(kafkaServer)
    props.put("super.users", "User:admin")
    kafkaServer.config.dynamicConfig.updateBrokerConfig(0, props)
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
    retval.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker,controller")
    retval.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    retval.put(SocketServerConfigs.LISTENERS_CONFIG, s"${retval.get(SocketServerConfigs.LISTENERS_CONFIG)},CONTROLLER://localhost:0")
    retval.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId}@localhost:0")
    retval
  }

  @Test
  def testCombinedControllerAuthorizerConfig(): Unit = {
    val props = createCombinedControllerConfig(0, 9092)
    val oldConfig = KafkaConfig.fromProps(props)
    oldConfig.dynamicConfig.initialize(None, None)

    val controllerServer: ControllerServer = mock(classOf[kafka.server.ControllerServer])
    when(controllerServer.config).thenReturn(oldConfig)
    when(controllerServer.kafkaYammerMetrics).thenReturn(KafkaYammerMetrics.INSTANCE)
    val metrics: Metrics = mock(classOf[Metrics])
    when(controllerServer.metrics).thenReturn(metrics)
    val quotaManagers: QuotaFactory.QuotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    when(quotaManagers.clientQuotaCallback).thenReturn(Optional.empty())
    when(controllerServer.quotaManagers).thenReturn(quotaManagers)
    val socketServer: SocketServer = mock(classOf[SocketServer])
    when(socketServer.reconfigurableConfigs).thenReturn(SocketServer.ReconfigurableConfigs)
    when(controllerServer.socketServer).thenReturn(socketServer)

    val authorizer = new TestAuthorizer
    when(controllerServer.authorizer).thenReturn(Some(authorizer))

    controllerServer.config.dynamicConfig.addReconfigurables(controllerServer)
    props.put("super.users", "User:admin")
    controllerServer.config.dynamicConfig.updateBrokerConfig(0, props)
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
    retval.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    retval.remove(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG)

    retval.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    retval.put(SocketServerConfigs.LISTENERS_CONFIG, "CONTROLLER://localhost:0")
    retval.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"${nodeId}@localhost:0")
    retval
  }

  @Test
  def testIsolatedControllerAuthorizerConfig(): Unit = {
    val props = createIsolatedControllerConfig(0, port = 9092)
    val oldConfig = KafkaConfig.fromProps(props)
    oldConfig.dynamicConfig.initialize(None, None)

    val controllerServer: ControllerServer = mock(classOf[kafka.server.ControllerServer])
    when(controllerServer.config).thenReturn(oldConfig)
    when(controllerServer.kafkaYammerMetrics).thenReturn(KafkaYammerMetrics.INSTANCE)
    val metrics: Metrics = mock(classOf[Metrics])
    when(controllerServer.metrics).thenReturn(metrics)
    val quotaManagers: QuotaFactory.QuotaManagers = mock(classOf[QuotaFactory.QuotaManagers])
    when(quotaManagers.clientQuotaCallback).thenReturn(Optional.empty())
    when(controllerServer.quotaManagers).thenReturn(quotaManagers)
    val socketServer: SocketServer = mock(classOf[SocketServer])
    when(socketServer.reconfigurableConfigs).thenReturn(SocketServer.ReconfigurableConfigs)
    when(controllerServer.socketServer).thenReturn(socketServer)

    val authorizer = new TestAuthorizer
    when(controllerServer.authorizer).thenReturn(Some(authorizer))

    controllerServer.config.dynamicConfig.addReconfigurables(controllerServer)
    props.put("super.users", "User:admin")
    controllerServer.config.dynamicConfig.updateBrokerConfig(0, props)
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
    assertEquals(List(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG),
      DynamicBrokerConfig.brokerConfigSynonyms(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, matchListenerOverride = true))
  }

  @Test
  def testDynamicConfigInitializationWithoutConfigsInZK(): Unit = {
    val zkClient: KafkaZkClient = mock(classOf[KafkaZkClient])
    when(zkClient.getEntityConfigs(anyString(), anyString())).thenReturn(new java.util.Properties())

    val initialProps = TestUtils.createBrokerConfig(0, TestUtils.MockZkConnect, port = 9092)
    initialProps.remove(ServerConfigs.BACKGROUND_THREADS_CONFIG)
    val oldConfig =  KafkaConfig.fromProps(initialProps)
    val dynamicBrokerConfig = new DynamicBrokerConfig(oldConfig)
    dynamicBrokerConfig.initialize(Some(zkClient), None)
    dynamicBrokerConfig.addBrokerReconfigurable(new TestDynamicThreadPool)

    val newprops = new Properties()
    newprops.put(ServerConfigs.NUM_IO_THREADS_CONFIG, "10")
    newprops.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "100")
    dynamicBrokerConfig.updateBrokerConfig(0, newprops)
  }

  @Test
  def testImproperConfigsAreRemoved(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null)
    val config = KafkaConfig(props)
    config.dynamicConfig.initialize(None, None)

    assertEquals(SocketServerConfigs.MAX_CONNECTIONS_DEFAULT, config.maxConnections)
    assertEquals(LogConfig.DEFAULT_MAX_MESSAGE_BYTES, config.messageMaxBytes)

    var newProps = new Properties()
    newProps.put(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, "9999")
    newProps.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "2222")

    config.dynamicConfig.updateDefaultConfig(newProps)
    assertEquals(9999, config.maxConnections)
    assertEquals(2222, config.messageMaxBytes)

    newProps = new Properties()
    newProps.put(SocketServerConfigs.MAX_CONNECTIONS_CONFIG, "INVALID_INT")
    newProps.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "1111")

    config.dynamicConfig.updateDefaultConfig(newProps)
    // Invalid value should be skipped and reassigned as default value
    assertEquals(SocketServerConfigs.MAX_CONNECTIONS_DEFAULT, config.maxConnections)
    // Even if One property is invalid, the below should get correctly updated.
    assertEquals(1111, config.messageMaxBytes)
  }

  @Test
  def testUpdateMetricReporters(): Unit = {
    val brokerId = 0
    val origProps = TestUtils.createBrokerConfig(brokerId, null, port = 8181)

    val config = KafkaConfig(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val metrics = Mockito.mock(classOf[Metrics])

    Mockito.when(serverMock.config).thenReturn(config)

    config.dynamicConfig.initialize(None, None)
    val m = new DynamicMetricsReporters(brokerId, config, metrics, "clusterId")
    config.dynamicConfig.addReconfigurable(m)
    assertEquals(1, m.currentReporters.size)
    assertEquals(classOf[JmxReporter].getName, m.currentReporters.keySet.head)

    val props = new Properties()
    props.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, s"${classOf[JmxReporter].getName},${classOf[MockMetricsReporter].getName}")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(2, m.currentReporters.size)
    assertEquals(Set(classOf[JmxReporter].getName, classOf[MockMetricsReporter].getName), m.currentReporters.keySet)
  }

  @Test
  def testUpdateMetricReportersNoJmxReporter(): Unit = {
    val brokerId = 0
    val origProps = TestUtils.createBrokerConfig(brokerId, null, port = 8181)
    origProps.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, "")

    val config = KafkaConfig(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val metrics = Mockito.mock(classOf[Metrics])

    Mockito.when(serverMock.config).thenReturn(config)

    config.dynamicConfig.initialize(None, None)
    val m = new DynamicMetricsReporters(brokerId, config, metrics, "clusterId")
    config.dynamicConfig.addReconfigurable(m)
    assertTrue(m.currentReporters.isEmpty)

    val props = new Properties()
    props.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, classOf[MockMetricsReporter].getName)
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(1, m.currentReporters.size)
    assertEquals(classOf[MockMetricsReporter].getName, m.currentReporters.keySet.head)

    props.remove(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG)
    config.dynamicConfig.updateDefaultConfig(props)
    assertTrue(m.currentReporters.isEmpty)
  }

  @Test
  def testNonInternalValuesDoesNotExposeInternalConfigs(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    props.put(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG, "1024")
    val config = new KafkaConfig(props)
    assertFalse(config.nonInternalValues.containsKey(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG))
    config.updateCurrentConfig(new KafkaConfig(props))
    assertFalse(config.nonInternalValues.containsKey(KRaftConfigs.METADATA_LOG_SEGMENT_MIN_BYTES_CONFIG))
  }

  @Test
  def testDynamicLogLocalRetentionMsConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "2592000000")
    val config = KafkaConfig(props)
    val dynamicLogConfig = new DynamicLogConfig(mock(classOf[LogManager]), mock(classOf[KafkaBroker]))
    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(dynamicLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2160000000")
    // update default config
    config.dynamicConfig.validate(newProps, perBrokerConfig = false)
    config.dynamicConfig.updateDefaultConfig(newProps)
    assertEquals(2160000000L, config.remoteLogManagerConfig.logLocalRetentionMs)

    // update per broker config
    config.dynamicConfig.validate(newProps, perBrokerConfig = true)
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "2150000000")
    config.dynamicConfig.updateBrokerConfig(0, newProps)
    assertEquals(2150000000L, config.remoteLogManagerConfig.logLocalRetentionMs)
  }

  @Test
  def testDynamicLogLocalRetentionSizeConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "4294967296")
    val config = KafkaConfig(props)
    val dynamicLogConfig = new DynamicLogConfig(mock(classOf[LogManager]), mock(classOf[KafkaBroker]))
    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(dynamicLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967295")
    // update default config
    config.dynamicConfig.validate(newProps, perBrokerConfig = false)
    config.dynamicConfig.updateDefaultConfig(newProps)
    assertEquals(4294967295L, config.remoteLogManagerConfig.logLocalRetentionBytes)

    // update per broker config
    config.dynamicConfig.validate(newProps, perBrokerConfig = true)
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "4294967294")
    config.dynamicConfig.updateBrokerConfig(0, newProps)
    assertEquals(4294967294L, config.remoteLogManagerConfig.logLocalRetentionBytes)
  }

  @Test
  def testDynamicLogLocalRetentionSkipsOnInvalidConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1000")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1024")
    val config = KafkaConfig(props)
    config.dynamicConfig.initialize(None, None)

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
  def testDynamicRemoteFetchMaxWaitMsConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    val config = KafkaConfig(props)
    val kafkaBroker = mock(classOf[KafkaBroker])
    when(kafkaBroker.config).thenReturn(config)
    when(kafkaBroker.remoteLogManagerOpt).thenReturn(None)
    assertEquals(500, config.remoteLogManagerConfig.remoteFetchMaxWaitMs)

    val dynamicRemoteLogConfig = new DynamicRemoteLogConfig(kafkaBroker)
    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(dynamicRemoteLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP, "30000")
    // update default config
    config.dynamicConfig.validate(newProps, perBrokerConfig = false)
    config.dynamicConfig.updateDefaultConfig(newProps)
    assertEquals(30000, config.remoteLogManagerConfig.remoteFetchMaxWaitMs)

    // update per broker config
    newProps.put(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP, "10000")
    config.dynamicConfig.validate(newProps, perBrokerConfig = true)
    config.dynamicConfig.updateBrokerConfig(0, newProps)
    assertEquals(10000, config.remoteLogManagerConfig.remoteFetchMaxWaitMs)

    // invalid values
    for (maxWaitMs <- Seq(-1, 0)) {
      newProps.put(RemoteLogManagerConfig.REMOTE_FETCH_MAX_WAIT_MS_PROP, maxWaitMs.toString)
      assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(newProps, perBrokerConfig = true))
      assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(newProps, perBrokerConfig = false))
    }
  }

  @Test
  def testDynamicRemoteListOffsetsRequestTimeoutMsConfig(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    val config = KafkaConfig(props)
    val kafkaBroker = mock(classOf[KafkaBroker])
    when(kafkaBroker.config).thenReturn(config)
    when(kafkaBroker.remoteLogManagerOpt).thenReturn(None)
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
      config.remoteLogManagerConfig.remoteListOffsetsRequestTimeoutMs)

    val dynamicRemoteLogConfig = new DynamicRemoteLogConfig(kafkaBroker)
    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(dynamicRemoteLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP, "60000")
    // update default config
    config.dynamicConfig.validate(newProps, perBrokerConfig = false)
    config.dynamicConfig.updateDefaultConfig(newProps)
    assertEquals(60000L, config.remoteLogManagerConfig.remoteListOffsetsRequestTimeoutMs)

    // update per broker config
    newProps.put(RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP, "10000")
    config.dynamicConfig.validate(newProps, perBrokerConfig = true)
    config.dynamicConfig.updateBrokerConfig(0, newProps)
    assertEquals(10000L, config.remoteLogManagerConfig.remoteListOffsetsRequestTimeoutMs)

    // invalid values
    for (timeoutMs <- Seq(-1, 0)) {
      newProps.put(RemoteLogManagerConfig.REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS_PROP, timeoutMs.toString)
      assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(newProps, perBrokerConfig = true))
      assertThrows(classOf[ConfigException], () => config.dynamicConfig.validate(newProps, perBrokerConfig = false))
    }
  }

  @Test
  def testUpdateDynamicRemoteLogManagerConfig(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, "2")

    val config = KafkaConfig(origProps)
    val serverMock = Mockito.mock(classOf[KafkaBroker])
    val remoteLogManager = Mockito.mock(classOf[RemoteLogManager])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.remoteLogManagerOpt).thenReturn(Some(remoteLogManager))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    val props = new Properties()

    props.put(RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP, "4")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(4L, config.remoteLogManagerConfig.remoteLogIndexFileCacheTotalSizeBytes())
    Mockito.verify(remoteLogManager).resizeCacheSize(4)

    Mockito.verifyNoMoreInteractions(remoteLogManager)
  }

  @Test
  def testRemoteLogManagerCopyQuotaUpdates(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 9092)
    val config = KafkaConfig.fromProps(props)
    val serverMock: KafkaBroker = mock(classOf[KafkaBroker])
    val remoteLogManager = mock(classOf[RemoteLogManager])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.remoteLogManagerOpt).thenReturn(Some(remoteLogManager))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND,
      config.remoteLogManagerConfig.remoteLogManagerCopyMaxBytesPerSecond())

    // Update default config
    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP, "100")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(100, config.remoteLogManagerConfig.remoteLogManagerCopyMaxBytesPerSecond())
    verify(remoteLogManager).updateCopyQuota(100)

    // Update per broker config
    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP, "200")
    config.dynamicConfig.updateBrokerConfig(0, props)
    assertEquals(200, config.remoteLogManagerConfig.remoteLogManagerCopyMaxBytesPerSecond())
    verify(remoteLogManager).updateCopyQuota(200)

    verifyNoMoreInteractions(remoteLogManager)
  }

  @Test
  def testRemoteLogManagerFetchQuotaUpdates(): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 9092)
    val config = KafkaConfig.fromProps(props)
    val serverMock: KafkaBroker = mock(classOf[KafkaBroker])
    val remoteLogManager = mock(classOf[RemoteLogManager])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.remoteLogManagerOpt).thenReturn(Some(remoteLogManager))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND,
      config.remoteLogManagerConfig.remoteLogManagerFetchMaxBytesPerSecond())

    // Update default config
    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP, "100")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(100, config.remoteLogManagerConfig.remoteLogManagerFetchMaxBytesPerSecond())
    verify(remoteLogManager).updateFetchQuota(100)

    // Update per broker config
    props.put(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP, "200")
    config.dynamicConfig.updateBrokerConfig(0, props)
    assertEquals(200, config.remoteLogManagerConfig.remoteLogManagerFetchMaxBytesPerSecond())
    verify(remoteLogManager).updateFetchQuota(200)

    verifyNoMoreInteractions(remoteLogManager)
  }

  @Test
  def testRemoteLogManagerMultipleConfigUpdates(): Unit = {
    val indexFileCacheSizeProp = RemoteLogManagerConfig.REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES_PROP
    val copyQuotaProp = RemoteLogManagerConfig.REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND_PROP
    val fetchQuotaProp = RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND_PROP

    val props = TestUtils.createBrokerConfig(0, null, port = 9092)
    val config = KafkaConfig.fromProps(props)
    val serverMock: KafkaBroker = mock(classOf[KafkaBroker])
    val remoteLogManager = Mockito.mock(classOf[RemoteLogManager])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.remoteLogManagerOpt).thenReturn(Some(remoteLogManager))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicRemoteLogConfig(serverMock))

    // Default values
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_INDEX_FILE_CACHE_TOTAL_SIZE_BYTES,
      config.remoteLogManagerConfig.remoteLogIndexFileCacheTotalSizeBytes())
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_COPY_MAX_BYTES_PER_SECOND,
      config.remoteLogManagerConfig.remoteLogManagerCopyMaxBytesPerSecond())
    assertEquals(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_MANAGER_FETCH_MAX_BYTES_PER_SECOND,
      config.remoteLogManagerConfig.remoteLogManagerFetchMaxBytesPerSecond())

    // Update default config
    props.put(indexFileCacheSizeProp, "4")
    props.put(copyQuotaProp, "100")
    props.put(fetchQuotaProp, "200")
    config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(4, config.remoteLogManagerConfig.remoteLogIndexFileCacheTotalSizeBytes())
    assertEquals(100, config.remoteLogManagerConfig.remoteLogManagerCopyMaxBytesPerSecond())
    assertEquals(200, config.remoteLogManagerConfig.remoteLogManagerFetchMaxBytesPerSecond())
    verify(remoteLogManager).resizeCacheSize(4)
    verify(remoteLogManager).updateCopyQuota(100)
    verify(remoteLogManager).updateFetchQuota(200)

    // Update per broker config
    props.put(indexFileCacheSizeProp, "8")
    props.put(copyQuotaProp, "200")
    props.put(fetchQuotaProp, "400")
    config.dynamicConfig.updateBrokerConfig(0, props)
    assertEquals(8, config.remoteLogManagerConfig.remoteLogIndexFileCacheTotalSizeBytes())
    assertEquals(200, config.remoteLogManagerConfig.remoteLogManagerCopyMaxBytesPerSecond())
    assertEquals(400, config.remoteLogManagerConfig.remoteLogManagerFetchMaxBytesPerSecond())
    verify(remoteLogManager).resizeCacheSize(8)
    verify(remoteLogManager).updateCopyQuota(200)
    verify(remoteLogManager).updateFetchQuota(400)

    verifyNoMoreInteractions(remoteLogManager)
  }

  def verifyIncorrectLogLocalRetentionProps(logLocalRetentionMs: Long,
                                            retentionMs: Long,
                                            logLocalRetentionBytes: Long,
                                            retentionBytes: Long): Unit = {
    val props = TestUtils.createBrokerConfig(0, null, port = 8181)
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, retentionMs.toString)
    props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, retentionBytes.toString)
    val config = KafkaConfig(props)
    val dynamicLogConfig = new DynamicLogConfig(mock(classOf[LogManager]), mock(classOf[KafkaBroker]))
    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(dynamicLogConfig)

    val newProps = new Properties()
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, logLocalRetentionMs.toString)
    newProps.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, logLocalRetentionBytes.toString)
    // validate default config
    assertThrows(classOf[ConfigException], () =>  config.dynamicConfig.validate(newProps, perBrokerConfig = false))
    // validate per broker config
    assertThrows(classOf[ConfigException], () =>  config.dynamicConfig.validate(newProps, perBrokerConfig = true))
  }

  class DynamicLogConfigContext(origProps: Properties) {
    val config = KafkaConfig(origProps)
    val serverMock = Mockito.mock(classOf[BrokerServer])
    val logManagerMock = Mockito.mock(classOf[LogManager])

    Mockito.when(serverMock.config).thenReturn(config)
    Mockito.when(serverMock.logManager).thenReturn(logManagerMock)
    Mockito.when(logManagerMock.allLogs).thenReturn(Iterable.empty)

    val currentDefaultLogConfig = new AtomicReference(new LogConfig(new Properties))
    Mockito.when(logManagerMock.currentDefaultConfig).thenAnswer(_ => currentDefaultLogConfig.get())
    Mockito.when(logManagerMock.reconfigureDefaultLogConfig(ArgumentMatchers.any(classOf[LogConfig])))
      .thenAnswer(invocation => currentDefaultLogConfig.set(invocation.getArgument(0)))

    config.dynamicConfig.initialize(None, None)
    config.dynamicConfig.addBrokerReconfigurable(new DynamicLogConfig(logManagerMock, serverMock))
  }

  @Test
  def testDynamicLogConfigHandlesSynonymsCorrectly(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "1")
    val ctx = new DynamicLogConfigContext(origProps)
    assertEquals(TimeUnit.MINUTES.toMillis(1), ctx.config.logRetentionTimeMillis)

    val props = new Properties()
    props.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "12345678")
    ctx.config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(TimeUnit.MINUTES.toMillis(1), ctx.currentDefaultLogConfig.get().retentionMs)
  }

  @Test
  def testLogRetentionTimeMinutesIsNotDynamicallyReconfigurable(): Unit = {
    val origProps = TestUtils.createBrokerConfig(0, null, port = 8181)
    origProps.put(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "1")
    val ctx = new DynamicLogConfigContext(origProps)
    assertEquals(TimeUnit.HOURS.toMillis(1), ctx.config.logRetentionTimeMillis)

    val props = new Properties()
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG, "3")
    ctx.config.dynamicConfig.updateDefaultConfig(props)
    assertEquals(TimeUnit.HOURS.toMillis(1), ctx.config.logRetentionTimeMillis)
    assertFalse(ctx.currentDefaultLogConfig.get().originals().containsKey(ServerLogConfigs.LOG_RETENTION_TIME_MINUTES_CONFIG))
  }
}

class TestDynamicThreadPool() extends BrokerReconfigurable {

  override def reconfigurableConfigs: Set[String] = {
    DynamicThreadPool.ReconfigurableConfigs
  }

  override def reconfigure(oldConfig: KafkaConfig, newConfig: KafkaConfig): Unit = {
    assertEquals(ServerConfigs.NUM_IO_THREADS_DEFAULT, oldConfig.numIoThreads)
    assertEquals(ServerConfigs.BACKGROUND_THREADS_DEFAULT, oldConfig.backgroundThreads)

    assertEquals(10, newConfig.numIoThreads)
    assertEquals(100, newConfig.backgroundThreads)
  }

  override def validateReconfiguration(newConfig: KafkaConfig): Unit = {
    assertEquals(10, newConfig.numIoThreads)
    assertEquals(100, newConfig.backgroundThreads)
  }
}
