/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.io.File
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util
import java.util.concurrent._
import java.util.{Collections, Properties}

import javax.management.ObjectName
import kafka.controller.{ControllerBrokerStateInfo, ControllerChannelManager}
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server.AbstractDynamicBrokerReconfigurationTest.{SecureExternal, SecureInternal}
import kafka.utils.Implicits._
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import org.apache.kafka.clients.admin.{Config, ConfigEntry, ScramMechanism}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Disabled, Test}

import scala.collection.{Seq, _}
import scala.jdk.CollectionConverters._

//  private val trustStoreFile1 = File.createTempFile("truststore", ".jks")
//  private val trustStoreFile2 = File.createTempFile("truststore", ".jks")
//  private val sslProperties1 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile1), "kafka")
//  private val sslProperties2 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile2), "kafka")
//  private val invalidSslProperties = invalidSslConfigs
//
//  def addExtraProps(props: Properties): Unit = {
//  }
//
//  @BeforeEach
//  override def setUp(): Unit = {
//    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism)))
//    super.setUp()
//
//    clearLeftOverProcessorMetrics() // clear metrics left over from other tests so that new ones can be tested
//
//    (0 until numServers).foreach { brokerId =>
//
//      val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
//      props ++= securityProps(sslProperties1, TRUSTSTORE_PROPS)
//      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
//      props.put(KafkaConfig.ListenersProp, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
//      props.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"$SecureInternal:SSL, $SecureExternal:SASL_SSL")
//      props.put(KafkaConfig.InterBrokerListenerNameProp, SecureInternal)
//      props.put(KafkaConfig.SslClientAuthProp, "requested")
//      props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, "PLAIN")
//      props.put(KafkaConfig.ZkEnableSecureAclsProp, "true")
//      props.put(KafkaConfig.SaslEnabledMechanismsProp, kafkaServerSaslMechanisms.mkString(","))
//      props.put(KafkaConfig.LogSegmentBytesProp, "2000") // low value to test log rolling on config update
//      props.put(KafkaConfig.NumReplicaFetchersProp, "2") // greater than one to test reducing threads
//      props.put(KafkaConfig.PasswordEncoderSecretProp, "dynamic-config-secret")
//      props.put(KafkaConfig.LogRetentionTimeMillisProp, 1680000000.toString)
//      props.put(KafkaConfig.LogRetentionTimeHoursProp, 168.toString)
//      addExtraProps(props)
//
//      props ++= sslProperties1
//      props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(SecureInternal))
//
//      // Set invalid top-level properties to ensure that listener config is used
//      // Don't set any dynamic configs here since they get overridden in tests
//      props ++= invalidSslProperties
//      props ++= securityProps(invalidSslProperties, KEYSTORE_PROPS, "")
//      props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(SecureExternal))
//
//      val kafkaConfig = KafkaConfig.fromProps(props)
//      configureDynamicKeystoreInZooKeeper(kafkaConfig, sslProperties1)
//
//      servers += TestUtils.createServer(kafkaConfig)
//    }
//
//    TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor = numServers, servers)
//    TestUtils.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, servers.head.config.offsetsTopicPartitions,
//      replicationFactor = numServers, servers, servers.head.groupCoordinator.offsetsTopicConfigs)
//
//    createAdminClient(SecurityProtocol.SSL, SecureInternal)
//
//    TestMetricsReporter.testReporters.clear()
//  }
//
//  @AfterEach
//  override def tearDown(): Unit = {
//    clientThreads.foreach(_.interrupt())
//    clientThreads.foreach(_.initiateShutdown())
//    clientThreads.foreach(_.join(5 * 1000))
//    executors.foreach(_.shutdownNow())
//    producers.foreach(_.close(Duration.ZERO))
//    consumers.foreach(_.close(Duration.ofMillis(0)))
//    adminClients.foreach(_.close())
//    TestUtils.shutdownServers(servers)
//    super.tearDown()
//    closeSasl()
//  }
class DynamicBrokerReconfigurationTest extends AbstractDynamicBrokerReconfigurationTest {

  @Test
  def testConfigDescribeUsingAdminClient(): Unit = {

    def verifyConfig(configName: String, configEntry: ConfigEntry, isSensitive: Boolean, isReadOnly: Boolean,
                     expectedProps: Properties): Unit = {
      if (isSensitive) {
        assertTrue(configEntry.isSensitive, s"Value is sensitive: $configName")
        assertNull(configEntry.value, s"Sensitive value returned for $configName")
      } else {
        assertFalse(configEntry.isSensitive, s"Config is not sensitive: $configName")
        assertEquals(expectedProps.getProperty(configName), configEntry.value)
      }
      assertEquals(isReadOnly, configEntry.isReadOnly, s"isReadOnly incorrect for $configName: $configEntry")
    }

    def verifySynonym(configName: String, synonym: ConfigSynonym, isSensitive: Boolean,
                      expectedPrefix: String, expectedSource: ConfigSource, expectedProps: Properties): Unit = {
      if (isSensitive)
        assertNull(synonym.value, s"Sensitive value returned for $configName")
      else
        assertEquals(expectedProps.getProperty(configName), synonym.value)
      assertTrue(synonym.name.startsWith(expectedPrefix), s"Expected listener config, got $synonym")
      assertEquals(expectedSource, synonym.source)
    }

    def verifySynonyms(configName: String, synonyms: util.List[ConfigSynonym], isSensitive: Boolean,
                       prefix: String, defaultValue: Option[String]): Unit = {

      val overrideCount = if (prefix.isEmpty) 0 else 2
      assertEquals(1 + overrideCount + defaultValue.size, synonyms.size, s"Wrong synonyms for $configName: $synonyms")
      if (overrideCount > 0) {
        val listenerPrefix = "listener.name.external.ssl."
        verifySynonym(configName, synonyms.get(0), isSensitive, listenerPrefix, ConfigSource.DYNAMIC_BROKER_CONFIG, sslProperties1)
        verifySynonym(configName, synonyms.get(1), isSensitive, listenerPrefix, ConfigSource.STATIC_BROKER_CONFIG, sslProperties1)
      }
      verifySynonym(configName, synonyms.get(overrideCount), isSensitive, "ssl.", ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties)
      defaultValue.foreach { value =>
        val defaultProps = new Properties
        defaultProps.setProperty(configName, value)
        verifySynonym(configName, synonyms.get(overrideCount + 1), isSensitive, "ssl.", ConfigSource.DEFAULT_CONFIG, defaultProps)
      }
    }

    def verifySslConfig(prefix: String, expectedProps: Properties, configDesc: Config): Unit = {
      // Validate file-based SSL keystore configs
      val keyStoreProps = new util.HashSet[String](KEYSTORE_PROPS)
      keyStoreProps.remove(SSL_KEYSTORE_KEY_CONFIG)
      keyStoreProps.remove(SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG)
      keyStoreProps.forEach { configName =>
        val desc = configEntry(configDesc, s"$prefix$configName")
        val isSensitive = configName.contains("password")
        verifyConfig(configName, desc, isSensitive, isReadOnly = prefix.nonEmpty, if (prefix.isEmpty) invalidSslProperties else sslProperties1)
        val defaultValue = if (configName == SSL_KEYSTORE_TYPE_CONFIG) Some("JKS") else None
        verifySynonyms(configName, desc.synonyms, isSensitive, prefix, defaultValue)
      }
    }

    val adminClient = adminClients.head
    alterSslKeystoreUsingConfigCommand(sslProperties1, SecureExternal)

    val configDesc = describeConfig(adminClient)
    verifySslConfig("listener.name.external.", sslProperties1, configDesc)
    verifySslConfig("", invalidSslProperties, configDesc)

    // Verify a few log configs with and without synonyms
    val expectedProps = new Properties
    expectedProps.setProperty(KafkaConfig.LogRetentionTimeMillisProp, "1680000000")
    expectedProps.setProperty(KafkaConfig.LogRetentionTimeHoursProp, "168")
    expectedProps.setProperty(KafkaConfig.LogRollTimeHoursProp, "168")
    expectedProps.setProperty(KafkaConfig.LogCleanerThreadsProp, "1")
    val logRetentionMs = configEntry(configDesc, KafkaConfig.LogRetentionTimeMillisProp)
    verifyConfig(KafkaConfig.LogRetentionTimeMillisProp, logRetentionMs,
      isSensitive = false, isReadOnly = false, expectedProps)
    val logRetentionHours = configEntry(configDesc, KafkaConfig.LogRetentionTimeHoursProp)
    verifyConfig(KafkaConfig.LogRetentionTimeHoursProp, logRetentionHours,
      isSensitive = false, isReadOnly = true, expectedProps)
    val logRollHours = configEntry(configDesc, KafkaConfig.LogRollTimeHoursProp)
    verifyConfig(KafkaConfig.LogRollTimeHoursProp, logRollHours,
      isSensitive = false, isReadOnly = true, expectedProps)
    val logCleanerThreads = configEntry(configDesc, KafkaConfig.LogCleanerThreadsProp)
    verifyConfig(KafkaConfig.LogCleanerThreadsProp, logCleanerThreads,
      isSensitive = false, isReadOnly = false, expectedProps)

    def synonymsList(configEntry: ConfigEntry): List[(String, ConfigSource)] =
      configEntry.synonyms.asScala.map(s => (s.name, s.source)).toList
    assertEquals(List((KafkaConfig.LogRetentionTimeMillisProp, ConfigSource.STATIC_BROKER_CONFIG),
      (KafkaConfig.LogRetentionTimeHoursProp, ConfigSource.STATIC_BROKER_CONFIG),
      (KafkaConfig.LogRetentionTimeHoursProp, ConfigSource.DEFAULT_CONFIG)),
      synonymsList(logRetentionMs))
    assertEquals(List((KafkaConfig.LogRetentionTimeHoursProp, ConfigSource.STATIC_BROKER_CONFIG),
      (KafkaConfig.LogRetentionTimeHoursProp, ConfigSource.DEFAULT_CONFIG)),
      synonymsList(logRetentionHours))
    assertEquals(List((KafkaConfig.LogRollTimeHoursProp, ConfigSource.DEFAULT_CONFIG)), synonymsList(logRollHours))
    assertEquals(List((KafkaConfig.LogCleanerThreadsProp, ConfigSource.DEFAULT_CONFIG)), synonymsList(logCleanerThreads))
  }

  @Test
  def testUpdatesUsingConfigProvider(): Unit = {
    val PollingIntervalVal = f"$${file:polling.interval:interval}"
    val PollingIntervalUpdateVal = f"$${file:polling.interval:updinterval}"
    val SslTruststoreTypeVal = f"$${file:ssl.truststore.type:storetype}"
    val SslKeystorePasswordVal = f"$${file:ssl.keystore.password:password}"

    val configPrefix = listenerPrefix(SecureExternal)
    val brokerConfigs = describeConfig(adminClients.head, servers).entries.asScala
    // the following are values before updated
    assertFalse(brokerConfigs.exists(_.name == TestMetricsReporter.PollingIntervalProp), "Initial value of polling interval")
    assertFalse(brokerConfigs.exists(_.name == configPrefix + KafkaConfig.SslTruststoreTypeProp), "Initial value of ssl truststore type")
    assertNull(brokerConfigs.find(_.name == configPrefix+KafkaConfig.SslKeystorePasswordProp).get.value, "Initial value of ssl keystore password")

    // setup ssl properties
    val secProps = securityProps(sslProperties1, KEYSTORE_PROPS, configPrefix)

    // configure config providers and properties need be updated
    val updatedProps = new Properties
    updatedProps.setProperty("config.providers", "file")
    updatedProps.setProperty("config.providers.file.class", "kafka.server.MockFileConfigProvider")
    updatedProps.put(KafkaConfig.MetricReporterClassesProp, classOf[TestMetricsReporter].getName)

    // 1. update Integer property using config provider
    updatedProps.put(TestMetricsReporter.PollingIntervalProp, PollingIntervalVal)

    // 2. update String property using config provider
    updatedProps.put(configPrefix+KafkaConfig.SslTruststoreTypeProp, SslTruststoreTypeVal)

    // merge two properties
    updatedProps ++= secProps

    // 3. update password property using config provider
    updatedProps.put(configPrefix+KafkaConfig.SslKeystorePasswordProp, SslKeystorePasswordVal)

    alterConfigsUsingConfigCommand(updatedProps)
    waitForConfig(TestMetricsReporter.PollingIntervalProp, "1000")
    waitForConfig(configPrefix+KafkaConfig.SslTruststoreTypeProp, "JKS")
    waitForConfig(configPrefix+KafkaConfig.SslKeystorePasswordProp, "ServerPassword")

    // wait for MetricsReporter
    val reporters = TestMetricsReporter.waitForReporters(servers.size)
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 1000)
      assertFalse(reporter.kafkaMetrics.isEmpty, "No metrics found")
    }

    // fetch from ZK, values should be unresolved
    val props = fetchBrokerConfigsFromZooKeeper(servers.head)
    assertTrue(props.getProperty(TestMetricsReporter.PollingIntervalProp) == PollingIntervalVal, "polling interval is not updated in ZK")
    assertTrue(props.getProperty(configPrefix+KafkaConfig.SslTruststoreTypeProp) == SslTruststoreTypeVal, "store type is not updated in ZK")
    assertTrue(props.getProperty(configPrefix+KafkaConfig.SslKeystorePasswordProp) == SslKeystorePasswordVal, "keystore password is not updated in ZK")

    // verify the update
    // 1. verify update not occurring if the value of property is same.
    alterConfigsUsingConfigCommand(updatedProps)
    waitForConfig(TestMetricsReporter.PollingIntervalProp, "1000")
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 1000)
    }

    // 2. verify update occurring if the value of property changed.
    updatedProps.put(TestMetricsReporter.PollingIntervalProp, PollingIntervalUpdateVal)
    alterConfigsUsingConfigCommand(updatedProps)
    waitForConfig(TestMetricsReporter.PollingIntervalProp, "2000")
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 2000)
    }
  }

  @Test
  def testKeyStoreAlter(): Unit = {
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions, replicationFactor = numServers, servers)

    // Start a producer and consumer that work with the current broker keystore.
    // This should continue working while changes are made
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)
    TestUtils.waitUntilTrue(() => consumerThread.received >= 10, "Messages not received")

    // Producer with new truststore should fail to connect before keystore update
    val producer1 = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    verifyAuthenticationFailure(producer1)

    // Update broker keystore for external listener
    alterSslKeystoreUsingConfigCommand(sslProperties2, SecureExternal)

    // New producer with old truststore should fail to connect
    val producer2 = ProducerBuilder().trustStoreProps(sslProperties1).maxRetries(0).build()
    verifyAuthenticationFailure(producer2)

    // Produce/consume should work with new truststore with new producer/consumer
    val producer = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    val consumer = ConsumerBuilder("group1").trustStoreProps(sslProperties2).topic(topic2).build()
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should fail without update
    val adminClient = adminClients.head
    alterSslKeystore(adminClient, sslProperties2, SecureInternal, expectFailure = true)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with compatible keystore should succeed
    val sslPropertiesCopy = sslProperties1.clone().asInstanceOf[Properties]
    val oldFile = new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
    val newFile = File.createTempFile("keystore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    sslPropertiesCopy.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, newFile.getPath)
    alterSslKeystore(adminClient, sslPropertiesCopy, SecureInternal)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify that keystores can be updated using same file name.
    val reusableProps = sslProperties2.clone().asInstanceOf[Properties]
    val reusableFile = File.createTempFile("keystore", ".jks")
    reusableProps.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, reusableFile.getPath)
    Files.copy(new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath,
      reusableFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    alterSslKeystore(adminClient, reusableProps, SecureExternal)
    val producer3 = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    verifyAuthenticationFailure(producer3)
    // Now alter using same file name. We can't check if the update has completed by comparing config on
    // the broker, so we wait for producer operation to succeed to verify that the update has been performed.
    Files.copy(new File(sslProperties2.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath,
      reusableFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    reusableFile.setLastModified(System.currentTimeMillis() + 1000)
    alterSslKeystore(adminClient, reusableProps, SecureExternal)
    TestUtils.waitUntilTrue(() => {
      try {
        producer3.partitionsFor(topic).size() == numPartitions
      } catch {
        case _: Exception  => false
      }
    }, "Keystore not updated")

    // Verify that all messages sent with retries=0 while keystores were being altered were consumed
    stopAndVerifyProduceConsume(producerThread, consumerThread)
  }

  @Test
  def testTrustStoreAlter(): Unit = {
    val producerBuilder = ProducerBuilder().listenerName(SecureInternal).securityProtocol(SecurityProtocol.SSL)

    // Producer with new keystore should fail to connect before truststore update
    verifyAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties2).build())

    // Update broker truststore for SSL listener with both certificates
    val combinedStoreProps = mergeTrustStores(sslProperties1, sslProperties2)
    val prefix = listenerPrefix(SecureInternal)
    val existingDynamicProps = new Properties
    servers.head.config.dynamicConfig.currentDynamicBrokerConfigs.foreach { case (k, v) =>
      existingDynamicProps.put(k, v)
    }
    val newProps = new Properties
    newProps ++= existingDynamicProps
    newProps ++= securityProps(combinedStoreProps, TRUSTSTORE_PROPS, prefix)
    reconfigureServers(newProps, perBrokerConfig = true,
      (s"$prefix$SSL_TRUSTSTORE_LOCATION_CONFIG", combinedStoreProps.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)))

    def verifySslProduceConsume(keyStoreProps: Properties, group: String): Unit = {
      val producer = producerBuilder.keyStoreProps(keyStoreProps).build()
      val consumer = ConsumerBuilder(group)
        .listenerName(SecureInternal)
        .securityProtocol(SecurityProtocol.SSL)
        .keyStoreProps(keyStoreProps)
        .autoOffsetReset("latest")
        .build()
      verifyProduceConsume(producer, consumer, 10, topic)
    }

    // Produce/consume should work with old as well as new client keystore
    verifySslProduceConsume(sslProperties1, "alter-truststore-1")
    verifySslProduceConsume(sslProperties2, "alter-truststore-2")

    // Revert to old truststore with only one certificate and update. Clients should connect only with old keystore.
    val oldTruststoreProps = new Properties
    oldTruststoreProps ++= existingDynamicProps
    oldTruststoreProps ++= securityProps(sslProperties1, TRUSTSTORE_PROPS, prefix)
    reconfigureServers(oldTruststoreProps, perBrokerConfig = true,
      (s"$prefix$SSL_TRUSTSTORE_LOCATION_CONFIG", sslProperties1.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)))
    verifyAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties2).build())
    verifySslProduceConsume(sslProperties1, "alter-truststore-3")

    // Update same truststore file to contain both certificates without changing any configs.
    // Clients should connect successfully with either keystore after admin client AlterConfigsRequest completes.
    Files.copy(Paths.get(combinedStoreProps.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)),
      Paths.get(sslProperties1.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)),
      StandardCopyOption.REPLACE_EXISTING)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, oldTruststoreProps, perBrokerConfig = true).all.get()
    verifySslProduceConsume(sslProperties1, "alter-truststore-4")
    verifySslProduceConsume(sslProperties2, "alter-truststore-5")

    // Update internal keystore/truststore and validate new client connections from broker (e.g. controller).
    // Alter internal keystore from `sslProperties1` to `sslProperties2`, force disconnect of a controller connection
    // and verify that metadata is propagated for new topic.
    val props2 = securityProps(sslProperties2, KEYSTORE_PROPS, prefix)
    props2 ++= securityProps(combinedStoreProps, TRUSTSTORE_PROPS, prefix)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props2, perBrokerConfig = true).all.get(15, TimeUnit.SECONDS)
    verifySslProduceConsume(sslProperties2, "alter-truststore-6")
    props2 ++= securityProps(sslProperties2, TRUSTSTORE_PROPS, prefix)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props2, perBrokerConfig = true).all.get(15, TimeUnit.SECONDS)
    verifySslProduceConsume(sslProperties2, "alter-truststore-7")
    waitForAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties1))

    val controller = servers.find(_.config.brokerId == TestUtils.waitUntilControllerElected(zkClient)).get
    val controllerChannelManager = controller.kafkaController.controllerChannelManager
    val brokerStateInfo: mutable.HashMap[Int, ControllerBrokerStateInfo] =
      JTestUtils.fieldValue(controllerChannelManager, classOf[ControllerChannelManager], "brokerStateInfo")
    brokerStateInfo(0).networkClient.disconnect("0")
    TestUtils.createTopic(zkClient, "testtopic2", numPartitions, replicationFactor = numServers, servers)
  }

  @Test
  def testLogCleanerConfig(): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)

    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 1)

    val props = new Properties
    props.put(KafkaConfig.LogCleanerThreadsProp, "2")
    props.put(KafkaConfig.LogCleanerDedupeBufferSizeProp, "20000000")
    props.put(KafkaConfig.LogCleanerDedupeBufferLoadFactorProp, "0.8")
    props.put(KafkaConfig.LogCleanerIoBufferSizeProp, "300000")
    props.put(KafkaConfig.MessageMaxBytesProp, "40000")
    props.put(KafkaConfig.LogCleanerIoMaxBytesPerSecondProp, "50000000")
    props.put(KafkaConfig.LogCleanerBackoffMsProp, "6000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogCleanerThreadsProp, "2"))

    // Verify cleaner config was updated. Wait for one of the configs to be updated and verify
    // that all other others were updated at the same time since they are reconfigured together
    val newCleanerConfig = servers.head.logManager.cleaner.currentConfig
    TestUtils.waitUntilTrue(() => newCleanerConfig.numThreads == 2, "Log cleaner not reconfigured")
    assertEquals(20000000, newCleanerConfig.dedupeBufferSize)
    assertEquals(0.8, newCleanerConfig.dedupeBufferLoadFactor, 0.001)
    assertEquals(300000, newCleanerConfig.ioBufferSize)
    assertEquals(40000, newCleanerConfig.maxMessageSize)
    assertEquals(50000000, newCleanerConfig.maxIoBytesPerSecond, 50000000)
    assertEquals(6000, newCleanerConfig.backOffMs)

    // Verify thread count
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)

    // Stop a couple of threads and verify they are recreated if any config is updated
    def cleanerThreads = Thread.getAllStackTraces.keySet.asScala.filter(_.getName.startsWith("kafka-log-cleaner-thread-"))
    cleanerThreads.take(2).foreach(_.interrupt())
    TestUtils.waitUntilTrue(() => cleanerThreads.size == (2 * numServers) - 2, "Threads did not exit")
    props.put(KafkaConfig.LogCleanerBackoffMsProp, "8000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogCleanerBackoffMsProp, "8000"))
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)
  }

  @Test
  def testConsecutiveConfigChange(): Unit = {
    val topic2 = "testtopic2"
    val topicProps = new Properties
    topicProps.put(KafkaConfig.MinInSyncReplicasProp, "2")
    TestUtils.createTopic(zkClient, topic2, 1, replicationFactor = numServers, servers, topicProps)
    var log = servers.head.logManager.getLog(new TopicPartition(topic2, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    assertTrue(log.config.overriddenConfigs.contains(KafkaConfig.MinInSyncReplicasProp))
    assertEquals("2", log.config.originals().get(KafkaConfig.MinInSyncReplicasProp).toString)

    val props = new Properties
    props.put(KafkaConfig.MinInSyncReplicasProp, "3")
    // Make a broker-default config
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.MinInSyncReplicasProp, "3"))
    // Verify that all broker defaults have been updated again
    servers.foreach { server =>
      props.forEach { (k, v) =>
        assertEquals(v, server.config.originals.get(k).toString, s"Not reconfigured $k")
      }
    }

    log = servers.head.logManager.getLog(new TopicPartition(topic2, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    assertTrue(log.config.overriddenConfigs.contains(KafkaConfig.MinInSyncReplicasProp))
    assertEquals("2", log.config.originals().get(KafkaConfig.MinInSyncReplicasProp).toString) // Verify topic-level config survives

    // Make a second broker-default change
    props.clear()
    props.put(KafkaConfig.LogRetentionTimeMillisProp, "604800000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogRetentionTimeMillisProp, "604800000"))
    log = servers.head.logManager.getLog(new TopicPartition(topic2, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    assertTrue(log.config.overriddenConfigs.contains(KafkaConfig.MinInSyncReplicasProp))
    assertEquals("2", log.config.originals().get(KafkaConfig.MinInSyncReplicasProp).toString) // Verify topic-level config still survives
  }

  @Test
  def testDefaultTopicConfig(): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)

    val props = new Properties
    props.put(KafkaConfig.LogSegmentBytesProp, "4000")
    props.put(KafkaConfig.LogRollTimeMillisProp, TimeUnit.HOURS.toMillis(2).toString)
    props.put(KafkaConfig.LogRollTimeJitterMillisProp, TimeUnit.HOURS.toMillis(1).toString)
    props.put(KafkaConfig.LogIndexSizeMaxBytesProp, "100000")
    props.put(KafkaConfig.LogFlushIntervalMessagesProp, "1000")
    props.put(KafkaConfig.LogFlushIntervalMsProp, "60000")
    props.put(KafkaConfig.LogRetentionBytesProp, "10000000")
    props.put(KafkaConfig.LogRetentionTimeMillisProp, TimeUnit.DAYS.toMillis(1).toString)
    props.put(KafkaConfig.MessageMaxBytesProp, "100000")
    props.put(KafkaConfig.LogIndexIntervalBytesProp, "10000")
    props.put(KafkaConfig.LogCleanerDeleteRetentionMsProp, TimeUnit.DAYS.toMillis(1).toString)
    props.put(KafkaConfig.LogCleanerMinCompactionLagMsProp, "60000")
    props.put(KafkaConfig.LogDeleteDelayMsProp, "60000")
    props.put(KafkaConfig.LogCleanerMinCleanRatioProp, "0.3")
    props.put(KafkaConfig.LogCleanupPolicyProp, "delete")
    props.put(KafkaConfig.UncleanLeaderElectionEnableProp, "false")
    props.put(KafkaConfig.MinInSyncReplicasProp, "2")
    props.put(KafkaConfig.CompressionTypeProp, "gzip")
    props.put(KafkaConfig.LogPreAllocateProp, true.toString)
    props.put(KafkaConfig.LogMessageTimestampTypeProp, TimestampType.LOG_APPEND_TIME.toString)
    props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, "1000")
    props.put(KafkaConfig.LogMessageDownConversionEnableProp, "false")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogSegmentBytesProp, "4000"))

    // Verify that all broker defaults have been updated
    servers.foreach { server =>
      props.forEach { (k, v) =>
        assertEquals(server.config.originals.get(k).toString, v, s"Not reconfigured $k")
      }
    }

    // Verify that configs of existing logs have been updated
    val newLogConfig = LogConfig(LogConfig.extractLogConfigMap(servers.head.config))
    TestUtils.waitUntilTrue(() => servers.head.logManager.currentDefaultConfig == newLogConfig,
      "Config not updated in LogManager")

    val log = servers.head.logManager.getLog(new TopicPartition(topic, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    TestUtils.waitUntilTrue(() => log.config.segmentSize == 4000, "Existing topic config using defaults not updated")
    props.asScala.foreach { case (k, v) =>
      val logConfigName = DynamicLogConfig.KafkaConfigToLogConfigName(k)
      val expectedValue = if (k == KafkaConfig.LogCleanupPolicyProp) s"[$v]" else v
      assertEquals(expectedValue, log.config.originals.get(logConfigName).toString,
        s"Not reconfigured $logConfigName for existing log")
    }
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.LOG_APPEND_TIME)

    // Verify that the new config is actually used for new segments of existing logs
    TestUtils.waitUntilTrue(() => log.logSegments.exists(_.size > 3000), "Log segment size increase not applied")

    // Verify that overridden topic configs are not updated when broker default is updated
    val log2 = servers.head.logManager.getLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))
      .getOrElse(throw new IllegalStateException("Log not found"))
    assertFalse(log2.config.delete, "Overridden clean up policy should not be updated")
    assertEquals(ProducerCompressionCodec.name, log2.config.compressionType)

    // Verify that we can alter subset of log configs
    props.clear()
    props.put(KafkaConfig.LogMessageTimestampTypeProp, TimestampType.CREATE_TIME.toString)
    props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, "1000")
    reconfigureServers(props, perBrokerConfig = false, (KafkaConfig.LogMessageTimestampTypeProp, TimestampType.CREATE_TIME.toString))
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.CREATE_TIME)
    // Verify that invalid configs are not applied
    val invalidProps = Map(
      KafkaConfig.LogMessageTimestampDifferenceMaxMsProp -> "abc", // Invalid type
      KafkaConfig.LogMessageTimestampTypeProp -> "invalid", // Invalid value
      KafkaConfig.LogRollTimeMillisProp -> "0" // Fails KafkaConfig validation
    )
    invalidProps.foreach { case (k, v) =>
      val newProps = new Properties
      newProps ++= props
      props.put(k, v)
      reconfigureServers(props, perBrokerConfig = false, (k, props.getProperty(k)), expectFailure = true)
    }

    // Verify that even though broker defaults can be defined at default cluster level for consistent
    // configuration across brokers, they can also be defined at per-broker level for testing
    props.clear()
    props.put(KafkaConfig.LogIndexSizeMaxBytesProp, "500000")
    props.put(KafkaConfig.LogRetentionTimeMillisProp, TimeUnit.DAYS.toMillis(2).toString)
    alterConfigsOnServer(servers.head, props)
    assertEquals(500000, servers.head.config.values.get(KafkaConfig.LogIndexSizeMaxBytesProp))
    assertEquals(TimeUnit.DAYS.toMillis(2), servers.head.config.values.get(KafkaConfig.LogRetentionTimeMillisProp))
    servers.tail.foreach { server =>
      assertEquals(Defaults.LogIndexSizeMaxBytes, server.config.values.get(KafkaConfig.LogIndexSizeMaxBytesProp))
      assertEquals(1680000000L, server.config.values.get(KafkaConfig.LogRetentionTimeMillisProp))
    }

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)

    // Verify that configuration at both per-broker level and default cluster level could be deleted and
    // the default value should be restored
    props.clear()
    props.put(KafkaConfig.LogRetentionTimeMillisProp, "")
    props.put(KafkaConfig.LogIndexSizeMaxBytesProp, "")
    TestUtils.incrementalAlterConfigs(servers.take(1), adminClients.head, props, perBrokerConfig = true, opType = OpType.DELETE).all.get
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props, perBrokerConfig = false, opType = OpType.DELETE).all.get
    servers.foreach { server =>
      waitForConfigOnServer(server, KafkaConfig.LogRetentionTimeMillisProp, 1680000000.toString)
    }
    servers.foreach { server =>
      val log = server.logManager.getLog(new TopicPartition(topic, 0)).getOrElse(throw new IllegalStateException("Log not found"))
      // Verify default values for these two configurations are restored on all brokers
      TestUtils.waitUntilTrue(() => log.config.maxIndexSize == Defaults.LogIndexSizeMaxBytes && log.config.retentionMs == 1680000000L,
        "Existing topic config using defaults not updated")
    }
  }

  @Test
  def testUncleanLeaderElectionEnable(): Unit = {
    val controller = servers.find(_.config.brokerId == TestUtils.waitUntilControllerElected(zkClient)).get
    val controllerId = controller.config.brokerId

    // Create a topic with two replicas on brokers other than the controller
    val topic = "testtopic2"
    val assignment = Map(0 -> Seq((controllerId + 1) % servers.size, (controllerId + 2) % servers.size))
    TestUtils.createTopic(zkClient, topic, assignment, servers)

    val producer = ProducerBuilder().acks(1).build()
    val consumer = ConsumerBuilder("unclean-leader-test").enableAutoCommit(false).topic(topic).build()
    verifyProduceConsume(producer, consumer, numRecords = 10, topic)
    consumer.commitSync()

    def partitionInfo: TopicPartitionInfo =
      adminClients.head.describeTopics(Collections.singleton(topic)).values.get(topic).get().partitions().get(0)

    val partitionInfo0 = partitionInfo
    assertEquals(partitionInfo0.replicas.get(0), partitionInfo0.leader)
    val leaderBroker = servers.find(_.config.brokerId == partitionInfo0.replicas.get(0).id).get
    val followerBroker = servers.find(_.config.brokerId == partitionInfo0.replicas.get(1).id).get

    // Stop follower
    followerBroker.shutdown()
    followerBroker.awaitShutdown()

    // Produce and consume some messages when the only follower is down, this should succeed since MinIsr is 1
    verifyProduceConsume(producer, consumer, numRecords = 10, topic)
    consumer.commitSync()

    // Shutdown leader and startup follower
    leaderBroker.shutdown()
    leaderBroker.awaitShutdown()
    followerBroker.startup()

    // Verify that new leader is not elected with unclean leader disabled since there are no ISRs
    TestUtils.waitUntilTrue(() => partitionInfo.leader == null, "Unclean leader elected")

    // Enable unclean leader election
    val newProps = new Properties
    newProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, newProps, perBrokerConfig = false).all.get
    waitForConfigOnServer(controller, KafkaConfig.UncleanLeaderElectionEnableProp, "true")

    // Verify that the old follower with missing records is elected as the new leader
    val (newLeader, elected) = TestUtils.computeUntilTrue(partitionInfo.leader)(leader => leader != null)
    assertTrue(elected, "Unclean leader not elected")
    assertEquals(followerBroker.config.brokerId, newLeader.id)

    // New leader doesn't have the last 10 records committed on the old leader that have already been consumed.
    // With unclean leader election enabled, we should be able to produce to the new leader. The first 10 records
    // produced will not be consumed since they have offsets less than the consumer's committed offset.
    // Next 10 records produced should be consumed.
    (1 to 10).map(i => new ProducerRecord(topic, s"key$i", s"value$i"))
      .map(producer.send)
      .map(_.get(10, TimeUnit.SECONDS))
    verifyProduceConsume(producer, consumer, numRecords = 10, topic)
    consumer.commitSync()
  }

  @Test
  def testThreadPoolResize(): Unit = {
    val requestHandlerPrefix = "data-plane-kafka-request-handler-"
    val networkThreadPrefix = "data-plane-kafka-network-thread-"
    val fetcherThreadPrefix = "ReplicaFetcherThread-"
    // Executor threads and recovery threads are not verified since threads may not be running
    // For others, thread count should be configuredCount * threadMultiplier * numBrokers
    val threadMultiplier = Map(
      requestHandlerPrefix -> 1,
      networkThreadPrefix -> 2, // 2 endpoints
      fetcherThreadPrefix -> (servers.size - 1)
    )

    // Tolerate threads left over from previous tests
    def leftOverThreadCount(prefix: String, perBrokerCount: Int): Int = {
      val count = matchingThreads(prefix).size - perBrokerCount * servers.size * threadMultiplier(prefix)
      if (count > 0) count else 0
    }

    val leftOverThreads = Map(
      requestHandlerPrefix -> leftOverThreadCount(requestHandlerPrefix, servers.head.config.numIoThreads),
      networkThreadPrefix -> leftOverThreadCount(networkThreadPrefix, servers.head.config.numNetworkThreads),
      fetcherThreadPrefix -> leftOverThreadCount(fetcherThreadPrefix, servers.head.config.numReplicaFetchers)
    )

    def maybeVerifyThreadPoolSize(propName: String, size: Int, threadPrefix: String): Unit = {
      val ignoreCount = leftOverThreads.getOrElse(threadPrefix, 0)
      val expectedCountPerBroker = threadMultiplier.getOrElse(threadPrefix, 0) * size
      if (expectedCountPerBroker > 0)
        verifyThreads(threadPrefix, expectedCountPerBroker, ignoreCount)
    }

    def reducePoolSize(propName: String, currentSize: => Int, threadPrefix: String): Int = {
      val newSize = if (currentSize / 2 == 0) 1 else currentSize / 2
      resizeThreadPool(propName, newSize, threadPrefix)
      newSize
    }

    def increasePoolSize(propName: String, currentSize: => Int, threadPrefix: String): Int = {
      val newSize = if (currentSize == 1) currentSize * 2 else currentSize * 2 - 1
      resizeThreadPool(propName, newSize, threadPrefix)
      newSize
    }

    def resizeThreadPool(propName: String, newSize: Int, threadPrefix: String): Unit = {
      val props = new Properties
      props.put(propName, newSize.toString)
      reconfigureServers(props, perBrokerConfig = false, (propName, newSize.toString))
      maybeVerifyThreadPoolSize(propName, newSize, threadPrefix)
    }

    def verifyThreadPoolResize(propName: String, currentSize: => Int, threadPrefix: String, mayReceiveDuplicates: Boolean): Unit = {
      maybeVerifyThreadPoolSize(propName, currentSize, threadPrefix)
      val numRetries = if (mayReceiveDuplicates) 100 else 0
      val (producerThread, consumerThread) = startProduceConsume(retries = numRetries)
      var threadPoolSize = currentSize
      (1 to 2).foreach { _ =>
        threadPoolSize = reducePoolSize(propName, threadPoolSize, threadPrefix)
        Thread.sleep(100)
        threadPoolSize = increasePoolSize(propName, threadPoolSize, threadPrefix)
        Thread.sleep(100)
      }
      stopAndVerifyProduceConsume(producerThread, consumerThread, mayReceiveDuplicates)
      // Verify that all threads are alive
      maybeVerifyThreadPoolSize(propName, threadPoolSize, threadPrefix)
    }

    val config = servers.head.config
    verifyThreadPoolResize(KafkaConfig.NumIoThreadsProp, config.numIoThreads,
      requestHandlerPrefix, mayReceiveDuplicates = false)
    verifyThreadPoolResize(KafkaConfig.NumReplicaFetchersProp, config.numReplicaFetchers,
      fetcherThreadPrefix, mayReceiveDuplicates = false)
    verifyThreadPoolResize(KafkaConfig.BackgroundThreadsProp, config.backgroundThreads,
      "kafka-scheduler-", mayReceiveDuplicates = false)
    verifyThreadPoolResize(KafkaConfig.NumRecoveryThreadsPerDataDirProp, config.numRecoveryThreadsPerDataDir,
      "", mayReceiveDuplicates = false)
    verifyThreadPoolResize(KafkaConfig.NumNetworkThreadsProp, config.numNetworkThreads,
      networkThreadPrefix, mayReceiveDuplicates = true)
    verifyThreads("data-plane-kafka-socket-acceptor-", config.listeners.size)

    verifyProcessorMetrics()
    verifyMarkPartitionsForTruncation()
  }

  @Test
  def testMetricsReporterUpdate(): Unit = {
    // Add a new metrics reporter
    val newProps = new Properties
    newProps.put(TestMetricsReporter.PollingIntervalProp, "100")
    configureMetricsReporters(Seq(classOf[TestMetricsReporter]), newProps)

    val reporters = TestMetricsReporter.waitForReporters(servers.size)
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 100)
      assertFalse(reporter.kafkaMetrics.isEmpty, "No metrics found")
      reporter.verifyMetricValue("request-total", "socket-server-metrics")
    }
    assertEquals(servers.map(_.config.brokerId).toSet, TestMetricsReporter.configuredBrokers.toSet)

    // non-default value to trigger a new metric
    val clientId = "test-client-1"
    servers.foreach { server =>
      server.quotaManagers.produce.updateQuota(None, Some(clientId), Some(clientId),
        Some(Quota.upperBound(10000000)))
    }
    val (producerThread, consumerThread) = startProduceConsume(retries = 0, clientId)
    TestUtils.waitUntilTrue(() => consumerThread.received >= 5, "Messages not sent")

    // Verify that JMX reporter is still active (test a metric registered after the dynamic reporter update)
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val byteRate = mbeanServer.getAttribute(new ObjectName(s"kafka.server:type=Produce,client-id=$clientId"), "byte-rate")
    assertTrue(byteRate.asInstanceOf[Double] > 0, "JMX attribute not updated")

    // Property not related to the metrics reporter config should not reconfigure reporter
    newProps.setProperty("some.prop", "some.value")
    reconfigureServers(newProps, perBrokerConfig = false, (TestMetricsReporter.PollingIntervalProp, "100"))
    reporters.foreach(_.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 100))

    // Update of custom config of metrics reporter should reconfigure reporter
    newProps.put(TestMetricsReporter.PollingIntervalProp, "1000")
    reconfigureServers(newProps, perBrokerConfig = false, (TestMetricsReporter.PollingIntervalProp, "1000"))
    reporters.foreach(_.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 1000))

    // Verify removal of metrics reporter
    configureMetricsReporters(Seq.empty[Class[_]], newProps)
    reporters.foreach(_.verifyState(reconfigureCount = 1, deleteCount = 1, pollingInterval = 1000))
    TestMetricsReporter.testReporters.clear()

    // Verify recreation of metrics reporter
    newProps.put(TestMetricsReporter.PollingIntervalProp, "2000")
    configureMetricsReporters(Seq(classOf[TestMetricsReporter]), newProps)
    val newReporters = TestMetricsReporter.waitForReporters(servers.size)
    newReporters.foreach(_.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 2000))

    // Verify that validation failure of metrics reporter fails reconfiguration and leaves config unchanged
    newProps.put(KafkaConfig.MetricReporterClassesProp, "unknownMetricsReporter")
    reconfigureServers(newProps, perBrokerConfig = false, (TestMetricsReporter.PollingIntervalProp, "2000"), expectFailure = true)
    servers.foreach { server =>
      assertEquals(classOf[TestMetricsReporter].getName, server.config.originals.get(KafkaConfig.MetricReporterClassesProp))
    }
    newReporters.foreach(_.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 2000))

    // Verify that validation failure of custom config fails reconfiguration and leaves config unchanged
    newProps.put(TestMetricsReporter.PollingIntervalProp, "invalid")
    reconfigureServers(newProps, perBrokerConfig = false, (TestMetricsReporter.PollingIntervalProp, "2000"), expectFailure = true)
    newReporters.foreach(_.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 2000))

    // Delete reporters
    configureMetricsReporters(Seq.empty[Class[_]], newProps)
    TestMetricsReporter.testReporters.clear()

    // Verify that even though metrics reporters can be defined at default cluster level for consistent
    // configuration across brokers, they can also be defined at per-broker level for testing
    newProps.put(KafkaConfig.MetricReporterClassesProp, classOf[TestMetricsReporter].getName)
    newProps.put(TestMetricsReporter.PollingIntervalProp, "4000")
    alterConfigsOnServer(servers.head, newProps)
    TestUtils.waitUntilTrue(() => !TestMetricsReporter.testReporters.isEmpty, "Metrics reporter not created")
    val perBrokerReporter = TestMetricsReporter.waitForReporters(1).head
    perBrokerReporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 4000)

    // update TestMetricsReporter.PollingIntervalProp to 3000
    newProps.put(TestMetricsReporter.PollingIntervalProp, "3000")
    alterConfigsOnServer(servers.head, newProps)
    perBrokerReporter.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 3000)

    servers.tail.foreach { server => assertEquals("", server.config.originals.get(KafkaConfig.MetricReporterClassesProp)) }

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)
  }

  @Test
  def testAdvertisedListenerUpdate(): Unit = {
    val adminClient = adminClients.head
    val externalAdminClient = createAdminClient(SecurityProtocol.SASL_SSL, SecureExternal)

    // Ensure connections are made to brokers before external listener is made inaccessible
    describeConfig(externalAdminClient)

    // Update broker external listener to use invalid listener address
    // any address other than localhost is sufficient to fail (either connection or host name verification failure)
    val invalidHost = "192.168.0.1"
    alterAdvertisedListener(adminClient, externalAdminClient, "localhost", invalidHost)

    def validateEndpointsInZooKeeper(server: KafkaServer, endpointMatcher: String => Boolean): Unit = {
      val brokerInfo = zkClient.getBroker(server.config.brokerId)
      assertTrue(brokerInfo.nonEmpty, "Broker not registered")
      val endpoints = brokerInfo.get.endPoints.toString
      assertTrue(endpointMatcher(endpoints), s"Endpoint update not saved $endpoints")
    }

    // Verify that endpoints have been updated in ZK for all brokers
    servers.foreach(validateEndpointsInZooKeeper(_, endpoints => endpoints.contains(invalidHost)))

    // Trigger session expiry and ensure that controller registers new advertised listener after expiry
    val controllerEpoch = zkClient.getControllerEpoch
    val controllerServer = servers(zkClient.getControllerId.getOrElse(throw new IllegalStateException("No controller")))
    val controllerZkClient = controllerServer.zkClient
    val sessionExpiringClient = createZooKeeperClientToTriggerSessionExpiry(controllerZkClient.currentZooKeeper)
    sessionExpiringClient.close()
    TestUtils.waitUntilTrue(() => zkClient.getControllerEpoch != controllerEpoch,
      "Controller not re-elected after ZK session expiry")
    TestUtils.retry(10000)(validateEndpointsInZooKeeper(controllerServer, endpoints => endpoints.contains(invalidHost)))

    // Verify that producer connections fail since advertised listener is invalid
    val bootstrap = TestUtils.bootstrapServers(servers, new ListenerName(SecureExternal))
      .replaceAll(invalidHost, "localhost") // allow bootstrap connection to succeed
    val producer1 = ProducerBuilder()
      .trustStoreProps(sslProperties1)
      .maxRetries(0)
      .requestTimeoutMs(1000)
      .deliveryTimeoutMs(1000)
      .bootstrapServers(bootstrap)
      .build()

    val future = producer1.send(new ProducerRecord(topic, "key", "value"))
    assertTrue(assertThrows(classOf[ExecutionException], () => future.get(2, TimeUnit.SECONDS))
      .getCause.isInstanceOf[org.apache.kafka.common.errors.TimeoutException])

    alterAdvertisedListener(adminClient, externalAdminClient, invalidHost, "localhost")
    servers.foreach(validateEndpointsInZooKeeper(_, endpoints => !endpoints.contains(invalidHost)))

    // Verify that produce/consume work now
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions, replicationFactor = numServers, servers)
    val producer = ProducerBuilder().trustStoreProps(sslProperties1).maxRetries(0).build()
    val consumer = ConsumerBuilder("group2").trustStoreProps(sslProperties1).topic(topic2).build()
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify updating inter-broker listener
    val props = new Properties
    props.put(KafkaConfig.InterBrokerListenerNameProp, SecureExternal)
    val e = assertThrows(classOf[ExecutionException], () => reconfigureServers(props, perBrokerConfig = true, (KafkaConfig.InterBrokerListenerNameProp, SecureExternal)))
    assertTrue(e.getCause.isInstanceOf[InvalidRequestException], s"Unexpected exception ${e.getCause}")
    servers.foreach(server => assertEquals(SecureInternal, server.config.interBrokerListenerName.value))
  }

  @Test
  @Disabled // Re-enable once we make it less flaky (KAFKA-6824)
  def testAddRemoveSslListener(): Unit = {
    verifyAddListener("SSL", SecurityProtocol.SSL, Seq.empty)

    // Restart servers and check secret rotation
    servers.foreach(_.shutdown())
    servers.foreach(_.awaitShutdown())
    adminClients.foreach(_.close())
    adminClients.clear()

    // All passwords are currently encoded with password.encoder.secret. Encode with password.encoder.old.secret
    // and update ZK. When each server is started, it should decode using password.encoder.old.secret and update
    // ZK with newly encoded values using password.encoder.secret.
    servers.foreach { server =>
      val props = adminZkClient.fetchEntityConfig(ConfigType.Broker, server.config.brokerId.toString)
      val propsEncodedWithOldSecret = props.clone().asInstanceOf[Properties]
      val config = server.config
      val oldSecret = "old-dynamic-config-secret"
      config.dynamicConfig.staticBrokerConfigs.put(KafkaConfig.PasswordEncoderOldSecretProp, oldSecret)
      val passwordConfigs = props.asScala.filter { case (k, _) => DynamicBrokerConfig.isPasswordConfig(k) }
      assertTrue(passwordConfigs.nonEmpty, "Password configs not found")
      val passwordDecoder = createPasswordEncoder(config, config.passwordEncoderSecret)
      val passwordEncoder = createPasswordEncoder(config, Some(new Password(oldSecret)))
      passwordConfigs.foreach { case (name, value) =>
        val decoded = passwordDecoder.decode(value).value
        propsEncodedWithOldSecret.put(name, passwordEncoder.encode(new Password(decoded)))
      }
      val brokerId = server.config.brokerId
      adminZkClient.changeBrokerConfig(Seq(brokerId), propsEncodedWithOldSecret)
      val updatedProps = adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.toString)
      passwordConfigs.foreach { case (name, value) => assertNotEquals(props.get(value), updatedProps.get(name)) }

      server.startup()
      TestUtils.retry(10000) {
        val newProps = adminZkClient.fetchEntityConfig(ConfigType.Broker, brokerId.toString)
        passwordConfigs.foreach { case (name, value) =>
          assertEquals(passwordDecoder.decode(value), passwordDecoder.decode(newProps.getProperty(name))) }
      }
    }

    verifyListener(SecurityProtocol.SSL, None, "add-ssl-listener-group2")
    createAdminClient(SecurityProtocol.SSL, SecureInternal)
    verifyRemoveListener("SSL", SecurityProtocol.SSL, Seq.empty)
  }

  @Test
  def testAddRemoveSaslListeners(): Unit = {
    createScramCredentials(adminClients.head, JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)
    createScramCredentials(adminClients.head, JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword)
    initializeKerberos()
    // make sure each server's credential cache has all the created credentials
    // (check after initializing Kerberos to minimize delays)
    List(JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramAdmin).foreach { scramUser =>
      servers.foreach { server =>
        ScramMechanism.values().filter(_ != ScramMechanism.UNKNOWN).foreach(mechanism =>
          TestUtils.waitUntilTrue(() => server.credentialProvider.credentialCache.cache(
            mechanism.mechanismName(), classOf[ScramCredential]).get(scramUser) != null,
            s"$mechanism credentials not created for $scramUser"))
      }}

    //verifyAddListener("SASL_SSL", SecurityProtocol.SASL_SSL, Seq("SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"))
    verifyAddListener("SASL_PLAINTEXT", SecurityProtocol.SASL_PLAINTEXT, Seq("GSSAPI"))
    //verifyRemoveListener("SASL_SSL", SecurityProtocol.SASL_SSL, Seq("SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"))
    verifyRemoveListener("SASL_PLAINTEXT", SecurityProtocol.SASL_PLAINTEXT, Seq("GSSAPI"))

    // Verify that a listener added to a subset of servers doesn't cause any issues
    // when metadata is processed by the client.
    addListener(servers.tail, "SCRAM_LISTENER", SecurityProtocol.SASL_PLAINTEXT, Seq("SCRAM-SHA-256"))
    val bootstrap = TestUtils.bootstrapServers(servers.tail, new ListenerName("SCRAM_LISTENER"))
    val producer = ProducerBuilder().bootstrapServers(bootstrap)
      .securityProtocol(SecurityProtocol.SASL_PLAINTEXT)
      .saslMechanism("SCRAM-SHA-256")
      .maxRetries(1000)
      .build()
    val partitions = producer.partitionsFor(topic).asScala
    assertEquals(0, partitions.count(p => p.leader != null && p.leader.id == servers.head.config.brokerId))
    assertTrue(partitions.exists(_.leader == null), "Did not find partitions with no leader")
  }
}
