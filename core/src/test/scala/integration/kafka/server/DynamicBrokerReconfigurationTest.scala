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

import java.io.{Closeable, File, IOException, Reader, StringReader}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.lang.management.ManagementFactory
import java.security.KeyStore
import java.time.Duration
import java.util
import java.util.{Collections, Optional, Properties}
import java.util.concurrent._
import javax.management.ObjectName
import com.yammer.metrics.core.MetricName
import kafka.admin.ConfigCommand
import kafka.api.SaslSetup
import kafka.log.UnifiedLog
import kafka.network.{DataPlaneAcceptor, Processor, RequestChannel}
import kafka.security.JaasTestUtils
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, Reconfigurable, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.config.provider.FileConfigProvider
import org.apache.kafka.common.errors.{AuthenticationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{JmxReporter, KafkaMetric, MetricsContext, MetricsReporter, Quota}
import org.apache.kafka.common.network.{ConnectionMode, ListenerName}
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs, ServerTopicConfigSynonyms}
import org.apache.kafka.server.metrics.{KafkaYammerMetrics, MetricConfigs}
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.server.util.ShutdownableThread
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.apache.kafka.test.TestSslUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import java.util.concurrent.atomic.AtomicInteger
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.collection.Seq

object DynamicBrokerReconfigurationTest {
  val Plain = "PLAIN"
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"
}

class DynamicBrokerReconfigurationTest extends QuorumTestHarness with SaslSetup {

  import DynamicBrokerReconfigurationTest._

  private val servers = new ArrayBuffer[KafkaBroker]
  private val numServers = 3
  private val numPartitions = 10
  private val producers = new ArrayBuffer[KafkaProducer[String, String]]
  private val consumers = new ArrayBuffer[Consumer[String, String]]
  private val adminClients = new ArrayBuffer[Admin]()
  private val clientThreads = new ArrayBuffer[ShutdownableThread]()
  private val executors = new ArrayBuffer[ExecutorService]
  private val topic = "testtopic"

  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")

  private val trustStoreFile1 = TestUtils.tempFile("truststore", ".jks")
  private val trustStoreFile2 = TestUtils.tempFile("truststore", ".jks")
  private val sslProperties1 = JaasTestUtils.sslConfigs(ConnectionMode.SERVER, false, Optional.of(trustStoreFile1), "kafka")
  private val sslProperties2 = JaasTestUtils.sslConfigs(ConnectionMode.SERVER, false, Optional.of(trustStoreFile2), "kafka")
  private val invalidSslProperties = invalidSslConfigs

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val propsSeq = super.kraftControllerConfigs(testInfo)
    propsSeq.foreach(props => props.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_INTERVAL_MS_CONFIG, "100"))
    propsSeq
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism)))
    super.setUp(testInfo)

    clearLeftOverProcessorMetrics() // clear metrics left over from other tests so that new ones can be tested

    (0 until numServers).foreach { brokerId =>

      val props = TestUtils.createBrokerConfig(brokerId, null)
      props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
      props ++= securityProps(sslProperties1, TRUSTSTORE_PROPS)
      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
      props.put(SocketServerConfigs.LISTENERS_CONFIG, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
      props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, s"PLAINTEXT:PLAINTEXT, $SecureInternal:SSL, $SecureExternal:SASL_SSL, CONTROLLER:$controllerListenerSecurityProtocol")
      props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, SecureInternal)
      props.put(BrokerSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "requested")
      props.put(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, "PLAIN")
      props.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, kafkaServerSaslMechanisms.mkString(","))
      props.put(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, "2000") // low value to test log rolling on config update
      props.put(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, "2") // greater than one to test reducing threads
      props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, 1680000000.toString)
      props.put(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, 168.toString)

      props ++= sslProperties1
      props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(SecureInternal))

      // Set invalid top-level properties to ensure that listener config is used
      // Don't set any dynamic configs here since they get overridden in tests
      props ++= invalidSslProperties
      props ++= securityProps(invalidSslProperties, KEYSTORE_PROPS)
      props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(SecureExternal))

      val kafkaConfig = KafkaConfig.fromProps(props)

      servers += createBroker(kafkaConfig)
    }

    createAdminClient(SecurityProtocol.SSL, SecureInternal)

    TestUtils.createTopicWithAdmin(adminClients.head, topic, servers, controllerServers, numPartitions, replicationFactor = numServers)
    TestUtils.createTopicWithAdmin(adminClients.head, Topic.GROUP_METADATA_TOPIC_NAME, servers, controllerServers,
      numPartitions = servers.head.config.groupCoordinatorConfig.offsetsTopicPartitions,
      replicationFactor = numServers,
      topicConfig = servers.head.groupCoordinator.groupMetadataTopicConfigs)

    TestMetricsReporter.testReporters.clear()
  }

  @AfterEach
  override def tearDown(): Unit = {
    clientThreads.foreach(_.interrupt())
    clientThreads.foreach(_.initiateShutdown())
    clientThreads.foreach(_.join(5 * 1000))
    executors.foreach(_.shutdownNow())
    producers.foreach(_.close(Duration.ZERO))
    consumers.foreach(_.close(Duration.ofMillis(0)))
    adminClients.foreach(_.close())
    TestUtils.shutdownServers(servers)
    super.tearDown()
    closeSasl()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConfigDescribeUsingAdminClient(quorum: String, groupProtocol: String): Unit = {

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
        verifyConfig(configName, desc, isSensitive, isReadOnly = prefix.nonEmpty, expectedProps)
        val defaultValue = if (configName == SSL_KEYSTORE_TYPE_CONFIG) Some("JKS") else None
        verifySynonyms(configName, desc.synonyms, isSensitive, prefix, defaultValue)
      }
    }

    val adminClient = adminClients.head
    alterSslKeystoreUsingConfigCommand(sslProperties1, SecureExternal)

    val configDesc = TestUtils.tryUntilNoAssertionError() {
      val describeConfigsResult = describeConfig(adminClient)
      verifySslConfig("listener.name.external.", sslProperties1, describeConfigsResult)
      verifySslConfig("", invalidSslProperties, describeConfigsResult)
      describeConfigsResult
    }

    // Verify a few log configs with and without synonyms
    val expectedProps = new Properties
    expectedProps.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "1680000000")
    expectedProps.setProperty(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, "168")
    expectedProps.setProperty(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, "168")
    expectedProps.setProperty(CleanerConfig.LOG_CLEANER_THREADS_PROP, "1")
    val logRetentionMs = configEntry(configDesc, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG)
    verifyConfig(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, logRetentionMs,
      isSensitive = false, isReadOnly = false, expectedProps)
    val logRetentionHours = configEntry(configDesc, ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG)
    verifyConfig(ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, logRetentionHours,
      isSensitive = false, isReadOnly = true, expectedProps)
    val logRollHours = configEntry(configDesc, ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG)
    verifyConfig(ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, logRollHours,
      isSensitive = false, isReadOnly = true, expectedProps)
    val logCleanerThreads = configEntry(configDesc, CleanerConfig.LOG_CLEANER_THREADS_PROP)
    verifyConfig(CleanerConfig.LOG_CLEANER_THREADS_PROP, logCleanerThreads,
      isSensitive = false, isReadOnly = false, expectedProps)

    def synonymsList(configEntry: ConfigEntry): List[(String, ConfigSource)] =
      configEntry.synonyms.asScala.map(s => (s.name, s.source)).toList
    assertEquals(List((ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, ConfigSource.STATIC_BROKER_CONFIG),
      (ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.STATIC_BROKER_CONFIG),
      (ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.DEFAULT_CONFIG)),
      synonymsList(logRetentionMs))
    assertEquals(List((ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.STATIC_BROKER_CONFIG),
      (ServerLogConfigs.LOG_RETENTION_TIME_HOURS_CONFIG, ConfigSource.DEFAULT_CONFIG)),
      synonymsList(logRetentionHours))
    assertEquals(List((ServerLogConfigs.LOG_ROLL_TIME_HOURS_CONFIG, ConfigSource.DEFAULT_CONFIG)), synonymsList(logRollHours))
    assertEquals(List((CleanerConfig.LOG_CLEANER_THREADS_PROP, ConfigSource.DEFAULT_CONFIG)), synonymsList(logCleanerThreads))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testUpdatesUsingConfigProvider(quorum: String, groupProtocol: String): Unit = {
    val PollingIntervalVal = f"$${file:polling.interval:interval}"
    val PollingIntervalUpdateVal = f"$${file:polling.interval:updinterval}"
    val SslTruststoreTypeVal = f"$${file:ssl.truststore.type:storetype}"
    val SslKeystorePasswordVal = f"$${file:ssl.keystore.password:password}"

    val configPrefix = listenerPrefix(SecureExternal)
    val brokerConfigs = describeConfig(adminClients.head, servers).entries.asScala
    // the following are values before updated
    assertFalse(brokerConfigs.exists(_.name == TestMetricsReporter.PollingIntervalProp), "Initial value of polling interval")
    assertFalse(brokerConfigs.exists(_.name == configPrefix + SSL_TRUSTSTORE_TYPE_CONFIG), "Initial value of ssl truststore type")
    assertNull(brokerConfigs.find(_.name == configPrefix + SSL_KEYSTORE_PASSWORD_CONFIG).get.value, "Initial value of ssl keystore password")

    // setup ssl properties
    val secProps = securityProps(sslProperties1, KEYSTORE_PROPS, configPrefix)

    // configure config providers and properties need be updated
    val updatedProps = new Properties
    updatedProps.setProperty("config.providers", "file")
    updatedProps.setProperty("config.providers.file.class", "kafka.server.MockFileConfigProvider")
    updatedProps.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, classOf[TestMetricsReporter].getName)

    // 1. update Integer property using config provider
    updatedProps.put(TestMetricsReporter.PollingIntervalProp, PollingIntervalVal)

    // 2. update String property using config provider
    updatedProps.put(configPrefix + SSL_TRUSTSTORE_TYPE_CONFIG, SslTruststoreTypeVal)

    // merge two properties
    updatedProps ++= secProps

    // 3. update password property using config provider
    updatedProps.put(configPrefix + SSL_KEYSTORE_PASSWORD_CONFIG, SslKeystorePasswordVal)

    alterConfigsUsingConfigCommand(updatedProps)
    waitForConfig(TestMetricsReporter.PollingIntervalProp, "1000")
    waitForConfig(configPrefix + SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
    waitForConfig(configPrefix + SSL_KEYSTORE_PASSWORD_CONFIG, "ServerPassword")

    // wait for MetricsReporter
    val reporters = TestMetricsReporter.waitForReporters(servers.size)
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 1000)
      assertFalse(reporter.kafkaMetrics.isEmpty, "No metrics found")
    }

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testKeyStoreAlter(quorum: String, groupProtocol: String): Unit = {
    val topic2 = "testtopic2"
    TestUtils.createTopicWithAdmin(adminClients.head, topic2, servers, controllerServers, numPartitions, replicationFactor = numServers)

    // Start a producer and consumer that work with the current broker keystore.
    // This should continue working while changes are made
    val (producerThread, consumerThread) = startProduceConsume(retries = 0, groupProtocol)
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
    // Start the new consumer in a separate group than the continuous consumer started at the beginning of the test so
    // that it is not disrupted by rebalance.
    val consumer = ConsumerBuilder("group2", groupProtocol).trustStoreProps(sslProperties2).topic(topic2).build()
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should fail without update
    alterSslKeystore(sslProperties2, SecureInternal, expectFailure = true)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with compatible keystore should succeed
    val sslPropertiesCopy = sslProperties1.clone().asInstanceOf[Properties]
    val oldFile = new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
    val newFile = TestUtils.tempFile("keystore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    sslPropertiesCopy.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, newFile.getPath)
    alterSslKeystore(sslPropertiesCopy, SecureInternal)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify that keystores can be updated using same file name.
    val reusableProps = sslProperties2.clone().asInstanceOf[Properties]
    val reusableFile = TestUtils.tempFile("keystore", ".jks")
    reusableProps.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, reusableFile.getPath)
    Files.copy(new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath,
      reusableFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    alterSslKeystore(reusableProps, SecureExternal)
    val producer3 = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    verifyAuthenticationFailure(producer3)
    // Now alter using same file name. We can't check if the update has completed by comparing config on
    // the broker, so we wait for producer operation to succeed to verify that the update has been performed.
    Files.copy(new File(sslProperties2.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath,
      reusableFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    reusableFile.setLastModified(System.currentTimeMillis() + 1000)
    alterSslKeystore(reusableProps, SecureExternal)
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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testTrustStoreAlter(quorum: String, groupProtocol: String): Unit = {
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
      val consumer = ConsumerBuilder(group, groupProtocol)
        .listenerName(SecureInternal)
        .securityProtocol(SecurityProtocol.SSL)
        .keyStoreProps(keyStoreProps)
        .autoOffsetReset("latest")
        .build()
      verifyProduceConsume(producer, consumer, 10, topic)
    }

    val group_id = new AtomicInteger(1)
    def next_group_name(): String = s"alter-truststore-${group_id.getAndIncrement()}"

    // Produce/consume should work with old as well as new client keystore
    verifySslProduceConsume(sslProperties1, next_group_name())
    verifySslProduceConsume(sslProperties2, next_group_name())

    // Revert to old truststore with only one certificate and update. Clients should connect only with old keystore.
    val oldTruststoreProps = new Properties
    oldTruststoreProps ++= existingDynamicProps
    oldTruststoreProps ++= securityProps(sslProperties1, TRUSTSTORE_PROPS, prefix)
    reconfigureServers(oldTruststoreProps, perBrokerConfig = true,
      (s"$prefix$SSL_TRUSTSTORE_LOCATION_CONFIG", sslProperties1.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)))
    verifyAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties2).build())
    verifySslProduceConsume(sslProperties1, next_group_name())

    // Update same truststore file to contain both certificates without changing any configs.
    // Clients should connect successfully with either keystore after admin client AlterConfigsRequest completes.
    Files.copy(Paths.get(combinedStoreProps.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)),
      Paths.get(sslProperties1.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)),
      StandardCopyOption.REPLACE_EXISTING)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, oldTruststoreProps, perBrokerConfig = true).all.get()
    TestUtils.retry(30000) {
      try {
        verifySslProduceConsume(sslProperties1, next_group_name())
        verifySslProduceConsume(sslProperties2, next_group_name())
      } catch {
        case t: Throwable => throw new AssertionError(t)
      }
    }

    // Update internal keystore/truststore and validate new client connections from broker (e.g. controller).
    // Alter internal keystore from `sslProperties1` to `sslProperties2`, force disconnect of a controller connection
    // and verify that metadata is propagated for new topic.
    val props2 = securityProps(sslProperties2, KEYSTORE_PROPS, prefix)
    props2 ++= securityProps(combinedStoreProps, TRUSTSTORE_PROPS, prefix)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props2, perBrokerConfig = true).all.get(15, TimeUnit.SECONDS)
    verifySslProduceConsume(sslProperties2, next_group_name())
    props2 ++= securityProps(sslProperties2, TRUSTSTORE_PROPS, prefix)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props2, perBrokerConfig = true).all.get(15, TimeUnit.SECONDS)
    verifySslProduceConsume(sslProperties2, next_group_name())
    waitForAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties1))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testLogCleanerConfig(quorum: String, groupProtocol: String): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0, groupProtocol)

    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 1)

    val props = new Properties
    props.put(CleanerConfig.LOG_CLEANER_THREADS_PROP, "2")
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "20000000")
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, "0.8")
    props.put(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, "300000")
    props.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "40000")
    props.put(CleanerConfig.LOG_CLEANER_IO_MAX_BYTES_PER_SECOND_PROP, "50000000")
    props.put(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, "6000")

    // Verify cleaner config was updated. Wait for one of the configs to be updated and verify
    // that all other others were updated at the same time since they are reconfigured together
    var newCleanerConfig: CleanerConfig = null
    TestUtils.waitUntilTrue(() => {
      reconfigureServers(props, perBrokerConfig = false, (CleanerConfig.LOG_CLEANER_THREADS_PROP, "2"))
      newCleanerConfig = servers.head.logManager.cleaner.currentConfig
      newCleanerConfig.numThreads == 2
    }, "Log cleaner not reconfigured", 60000)
    assertEquals(20000000, newCleanerConfig.dedupeBufferSize)
    assertEquals(0.8, newCleanerConfig.dedupeBufferLoadFactor, 0.001)
    assertEquals(300000, newCleanerConfig.ioBufferSize)
    assertEquals(40000, newCleanerConfig.maxMessageSize)
    assertEquals(50000000, newCleanerConfig.maxIoBytesPerSecond, 50000000)
    assertEquals(6000, newCleanerConfig.backoffMs)

    // Verify thread count
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)

    // Stop a couple of threads and verify they are recreated if any config is updated
    def cleanerThreads = Thread.getAllStackTraces.keySet.asScala.filter(_.getName.startsWith("kafka-log-cleaner-thread-"))
    cleanerThreads.take(2).foreach(_.interrupt())
    TestUtils.waitUntilTrue(() => cleanerThreads.size == (2 * numServers) - 2, "Threads did not exit")
    props.put(CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, "8000")
    reconfigureServers(props, perBrokerConfig = false, (CleanerConfig.LOG_CLEANER_BACKOFF_MS_PROP, "8000"))
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)

    servers.foreach(_.shutdown())
    servers.foreach(_.awaitShutdown())
    servers.foreach(_.startup())

    // Verify dynamic config persists a server restart
    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 2)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testConsecutiveConfigChange(quorum: String, groupProtocol: String): Unit = {
    val topic2 = "testtopic2"
    val topicProps = new Properties
    topicProps.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    TestUtils.createTopicWithAdmin(adminClients.head, topic2, servers, controllerServers, numPartitions = 1, replicationFactor = numServers, topicConfig = topicProps)

    def getLogOrThrow(tp: TopicPartition): UnifiedLog = {
      var (logOpt, found) = TestUtils.computeUntilTrue {
        servers.head.logManager.getLog(tp)
      }(_.isDefined)
      assertTrue(found, "Log not found")
      logOpt.get
    }

    var log = getLogOrThrow(new TopicPartition(topic2, 0))
    assertTrue(log.config.overriddenConfigs.contains(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG))
    assertEquals("2", log.config.originals().get(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG).toString)

    val props = new Properties
    props.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "3")
    // Make a broker-default config
    reconfigureServers(props, perBrokerConfig = false, (ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "3"))
    // Verify that all broker defaults have been updated again
    servers.foreach { server =>
      props.forEach { (k, v) =>
        assertEquals(v, server.config.originals.get(k).toString, s"Not reconfigured $k")
      }
    }

    log = getLogOrThrow(new TopicPartition(topic2, 0))
    assertTrue(log.config.overriddenConfigs.contains(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG))
    assertEquals("2", log.config.originals().get(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG).toString) // Verify topic-level config survives

    // Make a second broker-default change
    props.clear()
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "604800000")
    reconfigureServers(props, perBrokerConfig = false, (ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "604800000"))
    log = getLogOrThrow(new TopicPartition(topic2, 0))
    assertTrue(log.config.overriddenConfigs.contains(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG))
    assertEquals("2", log.config.originals().get(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG).toString) // Verify topic-level config still survives
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testDefaultTopicConfig(quorum: String, groupProtocol: String): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0, groupProtocol)

    val props = new Properties
    val logIndexSizeMaxBytes = "100000"
    val logRetentionMs = TimeUnit.DAYS.toMillis(1)
    props.put(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, "4000")
    props.put(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, TimeUnit.HOURS.toMillis(2).toString)
    props.put(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, TimeUnit.HOURS.toMillis(1).toString)
    props.put(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, logIndexSizeMaxBytes)
    props.put(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, "60000")
    props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "10000000")
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, logRetentionMs.toString)
    props.put(ServerConfigs.MESSAGE_MAX_BYTES_CONFIG, "100000")
    props.put(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG, "10000")
    props.put(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, TimeUnit.DAYS.toMillis(1).toString)
    props.put(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP, "60000")
    props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "60000")
    props.put(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP, "0.3")
    props.put(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, "delete")
    props.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
    props.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    props.put(ServerConfigs.COMPRESSION_TYPE_CONFIG, "gzip")
    props.put(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG, true.toString)
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.toString)
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, "1000")
    reconfigureServers(props, perBrokerConfig = false, (ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, "4000"))

    // Verify that all broker defaults have been updated
    servers.foreach { server =>
      props.forEach { (k, v) =>
        assertEquals(server.config.originals.get(k).toString, v, s"Not reconfigured $k")
      }
    }

    // Verify that configs of existing logs have been updated
    val newLogConfig = new LogConfig(servers.head.config.extractLogConfigMap)
    TestUtils.waitUntilTrue(() => servers.head.logManager.currentDefaultConfig == newLogConfig,
      "Config not updated in LogManager")

    val log = servers.head.logManager.getLog(new TopicPartition(topic, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    TestUtils.waitUntilTrue(() => log.config.segmentSize == 4000, "Existing topic config using defaults not updated")
    val KafkaConfigToLogConfigName: Map[String, String] =
      ServerTopicConfigSynonyms.TOPIC_CONFIG_SYNONYMS.asScala.map { case (k, v) => (v, k) }
    props.asScala.foreach { case (k, v) =>
      val logConfigName = KafkaConfigToLogConfigName(k)
      val expectedValue = if (k == ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG) s"[$v]" else v
      assertEquals(expectedValue, log.config.originals.get(logConfigName).toString,
        s"Not reconfigured $logConfigName for existing log")
    }
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.LOG_APPEND_TIME)

    // Verify that the new config is actually used for new segments of existing logs
    TestUtils.waitUntilTrue(() => log.logSegments.asScala.exists(_.size > 3000), "Log segment size increase not applied")

    // Verify that overridden topic configs are not updated when broker default is updated
    val log2 = servers.head.logManager.getLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))
      .getOrElse(throw new IllegalStateException("Log not found"))
    assertFalse(log2.config.delete, "Overridden clean up policy should not be updated")
    assertEquals(BrokerCompressionType.PRODUCER, log2.config.compressionType)

    // Verify that we can alter subset of log configs
    props.clear()
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString)
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, "1000")
    reconfigureServers(props, perBrokerConfig = false, (ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString))
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.CREATE_TIME)
    // Verify that invalid configs are not applied
    val invalidProps = Map(
      ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG -> "abc", // Invalid type
      ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG -> "abc", // Invalid type
      ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG -> "invalid", // Invalid value
      ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG -> "0" // Fails KafkaConfig validation
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
    props.put(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, "500000")
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, TimeUnit.DAYS.toMillis(2).toString)
    alterConfigsOnServer(servers.head, props)
    assertEquals(500000, servers.head.config.values.get(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG))
    assertEquals(TimeUnit.DAYS.toMillis(2), servers.head.config.values.get(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))
    servers.tail.foreach { server =>
      assertEquals(logIndexSizeMaxBytes.toInt, server.config.values.get(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG))
      assertEquals(logRetentionMs, server.config.values.get(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))
    }

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)

    // Verify that configuration at both per-broker level and default cluster level could be deleted and
    // the default value should be restored
    props.clear()
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, "")
    props.put(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, "")
    TestUtils.incrementalAlterConfigs(servers.take(1), adminClients.head, props, perBrokerConfig = true, opType = OpType.DELETE).all.get
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props, perBrokerConfig = false, opType = OpType.DELETE).all.get
    servers.foreach { server =>
      waitForConfigOnServer(server, ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, 1680000000.toString)
    }
    servers.foreach { server =>
      val log = server.logManager.getLog(new TopicPartition(topic, 0)).getOrElse(throw new IllegalStateException("Log not found"))
      // Verify default values for these two configurations are restored on all brokers
      TestUtils.waitUntilTrue(() => log.config.maxIndexSize == ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT && log.config.retentionMs == 1680000000L,
        "Existing topic config using defaults not updated")
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testUncleanLeaderElectionEnable(quorum: String, groupProtocol: String): Unit = {
    // Create a topic with two replicas on brokers other than the controller
    val topic = "testtopic2"
    TestUtils.createTopicWithAdmin(adminClients.head, topic, servers, controllerServers, replicaAssignment = Map(0 -> Seq(0, 1)))

    val producer = ProducerBuilder().acks(1).build()
    val consumer = ConsumerBuilder("unclean-leader-test", groupProtocol).enableAutoCommit(false).topic(topic).build()
    verifyProduceConsume(producer, consumer, numRecords = 10, topic)
    consumer.commitSync()

    def partitionInfo: TopicPartitionInfo =
      adminClients.head.describeTopics(Collections.singleton(topic)).topicNameValues().get(topic).get().partitions().get(0)

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
    newProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, newProps, perBrokerConfig = false).all.get
    TestUtils.ensureConsistentKRaftMetadata(servers, controllerServer)

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

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testThreadPoolResize(quorum: String, groupProtocol: String): Unit = {

    // In kraft mode, the StripedReplicaPlacer#initialize includes some randomization,
    // so the replica assignment is not deterministic.
    // If a fetcher thread is not assigned any topic partition, it will not be created.
    // Change the replica assignment to ensure that all fetcher threads are created.
    TestUtils.deleteTopicWithAdmin(adminClients.head, topic, servers, controllerServers)
    val replicaAssignment = Map(
      0 -> Seq(0, 1, 2), 1 -> Seq(1, 2, 0), 2 -> Seq(2, 1, 0), 3 -> Seq(0, 1, 2), 4 -> Seq(1, 2, 0),
      5 -> Seq(2, 1, 0), 6 -> Seq(0, 1, 2), 7 -> Seq(1, 2, 0), 8 -> Seq(2, 1, 0), 9 -> Seq(0, 1, 2))
    TestUtils.createTopicWithAdmin(adminClients.head, topic, servers, controllerServers, replicaAssignment = replicaAssignment)

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

    def maybeVerifyThreadPoolSize(size: Int, threadPrefix: String): Unit = {
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
      maybeVerifyThreadPoolSize(newSize, threadPrefix)
    }

    def verifyThreadPoolResize(propName: String, currentSize: => Int, threadPrefix: String, mayReceiveDuplicates: Boolean): Unit = {
      maybeVerifyThreadPoolSize(currentSize, threadPrefix)
      val numRetries = if (mayReceiveDuplicates) 100 else 0
      val (producerThread, consumerThread) = startProduceConsume(retries = numRetries, groupProtocol)
      var threadPoolSize = currentSize
      (1 to 2).foreach { _ =>
        threadPoolSize = reducePoolSize(propName, threadPoolSize, threadPrefix)
        Thread.sleep(100)
        threadPoolSize = increasePoolSize(propName, threadPoolSize, threadPrefix)
        Thread.sleep(100)
      }
      stopAndVerifyProduceConsume(producerThread, consumerThread, mayReceiveDuplicates)
      // Verify that all threads are alive
      maybeVerifyThreadPoolSize(threadPoolSize, threadPrefix)
    }

    val config = servers.head.config
    verifyThreadPoolResize(ServerConfigs.NUM_IO_THREADS_CONFIG, config.numIoThreads,
      requestHandlerPrefix, mayReceiveDuplicates = false)
    verifyThreadPoolResize(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, config.numReplicaFetchers,
      fetcherThreadPrefix, mayReceiveDuplicates = false)
    verifyThreadPoolResize(ServerConfigs.BACKGROUND_THREADS_CONFIG, config.backgroundThreads,
      "kafka-scheduler-", mayReceiveDuplicates = false)
    verifyThreadPoolResize(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG, config.numRecoveryThreadsPerDataDir,
      "", mayReceiveDuplicates = false)
    verifyThreadPoolResize(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, config.numNetworkThreads,
      networkThreadPrefix, mayReceiveDuplicates = true)
    verifyThreads("data-plane-kafka-socket-acceptor-", config.listeners.size, 1)

    verifyProcessorMetrics()
    verifyMarkPartitionsForTruncation()
  }

  private def isProcessorMetric(metricName: MetricName): Boolean = {
    val mbeanName = metricName.getMBeanName
    mbeanName.contains(s"${Processor.NetworkProcessorMetricTag}=") || mbeanName.contains(s"${RequestChannel.ProcessorMetricTag}=")
  }

  private def clearLeftOverProcessorMetrics(): Unit = {
    val metricsFromOldTests = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.filter(isProcessorMetric)
    metricsFromOldTests.foreach(KafkaYammerMetrics.defaultRegistry.removeMetric)
  }

  // Verify that metrics from processors that were removed have been deleted.
  // Since processor ids are not reused, it is sufficient to check metrics count
  // based on the current number of processors
  private def verifyProcessorMetrics(): Unit = {
    val numProcessors = servers.head.config.numNetworkThreads * 2 // 2 listeners

    val kafkaMetrics = servers.head.metrics.metrics().keySet.asScala
      .filter(_.tags.containsKey(Processor.NetworkProcessorMetricTag))
      .groupBy(_.tags.get(Processor.ListenerMetricTag))

    assertEquals(2, kafkaMetrics.size) // 2 listeners
    // 2 threads per listener
    assertEquals(2, kafkaMetrics("INTERNAL").groupBy(_.tags().get(Processor.NetworkProcessorMetricTag)).size)
    assertEquals(2, kafkaMetrics("EXTERNAL").groupBy(_.tags().get(Processor.NetworkProcessorMetricTag)).size)

    KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala
      .filter(isProcessorMetric)
      .groupBy(_.getName)
      .foreach { case (name, set) => assertEquals(numProcessors, set.size, s"Metrics not deleted $name") }
  }

  // Verify that replicaFetcherManager.markPartitionsForTruncation uses the current fetcher thread size
  // to obtain partition assignment
  private def verifyMarkPartitionsForTruncation(): Unit = {
    val leaderId = 0
    val topicDescription = adminClients.head.
      describeTopics(java.util.Arrays.asList(topic)).
      allTopicNames().
      get(3, TimeUnit.MINUTES).get(topic)
    val partitions = topicDescription.partitions().asScala.
      filter(p => p.leader().id() == leaderId).
      map(p => new TopicPartition(topic, p.partition()))
    assertTrue(partitions.nonEmpty, s"Partitions not found with leader $leaderId")
    partitions.foreach { tp =>
      (1 to 2).foreach { i =>
        val replicaFetcherManager = servers(i).replicaManager.replicaFetcherManager
        val truncationOffset = tp.partition
        replicaFetcherManager.markPartitionsForTruncation(leaderId, tp, truncationOffset)
        val fetcherThreads = replicaFetcherManager.fetcherThreadMap.filter(_._2.fetchState(tp).isDefined)
        assertEquals(1, fetcherThreads.size)
        assertEquals(replicaFetcherManager.getFetcherId(tp), fetcherThreads.head._1.fetcherId)
        val thread = fetcherThreads.head._2
        assertEquals(Some(truncationOffset), thread.fetchState(tp).map(_.fetchOffset))
        assertEquals(Some(Truncating), thread.fetchState(tp).map(_.state))
      }
    }
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testMetricsReporterUpdate(quorum: String, groupProtocol: String): Unit = {
    // Add a new metrics reporter
    val newProps = new Properties
    newProps.put(TestMetricsReporter.PollingIntervalProp, "100")
    configureMetricsReporters(Seq(classOf[JmxReporter], classOf[TestMetricsReporter]), newProps)

    val reporters = TestMetricsReporter.waitForReporters(servers.size + controllerServers.size)
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 100)
      assertFalse(reporter.kafkaMetrics.isEmpty, "No metrics found")
      TestUtils.retry(30_000) {
        reporter.verifyMetricValue("request-total", "socket-server-metrics")
      }
    }
    assertEquals(Set(controllerServer.config.nodeId) ++ servers.map(_.config.brokerId),
      TestMetricsReporter.configuredBrokers.toSet)

    // non-default value to trigger a new metric
    val clientId = "test-client-1"
    servers.foreach { server =>
      server.quotaManagers.produce.updateQuota(None, Some(clientId), Some(clientId),
        Some(Quota.upperBound(10000000)))
    }
    val (producerThread, consumerThread) = startProduceConsume(retries = 0, groupProtocol, clientId)
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
    val newReporters = TestMetricsReporter.waitForReporters(servers.size + controllerServers.size)
    newReporters.foreach(_.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 2000))

    // Verify that validation failure of metrics reporter fails reconfiguration and leaves config unchanged
    newProps.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, "unknownMetricsReporter")
    reconfigureServers(newProps, perBrokerConfig = false, (TestMetricsReporter.PollingIntervalProp, "2000"), expectFailure = true)
    servers.foreach { server =>
      assertEquals(classOf[TestMetricsReporter].getName, server.config.originals.get(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG))
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
    newProps.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, classOf[TestMetricsReporter].getName)
    newProps.put(TestMetricsReporter.PollingIntervalProp, "4000")
    alterConfigsOnServer(servers.head, newProps)
    TestUtils.waitUntilTrue(() => !TestMetricsReporter.testReporters.isEmpty, "Metrics reporter not created")
    val perBrokerReporter = TestMetricsReporter.waitForReporters(1).head
    perBrokerReporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 4000)

    // update TestMetricsReporter.PollingIntervalProp to 3000
    newProps.put(TestMetricsReporter.PollingIntervalProp, "3000")
    alterConfigsOnServer(servers.head, newProps)
    perBrokerReporter.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 3000)

    servers.tail.foreach { server => assertEquals("", server.config.originals.get(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG)) }

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testReconfigureRemovedListener(quorum: String, groupProtocol: String): Unit = {
    val client = adminClients.head
    val broker = servers.head
    assertEquals(2, broker.config.dynamicConfig.reconfigurables.asScala.count(r => r.isInstanceOf[DataPlaneAcceptor]))
    val broker0Resource = new ConfigResource(ConfigResource.Type.BROKER, broker.config.brokerId.toString)

    def acceptors: Seq[DataPlaneAcceptor] = broker.config.dynamicConfig.reconfigurables.asScala.filter(_.isInstanceOf[DataPlaneAcceptor])
      .map(_.asInstanceOf[DataPlaneAcceptor]).toSeq

    // add new PLAINTEXT listener
    client.incrementalAlterConfigs(Map(broker0Resource ->
      Seq(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.LISTENERS_CONFIG,
        s"PLAINTEXT://localhost:0, $SecureInternal://localhost:0, $SecureExternal://localhost:0"), AlterConfigOp.OpType.SET)
      ).asJavaCollection).asJava).all().get()

    TestUtils.waitUntilTrue(() => acceptors.size == 3, s"failed to add new DataPlaneAcceptor")

    // remove PLAINTEXT listener
    client.incrementalAlterConfigs(Map(broker0Resource ->
      Seq(new AlterConfigOp(new ConfigEntry(SocketServerConfigs.LISTENERS_CONFIG,
        s"$SecureInternal://localhost:0, $SecureExternal://localhost:0"), AlterConfigOp.OpType.SET)
      ).asJavaCollection).asJava).all().get()

    TestUtils.waitUntilTrue(() => acceptors.size == 2,
      s"failed to remove DataPlaneAcceptor. current: ${acceptors.map(_.endPoint.toString).mkString(",")}")
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testTransactionVerificationEnable(quorum: String, groupProtocol: String): Unit = {
    def verifyConfiguration(enabled: Boolean): Unit = {
      servers.foreach { server =>
        TestUtils.waitUntilTrue(() => server.logManager.producerStateManagerConfig.transactionVerificationEnabled == enabled, "Configuration was not updated.")
      }
      verifyThreads("AddPartitionsToTxnSenderThread-", 1)
    }
    // Verification enabled by default
    verifyConfiguration(true)

    // Dynamically turn verification off.
    val configPrefix = listenerPrefix(SecureExternal)
    val updatedProps = securityProps(sslProperties1, KEYSTORE_PROPS, configPrefix)
    updatedProps.put(TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "false")
    alterConfigsUsingConfigCommand(updatedProps)
    verifyConfiguration(false)

    // Ensure it remains off after shutdown.
    val shutdownServer = servers.head
    shutdownServer.shutdown()
    shutdownServer.awaitShutdown()
    shutdownServer.startup()
    verifyConfiguration(false)

    // Turn verification back on.
    updatedProps.put(TransactionLogConfig.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "true")
    alterConfigsUsingConfigCommand(updatedProps)
    verifyConfiguration(true)
  }

  private def awaitInitialPositions(consumer: Consumer[_, _]): Unit = {
    TestUtils.pollUntilTrue(consumer, () => !consumer.assignment.isEmpty, "Timed out while waiting for assignment")
    consumer.assignment.forEach(tp => consumer.position(tp))
  }

  private def clientProps(securityProtocol: SecurityProtocol, saslMechanism: Option[String] = None): Properties = {
    val props = new Properties
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS")
    if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL)
      props ++= kafkaClientSaslProperties(saslMechanism.getOrElse(kafkaClientSaslMechanism), dynamicJaasConfig = true)
    props ++= sslProperties1
    securityProps(props, props.keySet)
  }

  private def createAdminClient(securityProtocol: SecurityProtocol, listenerName: String): Admin = {
    val config = clientProps(securityProtocol)
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(listenerName))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = Admin.create(config)
    adminClients += adminClient
    adminClient
  }

  private def verifyProduceConsume(producer: KafkaProducer[String, String],
                                   consumer: Consumer[String, String],
                                   numRecords: Int,
                                   topic: String): Unit = {
    val producerRecords = (1 to numRecords).map(i => new ProducerRecord(topic, s"key$i", s"value$i"))
    producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))
    TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords)
  }

  private def verifyAuthenticationFailure(producer: KafkaProducer[_, _]): Unit = {
    assertThrows(classOf[AuthenticationException], () => producer.partitionsFor(topic))
  }

  private def waitForAuthenticationFailure(producerBuilder: ProducerBuilder): Unit = {
    TestUtils.waitUntilTrue(() => {
      try {
        verifyAuthenticationFailure(producerBuilder.build())
        true
      } catch {
        case _: Error => false
      }
    }, "Did not fail authentication with invalid config")
  }

  private def describeConfig(adminClient: Admin, servers: Seq[KafkaBroker] = this.servers): Config = {
    val configResources = servers.map { server =>
      new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
    }
    val describeOptions = new DescribeConfigsOptions().includeSynonyms(true)
    val describeResult = adminClient.describeConfigs(configResources.asJava, describeOptions).all.get
    assertEquals(servers.size, describeResult.values.size)
    val configDescription = describeResult.values.iterator.next
    assertFalse(configDescription.entries.isEmpty, "Configs are empty")
    configDescription
  }

  private def securityProps(srcProps: Properties, propNames: util.Set[_], listenerPrefix: String = ""): Properties = {
    val resultProps = new Properties
    propNames.asScala.filter(srcProps.containsKey).foreach { propName =>
      resultProps.setProperty(s"$listenerPrefix$propName", configValueAsString(srcProps.get(propName)))
    }
    resultProps
  }

  // Creates a new truststore with certificates from the provided stores and returns the properties of the new store
  private def mergeTrustStores(trustStore1Props: Properties, trustStore2Props: Properties): Properties = {

    def load(props: Properties): KeyStore = {
      val ks = KeyStore.getInstance("JKS")
      val password = props.get(SSL_TRUSTSTORE_PASSWORD_CONFIG).asInstanceOf[Password].value
      val in = Files.newInputStream(Paths.get(props.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)))
      try {
        ks.load(in, password.toCharArray)
        ks
      } finally {
        in.close()
      }
    }
    val cert1 = load(trustStore1Props).getCertificate("kafka")
    val cert2 = load(trustStore2Props).getCertificate("kafka")
    val certs = Map("kafka1" -> cert1, "kafka2" -> cert2)

    val combinedStorePath = TestUtils.tempFile("truststore", ".jks").getAbsolutePath
    val password = trustStore1Props.get(SSL_TRUSTSTORE_PASSWORD_CONFIG).asInstanceOf[Password]
    TestSslUtils.createTrustStore(combinedStorePath, password, certs.asJava)
    val newStoreProps = new Properties
    newStoreProps.put(SSL_TRUSTSTORE_LOCATION_CONFIG, combinedStorePath)
    newStoreProps.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, password)
    newStoreProps.put(SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
    newStoreProps
  }

  private def alterSslKeystore(props: Properties, listener: String, expectFailure: Boolean  = false): Unit = {
    val configPrefix = listenerPrefix(listener)
    val newProps = securityProps(props, KEYSTORE_PROPS, configPrefix)
    reconfigureServers(newProps, perBrokerConfig = true,
      (s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)), expectFailure)
  }

  private def alterSslKeystoreUsingConfigCommand(props: Properties, listener: String): Unit = {
    val configPrefix = listenerPrefix(listener)
    val newProps = securityProps(props, KEYSTORE_PROPS, configPrefix)
    alterConfigsUsingConfigCommand(newProps)
    waitForConfig(s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
  }

  private def alterConfigsOnServer(server: KafkaBroker, props: Properties): Unit = {
val configEntries = props.asScala.map { case (k, v) => new AlterConfigOp(new ConfigEntry(k, v), OpType.SET) }.toList.asJava
    val alterConfigs = new java.util.HashMap[ConfigResource, java.util.Collection[AlterConfigOp]]()
    alterConfigs.put(new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString), configEntries)
    adminClients.head.incrementalAlterConfigs(alterConfigs)
    props.asScala.foreach { case (k, v) => waitForConfigOnServer(server, k, v) }
  }

  private def alterConfigs(servers: Seq[KafkaBroker], adminClient: Admin, props: Properties,
                   perBrokerConfig: Boolean): AlterConfigsResult = {
    val configEntries = props.asScala.map { case (k, v) => new AlterConfigOp(new ConfigEntry(k, v), OpType.SET) }.toList.asJava
    val configs = if (perBrokerConfig) {
      val alterConfigs = new java.util.HashMap[ConfigResource, java.util.Collection[AlterConfigOp]]()
      servers.foreach(server => alterConfigs.put(new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString), configEntries))
      alterConfigs
    } else {
      val alterConfigs = new java.util.HashMap[ConfigResource, java.util.Collection[AlterConfigOp]]()
      alterConfigs.put(new ConfigResource(ConfigResource.Type.BROKER, ""), configEntries)
      alterConfigs
    }
    adminClient.incrementalAlterConfigs(configs)
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String), expectFailure: Boolean = false): Unit = {
    val alterResult = alterConfigs(servers, adminClients.head, newProps, perBrokerConfig)
    if (expectFailure) {
      val oldProps = servers.head.config.values.asScala.filter { case (k, _) => newProps.containsKey(k) }
      val brokerResources = if (perBrokerConfig)
        servers.map(server => new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString))
      else {
        Seq(new ConfigResource(ConfigResource.Type.BROKER, ""))
      }
      brokerResources.foreach { brokerResource =>
        val exception = assertThrows(classOf[ExecutionException], () => alterResult.values.get(brokerResource).get)
        assertEquals(classOf[InvalidRequestException], exception.getCause.getClass)
      }
      servers.foreach { server =>
        assertEquals(oldProps, server.config.values.asScala.filter { case (k, _) => newProps.containsKey(k) })
      }
    } else {
      alterResult.all.get
      waitForConfig(aPropToVerify._1, aPropToVerify._2)
    }
  }

  private def configEntry(configDesc: Config, configName: String): ConfigEntry = {
    configDesc.entries.asScala.find(cfg => cfg.name == configName)
      .getOrElse(throw new IllegalStateException(s"Config not found $configName"))
  }

  private def listenerPrefix(name: String): String = new ListenerName(name).configPrefix

  private def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server => waitForConfigOnServer(server, propName, propValue, maxWaitMs) }
  }

  private def waitForConfigOnServer(server: KafkaBroker, propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
    }
  }

  private def configureMetricsReporters(reporters: Seq[Class[_]], props: Properties,
                                        perBrokerConfig: Boolean = false): Unit = {
    val reporterStr = reporters.map(_.getName).mkString(",")
    props.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, reporterStr)
    reconfigureServers(props, perBrokerConfig, (MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, reporterStr))
    TestUtils.ensureConsistentKRaftMetadata(servers, controllerServer)
  }

  private def invalidSslConfigs: Properties = {
    val props = new Properties
    props.put(SSL_KEYSTORE_LOCATION_CONFIG, "invalid/file/path")
    props.put(SSL_KEYSTORE_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEY_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
    props
  }

  private def currentThreads: List[String] = {
    Thread.getAllStackTraces.keySet.asScala.toList.map(_.getName)
  }

  private def matchingThreads(threadPrefix: String): List[String] = {
    currentThreads.filter(_.startsWith(threadPrefix))
  }

  private def verifyThreads(threadPrefix: String, countPerBroker: Int, leftOverThreads: Int = 0): Unit = {
    val expectedCount = countPerBroker * servers.size
    val (threads, resized) = TestUtils.computeUntilTrue(matchingThreads(threadPrefix)) { matching =>
      matching.size >= expectedCount &&  matching.size <= expectedCount + leftOverThreads
    }
    assertTrue(resized, s"Invalid threads: expected $expectedCount, got ${threads.size}: $threads")
  }

  private def startProduceConsume(retries: Int, groupProtocol: String, producerClientId: String = "test-producer"): (ProducerThread, ConsumerThread) = {
    val producerThread = new ProducerThread(producerClientId, retries)
    clientThreads += producerThread
    val consumerThread = new ConsumerThread(producerThread, groupProtocol)
    clientThreads += consumerThread
    consumerThread.start()
    producerThread.start()
    TestUtils.waitUntilTrue(() => producerThread.sent >= 10, "Messages not sent")
    (producerThread, consumerThread)
  }

  private def stopAndVerifyProduceConsume(producerThread: ProducerThread, consumerThread: ConsumerThread,
                                          mayReceiveDuplicates: Boolean = false): Unit = {
    TestUtils.waitUntilTrue(() => producerThread.sent >= 10, "Messages not sent")
    producerThread.shutdown()
    consumerThread.initiateShutdown()
    consumerThread.awaitShutdown()
    assertEquals(producerThread.lastSent, consumerThread.lastReceived)
    assertEquals(0, consumerThread.missingRecords.size)
    if (!mayReceiveDuplicates)
      assertFalse(consumerThread.duplicates, "Duplicates not expected")
    assertFalse(consumerThread.outOfOrder, "Some messages received out of order")
  }

  private def configValueAsString(value: Any): String = {
    value match {
      case password: Password => password.value
      case list: util.List[_] => list.asScala.map(_.toString).mkString(",")
      case _ => value.toString
    }
  }

  private def alterConfigsUsingConfigCommand(props: Properties): Unit = {
    val propsFile = tempPropertiesFile(clientProps(SecurityProtocol.SSL))

    servers.foreach { server =>
      val args = Array("--bootstrap-server", TestUtils.bootstrapServers(servers, new ListenerName(SecureInternal)),
        "--command-config", propsFile.getAbsolutePath,
        "--alter", "--add-config", props.asScala.map { case (k, v) => s"$k=$v" }.mkString(","),
        "--entity-type", "brokers",
        "--entity-name", server.config.brokerId.toString)
      ConfigCommand.main(args)
    }
  }

  private def tempPropertiesFile(properties: Properties): File = TestUtils.tempPropertiesFile(properties.asScala)

  private abstract class ClientBuilder[T]() {
    protected var _bootstrapServers: Option[String] = None
    protected var _listenerName: String = SecureExternal
    protected var _securityProtocol = SecurityProtocol.SASL_SSL
    protected var _saslMechanism: String = kafkaClientSaslMechanism
    protected var _clientId = "test-client"
    protected val _propsOverride: Properties = new Properties

    def bootstrapServers(bootstrap: String): this.type = { _bootstrapServers = Some(bootstrap); this }
    def listenerName(listener: String): this.type = { _listenerName = listener; this }
    def securityProtocol(protocol: SecurityProtocol): this.type = { _securityProtocol = protocol; this }
    def saslMechanism(mechanism: String): this.type = { _saslMechanism = mechanism; this }
    def clientId(id: String): this.type = { _clientId = id; this }
    def keyStoreProps(props: Properties): this.type = { _propsOverride ++= securityProps(props, KEYSTORE_PROPS); this }
    def trustStoreProps(props: Properties): this.type = { _propsOverride ++= securityProps(props, TRUSTSTORE_PROPS); this }

    def bootstrapServers: String =
      _bootstrapServers.getOrElse(TestUtils.bootstrapServers(servers, new ListenerName(_listenerName)))

    def propsOverride: Properties = {
      val props = clientProps(_securityProtocol, Some(_saslMechanism))
      props.put(CommonClientConfigs.CLIENT_ID_CONFIG, _clientId)
      props ++= _propsOverride
      props
    }

    def build(): T
  }

  private case class ProducerBuilder() extends ClientBuilder[KafkaProducer[String, String]] {
    private var _retries = Int.MaxValue
    private var _acks = -1
    private var _requestTimeoutMs = 30000
    private var _deliveryTimeoutMs = 30000

    def maxRetries(retries: Int): ProducerBuilder = { _retries = retries; this }
    def acks(acks: Int): ProducerBuilder = { _acks = acks; this }
    def requestTimeoutMs(timeoutMs: Int): ProducerBuilder = { _requestTimeoutMs = timeoutMs; this }
    def deliveryTimeoutMs(timeoutMs: Int): ProducerBuilder = { _deliveryTimeoutMs= timeoutMs; this }

    override def build(): KafkaProducer[String, String] = {
      val producerProps = propsOverride
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      producerProps.put(ProducerConfig.ACKS_CONFIG, _acks.toString)
      producerProps.put(ProducerConfig.RETRIES_CONFIG, _retries.toString)
      producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, _deliveryTimeoutMs.toString)
      producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, _requestTimeoutMs.toString)
      // disable the idempotence since some tests want to test the cases when retries=0, and these tests are not testing producers
      producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")

      val producer = new KafkaProducer[String, String](producerProps, new StringSerializer, new StringSerializer)
      producers += producer
      producer
    }
  }

  private case class ConsumerBuilder(group: String, groupProtocol: String) extends ClientBuilder[Consumer[String, String]] {
    private var _autoOffsetReset = "earliest"
    private var _enableAutoCommit = false
    private var _topic = DynamicBrokerReconfigurationTest.this.topic

    def autoOffsetReset(reset: String): ConsumerBuilder = { _autoOffsetReset = reset; this }
    def enableAutoCommit(enable: Boolean): ConsumerBuilder = { _enableAutoCommit = enable; this }
    def topic(topic: String): ConsumerBuilder = { _topic = topic; this }

    override def build(): Consumer[String, String] = {
      val consumerProps = propsOverride
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, _autoOffsetReset)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group)
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, _enableAutoCommit.toString)
      consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol)

      val consumer = new KafkaConsumer[String, String](consumerProps, new StringDeserializer, new StringDeserializer)
      consumers += consumer

      consumer.subscribe(Collections.singleton(_topic))
      if (_autoOffsetReset == "latest")
        awaitInitialPositions(consumer)
      consumer
    }
  }

  private class ProducerThread(clientId: String, retries: Int)
    extends ShutdownableThread(clientId, false) {

    private val producer = ProducerBuilder().maxRetries(retries).clientId(clientId).build()
    val lastSent = new ConcurrentHashMap[Int, Int]()
    @volatile var sent = 0
    override def doWork(): Unit = {
      try {
        while (isRunning) {
          val key = sent.toString
          val partition = sent % numPartitions
          val record = new ProducerRecord(topic, partition, key, s"value$sent")
          producer.send(record).get(10, TimeUnit.SECONDS)
          lastSent.put(partition, sent)
          sent += 1
        }
      } finally {
        producer.close()
      }
    }
  }

  private class ConsumerThread(producerThread: ProducerThread, groupProtocol: String) extends ShutdownableThread("test-consumer", false) {
    private val consumer = ConsumerBuilder("group1", groupProtocol).enableAutoCommit(true).build()
    val lastReceived = new ConcurrentHashMap[Int, Int]()
    val missingRecords = new ConcurrentLinkedQueue[Int]()
    @volatile var outOfOrder = false
    @volatile var duplicates = false
    @volatile var lastBatch: ConsumerRecords[String, String] = _
    @volatile private var endTimeMs = Long.MaxValue
    @volatile var received = 0
    override def doWork(): Unit = {
      try {
        while (isRunning || (lastReceived != producerThread.lastSent && System.currentTimeMillis < endTimeMs)) {
          val records = consumer.poll(Duration.ofMillis(50L))
          received += records.count
          if (!records.isEmpty) {
            lastBatch = records
            records.partitions.forEach { tp =>
              val partition = tp.partition
              records.records(tp).asScala.map(_.key.toInt).foreach { key =>
                val prevKey = lastReceived.asScala.getOrElse(partition, partition - numPartitions)
                val expectedKey = prevKey + numPartitions
                if (key < prevKey)
                  outOfOrder = true
                else if (key == prevKey)
                  duplicates = true
                else {
                  for (i <- expectedKey until key by numPartitions)
                    missingRecords.add(i)
                }
                lastReceived.put(partition, key)
                missingRecords.remove(key)
              }
            }
          }
        }
      } finally {
        consumer.close()
      }
    }

    override def initiateShutdown(): Boolean = {
      endTimeMs = System.currentTimeMillis + 10 * 1000
      super.initiateShutdown()
    }

    def waitForMatchingRecords(predicate: ConsumerRecord[String, String] => Boolean): Unit = {
      TestUtils.waitUntilTrue(() => {
        val records = lastBatch
        if (records == null || records.isEmpty)
          false
        else
          records.asScala.toList.exists(predicate)
      }, "Received records did not match")
    }
  }
}

object TestMetricsReporter {
  val PollingIntervalProp = "polling.interval"
  val testReporters = new ConcurrentLinkedQueue[TestMetricsReporter]()
  val configuredBrokers = mutable.Set[Int]()

  def waitForReporters(count: Int): List[TestMetricsReporter] = {
    TestUtils.waitUntilTrue(() => testReporters.size == count, msg = "Metrics reporters not created")

    val reporters = testReporters.asScala.toList
    TestUtils.waitUntilTrue(() => reporters.forall(_.configureCount == 1), msg = "Metrics reporters not configured")
    reporters
  }
}

class TestMetricsReporter extends MetricsReporter with Reconfigurable with Closeable with ClusterResourceListener {
  import TestMetricsReporter._
  val kafkaMetrics = ArrayBuffer[KafkaMetric]()
  @volatile var initializeCount = 0
  @volatile var contextChangeCount = 0
  @volatile var configureCount = 0
  @volatile var reconfigureCount = 0
  @volatile var closeCount = 0
  @volatile var clusterUpdateCount = 0
  @volatile var pollingInterval: Int = -1
  testReporters.add(this)

  override def contextChange(metricsContext: MetricsContext): Unit = {
    contextChangeCount += 1
  }

  override def init(metrics: util.List[KafkaMetric]): Unit = {
    assertTrue(contextChangeCount > 0, "contextChange must be called before init")
    kafkaMetrics ++= metrics.asScala
    initializeCount += 1
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    configuredBrokers += configs.get(ServerConfigs.BROKER_ID_CONFIG).toString.toInt
    configureCount += 1
    pollingInterval = configs.get(PollingIntervalProp).toString.toInt
  }

  override def metricChange(metric: KafkaMetric): Unit = {
  }

  override def metricRemoval(metric: KafkaMetric): Unit = {
    kafkaMetrics -= metric
  }

  override def onUpdate(clusterResource: ClusterResource): Unit = {
    assertNotNull(clusterResource.clusterId, "Cluster id not set")
    clusterUpdateCount += 1
  }

  override def reconfigurableConfigs(): util.Set[String] = {
    Set(PollingIntervalProp).asJava
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    val pollingInterval = configs.get(PollingIntervalProp).toString.toInt
    if (pollingInterval <= 0)
      throw new ConfigException(s"Invalid polling interval $pollingInterval")
  }

  override def reconfigure(configs: util.Map[String, _]): Unit = {
    reconfigureCount += 1
    pollingInterval = configs.get(PollingIntervalProp).toString.toInt
  }

  override def close(): Unit = {
    closeCount += 1
  }

  def verifyState(reconfigureCount: Int, deleteCount: Int, pollingInterval: Int): Unit = {
    assertEquals(1, initializeCount)
    assertEquals(1, configureCount)
    assertEquals(reconfigureCount, this.reconfigureCount)
    assertEquals(deleteCount, closeCount)
    assertEquals(1, clusterUpdateCount)
    assertEquals(pollingInterval, this.pollingInterval)
  }

  def verifyMetricValue(name: String, group: String): Unit = {
    val matchingMetrics = kafkaMetrics.filter(metric => metric.metricName.name == name && metric.metricName.group == group)
    assertTrue(matchingMetrics.nonEmpty, "Metric not found")
    val total = matchingMetrics.foldLeft(0.0)((total, metric) => total + metric.metricValue.asInstanceOf[Double])
    assertTrue(total > 0.0, "Invalid metric value " + total + " for name " + name + " , group " + group)
  }
}


class MockFileConfigProvider extends FileConfigProvider {
  @throws(classOf[IOException])
  override def reader(path: Path): Reader = {
    new StringReader("key=testKey\npassword=ServerPassword\ninterval=1000\nupdinterval=2000\nstoretype=JKS")
  }
}
