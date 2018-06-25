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

import java.io.{Closeable, File, FileInputStream, FileWriter}
import java.nio.file.{Files, StandardCopyOption}
import java.lang.management.ManagementFactory
import java.security.KeyStore
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent._
import javax.management.ObjectName

import com.yammer.metrics.Metrics
import com.yammer.metrics.core.MetricName
import kafka.admin.ConfigCommand
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.coordinator.group.OffsetConfig
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.network.{Processor, RequestChannel}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.{ConfigEntityChangeNotificationZNode, ZooKeeperTestHarness}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, Reconfigurable, TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.{AuthenticationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsReporter}
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.test.TestSslUtils
import org.junit.Assert._
import org.junit.{After, Before, Test, Ignore}

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.collection.Seq

object DynamicBrokerReconfigurationTest {
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"
}

class DynamicBrokerReconfigurationTest extends ZooKeeperTestHarness with SaslSetup {

  import DynamicBrokerReconfigurationTest._

  private val servers = new ArrayBuffer[KafkaServer]
  private val numServers = 3
  private val numPartitions = 10
  private val producers = new ArrayBuffer[KafkaProducer[String, String]]
  private val consumers = new ArrayBuffer[KafkaConsumer[String, String]]
  private val adminClients = new ArrayBuffer[AdminClient]()
  private val clientThreads = new ArrayBuffer[ShutdownableThread]()
  private val executors = new ArrayBuffer[ExecutorService]
  private val topic = "testtopic"

  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")

  private val trustStoreFile1 = File.createTempFile("truststore", ".jks")
  private val trustStoreFile2 = File.createTempFile("truststore", ".jks")
  private val sslProperties1 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile1), "kafka")
  private val sslProperties2 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile2), "kafka")
  private val invalidSslProperties = invalidSslConfigs

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism)))
    super.setUp()

    clearLeftOverProcessorMetrics() // clear metrics left over from other tests so that new ones can be tested

    (0 until numServers).foreach { brokerId =>

      val props = TestUtils.createBrokerConfig(brokerId, zkConnect)
      props ++= securityProps(sslProperties1, TRUSTSTORE_PROPS)
      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
      props.put(KafkaConfig.ListenersProp, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
      props.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"$SecureInternal:SSL, $SecureExternal:SASL_SSL")
      props.put(KafkaConfig.InterBrokerListenerNameProp, SecureInternal)
      props.put(KafkaConfig.SslClientAuthProp, "requested")
      props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, "PLAIN")
      props.put(KafkaConfig.ZkEnableSecureAclsProp, "true")
      props.put(KafkaConfig.SaslEnabledMechanismsProp, kafkaServerSaslMechanisms.mkString(","))
      props.put(KafkaConfig.LogSegmentBytesProp, "2000") // low value to test log rolling on config update
      props.put(KafkaConfig.NumReplicaFetchersProp, "2") // greater than one to test reducing threads
      props.put(KafkaConfig.ProducerQuotaBytesPerSecondDefaultProp, "10000000") // non-default value to trigger a new metric
      props.put(KafkaConfig.PasswordEncoderSecretProp, "dynamic-config-secret")

      props ++= sslProperties1
      props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(SecureInternal))

      // Set invalid top-level properties to ensure that listener config is used
      // Don't set any dynamic configs here since they get overridden in tests
      props ++= invalidSslProperties
      props ++= securityProps(invalidSslProperties, KEYSTORE_PROPS, "")
      props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(SecureExternal))

      val kafkaConfig = KafkaConfig.fromProps(props)
      configureDynamicKeystoreInZooKeeper(kafkaConfig, sslProperties1)

      servers += TestUtils.createServer(kafkaConfig)
    }

    TestUtils.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, OffsetConfig.DefaultOffsetsTopicNumPartitions,
      replicationFactor = numServers, servers, servers.head.groupCoordinator.offsetsTopicConfigs)

    TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor = numServers, servers)
    createAdminClient(SecurityProtocol.SSL, SecureInternal)

    TestMetricsReporter.testReporters.clear()
  }

  @After
  override def tearDown() {
    clientThreads.foreach(_.interrupt())
    clientThreads.foreach(_.initiateShutdown())
    clientThreads.foreach(_.join(5 * 1000))
    executors.foreach(_.shutdownNow())
    producers.foreach(_.close(0, TimeUnit.MILLISECONDS))
    consumers.foreach(_.close(0, TimeUnit.MILLISECONDS))
    adminClients.foreach(_.close())
    TestUtils.shutdownServers(servers)
    super.tearDown()
    closeSasl()
  }

  @Test
  def testKeyStoreDescribeUsingAdminClient(): Unit = {

    def verifyConfig(configName: String, configEntry: ConfigEntry, isSensitive: Boolean, expectedProps: Properties): Unit = {
      if (isSensitive) {
        assertTrue(s"Value is sensitive: $configName", configEntry.isSensitive)
        assertNull(s"Sensitive value returned for $configName", configEntry.value)
      } else {
        assertFalse(s"Config is not sensitive: $configName", configEntry.isSensitive)
        assertEquals(expectedProps.getProperty(configName), configEntry.value)
      }
    }

    def verifySynonym(configName: String, synonym: ConfigSynonym, isSensitive: Boolean,
                      expectedPrefix: String, expectedSource: ConfigSource, expectedProps: Properties): Unit = {
      if (isSensitive)
        assertNull(s"Sensitive value returned for $configName", synonym.value)
      else
        assertEquals(expectedProps.getProperty(configName), synonym.value)
      assertTrue(s"Expected listener config, got $synonym", synonym.name.startsWith(expectedPrefix))
      assertEquals(expectedSource, synonym.source)
    }

    def verifySynonyms(configName: String, synonyms: util.List[ConfigSynonym], isSensitive: Boolean,
                       prefix: String, defaultValue: Option[String]): Unit = {

      val overrideCount = if (prefix.isEmpty) 0 else 2
      assertEquals(s"Wrong synonyms for $configName: $synonyms", 1 + overrideCount + defaultValue.size, synonyms.size)
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
      KEYSTORE_PROPS.asScala.foreach { configName =>
        val desc = configEntry(configDesc, s"$prefix$configName")
        val isSensitive = configName.contains("password")
        verifyConfig(configName, desc, isSensitive, if (prefix.isEmpty) invalidSslProperties else sslProperties1)
        val defaultValue = if (configName == SSL_KEYSTORE_TYPE_CONFIG) Some("JKS") else None
        verifySynonyms(configName, desc.synonyms, isSensitive, prefix, defaultValue)
      }
    }

    val adminClient = adminClients.head
    alterSslKeystoreUsingConfigCommand(sslProperties1, SecureExternal)

    val configDesc = describeConfig(adminClient)
    verifySslConfig("listener.name.external.", sslProperties1, configDesc)
    verifySslConfig("", invalidSslProperties, configDesc)
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
  }

  @Test
  def testLogCleanerConfig(): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(0)

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
      props.asScala.foreach { case (k, v) =>
        assertEquals(s"Not reconfigured $k", server.config.originals.get(k).toString, v)
      }
    }

    // Verify that configs of existing logs have been updated
    val newLogConfig = LogConfig(KafkaServer.copyKafkaConfigToLog(servers.head.config))
    assertEquals(newLogConfig, servers.head.logManager.currentDefaultConfig)
    val log = servers.head.logManager.getLog(new TopicPartition(topic, 0)).getOrElse(throw new IllegalStateException("Log not found"))
    TestUtils.waitUntilTrue(() => log.config.segmentSize == 4000, "Existing topic config using defaults not updated")
    props.asScala.foreach { case (k, v) =>
      val logConfigName = DynamicLogConfig.KafkaConfigToLogConfigName(k)
      val expectedValue = if (k == KafkaConfig.LogCleanupPolicyProp) s"[$v]" else v
      assertEquals(s"Not reconfigured $logConfigName for existing log", expectedValue,
        log.config.originals.get(logConfigName).toString)
    }
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.LOG_APPEND_TIME)

    // Verify that the new config is actually used for new segments of existing logs
    TestUtils.waitUntilTrue(() => log.logSegments.exists(_.size > 3000), "Log segment size increase not applied")

    // Verify that overridden topic configs are not updated when broker default is updated
    val log2 = servers.head.logManager.getLog(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))
      .getOrElse(throw new IllegalStateException("Log not found"))
    assertFalse("Overridden clean up policy should not be updated", log2.config.delete)
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
    alterConfigsOnServer(servers.head, props)
    assertEquals(500000, servers.head.config.values.get(KafkaConfig.LogIndexSizeMaxBytesProp))
    servers.tail.foreach { server => assertEquals(Defaults.LogIndexSizeMaxBytes, server.config.values.get(KafkaConfig.LogIndexSizeMaxBytesProp)) }

    // Verify that produce/consume worked throughout this test without any retries in producer
    stopAndVerifyProduceConsume(producerThread, consumerThread)
  }

  @Test
  def testUncleanLeaderElectionEnable(): Unit = {
    val topic = "testtopic2"
    TestUtils.createTopic(zkClient, topic, 1, replicationFactor = 2, servers)
    val producer = ProducerBuilder().maxRetries(1000).acks(1).build()
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
    val controller = servers.find(_.config.brokerId == TestUtils.waitUntilControllerElected(zkClient)).get

    // Verify that new leader is not elected with unclean leader disabled since there are no ISRs
    TestUtils.waitUntilTrue(() => partitionInfo.leader == null, "Unclean leader elected")

    // Enable unclean leader election
    val newProps = new Properties
    newProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, "true")
    TestUtils.alterConfigs(servers, adminClients.head, newProps, perBrokerConfig = false).all.get
    waitForConfigOnServer(controller, KafkaConfig.UncleanLeaderElectionEnableProp, "true")

    // Verify that the old follower with missing records is elected as the new leader
    val (newLeader, elected) = TestUtils.computeUntilTrue(partitionInfo.leader)(leader => leader != null)
    assertTrue("Unclean leader not elected", elected)
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
    val requestHandlerPrefix = "kafka-request-handler-"
    val networkThreadPrefix = "kafka-network-thread-"
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
      val (producerThread, consumerThread) = startProduceConsume(numRetries)
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
    verifyThreads("kafka-socket-acceptor-", config.listeners.size)

    verifyProcessorMetrics()
    verifyMarkPartitionsForTruncation()
  }

  private def isProcessorMetric(metricName: MetricName): Boolean = {
    val mbeanName = metricName.getMBeanName
    mbeanName.contains(s"${Processor.NetworkProcessorMetricTag}=") || mbeanName.contains(s"${RequestChannel.ProcessorMetricTag}=")
  }

  private def clearLeftOverProcessorMetrics(): Unit = {
    val metricsFromOldTests = Metrics.defaultRegistry.allMetrics.keySet.asScala.filter(isProcessorMetric)
    metricsFromOldTests.foreach(Metrics.defaultRegistry.removeMetric)
  }

  // Verify that metrics from processors that were removed have been deleted.
  // Since processor ids are not reused, it is sufficient to check metrics count
  // based on the current number of processors
  private def verifyProcessorMetrics(): Unit = {
    val numProcessors = servers.head.config.numNetworkThreads * 2 // 2 listeners

    val kafkaMetrics = servers.head.metrics.metrics().keySet.asScala
      .filter(_.tags.containsKey(Processor.NetworkProcessorMetricTag))
      .groupBy(_.tags.get(Processor.NetworkProcessorMetricTag))
    assertEquals(numProcessors, kafkaMetrics.size)

    Metrics.defaultRegistry.allMetrics.keySet.asScala
      .filter(isProcessorMetric)
      .groupBy(_.getName)
      .foreach { case (name, set) => assertEquals(s"Metrics not deleted $name", numProcessors, set.size) }
  }

  // Verify that replicaFetcherManager.markPartitionsForTruncation uses the current fetcher thread size
  // to obtain partition assignment
  private def verifyMarkPartitionsForTruncation(): Unit = {
    val leaderId = 0
    val partitions = (0 until numPartitions).map(i => new TopicPartition(topic, i)).filter { tp =>
      zkClient.getLeaderForPartition(tp).contains(leaderId)
    }
    assertTrue(s"Partitons not found with leader $leaderId", partitions.nonEmpty)
    partitions.foreach { tp =>
      (1 to 2).foreach { i =>
        val replicaFetcherManager = servers(i).replicaManager.replicaFetcherManager
        val truncationOffset = tp.partition
        replicaFetcherManager.markPartitionsForTruncation(leaderId, tp, truncationOffset)
        val fetcherThreads = replicaFetcherManager.fetcherThreadMap.filter(_._2.partitionStates.contains(tp))
        assertEquals(1, fetcherThreads.size)
        assertEquals(replicaFetcherManager.getFetcherId(tp.topic, tp.partition), fetcherThreads.head._1.fetcherId)
        assertEquals(truncationOffset, fetcherThreads.head._2.partitionStates.stateValue(tp).fetchOffset)
      }
    }
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
      assertFalse("No metrics found", reporter.kafkaMetrics.isEmpty)
      reporter.verifyMetricValue("request-total", "socket-server-metrics")
    }
    assertEquals(servers.map(_.config.brokerId).toSet, TestMetricsReporter.configuredBrokers.toSet)

    val clientId = "test-client-1"
    val (producerThread, consumerThread) = startProduceConsume(retries = 0, clientId)
    TestUtils.waitUntilTrue(() => consumerThread.received >= 5, "Messages not sent")

    // Verify that JMX reporter is still active (test a metric registered after the dynamic reporter update)
    val mbeanServer = ManagementFactory.getPlatformMBeanServer
    val byteRate = mbeanServer.getAttribute(new ObjectName(s"kafka.server:type=Produce,client-id=$clientId"), "byte-rate")
    assertTrue("JMX attribute not updated", byteRate.asInstanceOf[Double] > 0)

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
    newReporters.foreach(_.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 2000))

    // Verify that validation failure of custom config fails reconfiguration and leaves config unchanged
    newProps.put(TestMetricsReporter.PollingIntervalProp, "invalid")
    reconfigureServers(newProps, perBrokerConfig = false, (TestMetricsReporter.PollingIntervalProp, "2000"), expectFailure = true)
    newReporters.foreach(_.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 2000))

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
    perBrokerReporter.verifyState(reconfigureCount = 1, deleteCount = 0, pollingInterval = 4000)
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
      assertTrue("Broker not registered", brokerInfo.nonEmpty)
      val endpoints = brokerInfo.get.endPoints.toString
      assertTrue(s"Endpoint update not saved $endpoints", endpointMatcher(endpoints))
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
    val producer1 = ProducerBuilder().trustStoreProps(sslProperties1)
      .maxRetries(0)
      .requestTimeoutMs(1000)
      .bootstrapServers(bootstrap)
      .build()

    assertTrue(intercept[ExecutionException] {
      producer1.send(new ProducerRecord(topic, "key", "value")).get(2, TimeUnit.SECONDS)
    }.getCause.isInstanceOf[org.apache.kafka.common.errors.TimeoutException])

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
    try {
      reconfigureServers(props, perBrokerConfig = true, (KafkaConfig.InterBrokerListenerNameProp, SecureExternal))
      fail("Inter-broker listener cannot be dynamically updated")
    } catch {
      case e: ExecutionException =>
        assertTrue(s"Unexpected exception ${e.getCause}", e.getCause.isInstanceOf[InvalidRequestException])
        servers.foreach(server => assertEquals(SecureInternal, server.config.interBrokerListenerName.value))
    }
  }

  @Test
  @Ignore // Re-enable once we make it less flaky (KAFKA-6824)
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
      val passwordConfigs = props.asScala.filterKeys(DynamicBrokerConfig.isPasswordConfig)
      assertTrue("Password configs not found", passwordConfigs.nonEmpty)
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
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword)
    initializeKerberos()

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
    assertTrue("Did not find partitions with no leader", partitions.exists(_.leader == null))
  }

  private def addListener(servers: Seq[KafkaServer], listenerName: String, securityProtocol: SecurityProtocol,
                          saslMechanisms: Seq[String]): Unit = {
    val config = servers.head.config
    val existingListenerCount = config.listeners.size
    val listeners = config.listeners
      .map(e => s"${e.listenerName.value}://${e.host}:${e.port}")
      .mkString(",") + s",$listenerName://localhost:0"
    val listenerMap = config.listenerSecurityProtocolMap
      .map { case (name, protocol) => s"${name.value}:${protocol.name}" }
      .mkString(",") + s",$listenerName:${securityProtocol.name}"

    val props = fetchBrokerConfigsFromZooKeeper(servers.head)
    props.put(KafkaConfig.ListenersProp, listeners)
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, listenerMap)
    securityProtocol match {
      case SecurityProtocol.SSL =>
        addListenerPropsSsl(listenerName, props)
      case SecurityProtocol.SASL_PLAINTEXT =>
        addListenerPropsSasl(listenerName, saslMechanisms, props)
      case SecurityProtocol.SASL_SSL =>
        addListenerPropsSasl(listenerName, saslMechanisms, props)
        addListenerPropsSsl(listenerName, props)
      case SecurityProtocol.PLAINTEXT => // no additional props
    }

    // Add a config to verify that configs whose types are not known are not returned by describeConfigs()
    val unknownConfig = "some.config"
    props.put(unknownConfig, "some.config.value")

    TestUtils.alterConfigs(servers, adminClients.head, props, perBrokerConfig = true).all.get

    TestUtils.waitUntilTrue(() => servers.forall(server => server.config.listeners.size == existingListenerCount + 1),
      "Listener config not updated")
    TestUtils.waitUntilTrue(() => servers.forall(server => {
      try {
        server.socketServer.boundPort(new ListenerName(listenerName)) > 0
      } catch {
        case _: Exception => false
      }
    }), "Listener not created")

    val brokerConfigs = describeConfig(adminClients.head, servers).entries.asScala
    props.asScala.foreach { case (name, value) =>
      val entry = brokerConfigs.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Config not found $name"))
      if (DynamicBrokerConfig.isPasswordConfig(name) || name == unknownConfig)
        assertNull(s"Password or unknown config returned $entry", entry.value)
      else
        assertEquals(value, entry.value)
    }
  }

  private def verifyAddListener(listenerName: String, securityProtocol: SecurityProtocol,
                                saslMechanisms: Seq[String]): Unit = {
    addListener(servers, listenerName, securityProtocol, saslMechanisms)
    if (saslMechanisms.nonEmpty)
      saslMechanisms.foreach { mechanism =>
        verifyListener(securityProtocol, Some(mechanism), s"add-listener-group-$securityProtocol-$mechanism")
      }
    else
      verifyListener(securityProtocol, None, s"add-listener-group-$securityProtocol")
  }

  private def verifyRemoveListener(listenerName: String, securityProtocol: SecurityProtocol,
                                   saslMechanisms: Seq[String]): Unit = {
    val saslMechanism = if (saslMechanisms.isEmpty) "" else saslMechanisms.head
    val producer1 = ProducerBuilder().listenerName(listenerName)
      .securityProtocol(securityProtocol)
      .saslMechanism(saslMechanism)
      .maxRetries(1000)
      .build()
    val consumer1 = ConsumerBuilder(s"remove-listener-group-$securityProtocol")
      .listenerName(listenerName)
      .securityProtocol(securityProtocol)
      .saslMechanism(saslMechanism)
      .autoOffsetReset("latest")
      .build()
    verifyProduceConsume(producer1, consumer1, numRecords = 10, topic)
    // send another message to check consumer later
    producer1.send(new ProducerRecord(topic, "key", "value")).get(100, TimeUnit.MILLISECONDS)

    val config = servers.head.config
    val existingListenerCount = config.listeners.size
    val listeners = config.listeners
      .filter(e => e.listenerName.value != securityProtocol.name)
      .map(e => s"${e.listenerName.value}://${e.host}:${e.port}")
      .mkString(",")
    val listenerMap = config.listenerSecurityProtocolMap
      .filterKeys(listenerName => listenerName.value != securityProtocol.name)
      .map { case (listenerName, protocol) => s"${listenerName.value}:${protocol.name}" }
      .mkString(",")

    val props = fetchBrokerConfigsFromZooKeeper(servers.head)
    val listenerProps = props.asScala.keySet.filter(_.startsWith(listenerPrefix(listenerName)))
    listenerProps.foreach(props.remove)
    props.put(KafkaConfig.ListenersProp, listeners)
    props.put(KafkaConfig.ListenerSecurityProtocolMapProp, listenerMap)
    TestUtils.alterConfigs(servers, adminClients.head, props, perBrokerConfig = true).all.get

    TestUtils.waitUntilTrue(() => servers.forall(server => server.config.listeners.size == existingListenerCount - 1),
      "Listeners not updated")

    // Test that connections using deleted listener don't work
    val producerFuture = verifyConnectionFailure(producer1)
    val consumerFuture = verifyConnectionFailure(consumer1)

    // Test that other listeners still work
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions, replicationFactor = numServers, servers)
    val producer2 = ProducerBuilder().trustStoreProps(sslProperties1).maxRetries(0).build()
    val consumer2 = ConsumerBuilder(s"remove-listener-group2-$securityProtocol")
      .trustStoreProps(sslProperties1)
      .topic(topic2)
      .autoOffsetReset("latest")
      .build()
    verifyProduceConsume(producer2, consumer2, numRecords = 10, topic2)

    // Verify that producer/consumer using old listener don't work
    verifyTimeout(producerFuture)
    verifyTimeout(consumerFuture)
  }

  private def verifyListener(securityProtocol: SecurityProtocol, saslMechanism: Option[String], groupId: String): Unit = {
    val mechanism = saslMechanism.getOrElse("")
    val retries = 1000 // since it may take time for metadata to be updated on all brokers
    val producer = ProducerBuilder().listenerName(securityProtocol.name)
      .securityProtocol(securityProtocol)
      .saslMechanism(mechanism)
      .maxRetries(retries)
      .build()
    val consumer = ConsumerBuilder(s"add-listener-group-$securityProtocol-$mechanism")
      .listenerName(securityProtocol.name)
      .securityProtocol(securityProtocol)
      .saslMechanism(mechanism)
      .autoOffsetReset("latest")
      .build()
    verifyProduceConsume(producer, consumer, numRecords = 10, topic)
  }

  private def fetchBrokerConfigsFromZooKeeper(server: KafkaServer): Properties = {
    val props = adminZkClient.fetchEntityConfig(ConfigType.Broker, server.config.brokerId.toString)
    server.config.dynamicConfig.fromPersistentProps(props, perBrokerConfig = true)
  }

  private def awaitInitialPositions(consumer: KafkaConsumer[_, _]): Unit = {
    do {
      consumer.poll(1)
    } while (consumer.assignment.isEmpty)
    consumer.assignment.asScala.foreach(tp => consumer.position(tp))
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

  private def createAdminClient(securityProtocol: SecurityProtocol, listenerName: String): AdminClient = {
    val config = clientProps(securityProtocol)
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(listenerName))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = AdminClient.create(config)
    adminClients += adminClient
    adminClient
  }

  private def verifyProduceConsume(producer: KafkaProducer[String, String],
                                   consumer: KafkaConsumer[String, String],
                                   numRecords: Int,
                                   topic: String): Unit = {
    val producerRecords = (1 to numRecords).map(i => new ProducerRecord(topic, s"key$i", s"value$i"))
    producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))
    var received = 0
    TestUtils.waitUntilTrue(() => {
      received += consumer.poll(50).count
      received >= numRecords
    }, s"Consumed $received records until timeout instead of the expected $numRecords records")
    assertEquals(numRecords, received)
  }

  private def verifyAuthenticationFailure(producer: KafkaProducer[_, _]): Unit = {
    try {
      producer.partitionsFor(topic)
      fail("Producer connection did not fail with invalid keystore")
    } catch {
      case _:AuthenticationException => // expected exception
    }
  }

  private def describeConfig(adminClient: AdminClient, servers: Seq[KafkaServer] = this.servers): Config = {
    val configResources = servers.map { server =>
      new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
    }
    val describeOptions = new DescribeConfigsOptions().includeSynonyms(true)
    val describeResult = adminClient.describeConfigs(configResources.asJava, describeOptions).all.get
    assertEquals(servers.size, describeResult.values.size)
    val configDescription = describeResult.values.iterator.next
    assertFalse("Configs are empty", configDescription.entries.isEmpty)
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
      val in = new FileInputStream(props.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG))
      try {
        ks.load(in, password.toCharArray)
      } finally {
        in.close()
      }
      ks
    }
    val cert1 = load(trustStore1Props).getCertificate("kafka")
    val cert2 = load(trustStore2Props).getCertificate("kafka")
    val certs = Map("kafka1" -> cert1, "kafka2" -> cert2)

    val combinedStorePath = File.createTempFile("truststore", ".jks").getAbsolutePath
    val password = trustStore1Props.get(SSL_TRUSTSTORE_PASSWORD_CONFIG).asInstanceOf[Password]
    TestSslUtils.createTrustStore(combinedStorePath, password, certs.asJava)
    val newStoreProps = new Properties
    newStoreProps.put(SSL_TRUSTSTORE_LOCATION_CONFIG, combinedStorePath)
    newStoreProps.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, password)
    newStoreProps.put(SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
    newStoreProps
  }

  private def alterSslKeystore(adminClient: AdminClient, props: Properties, listener: String, expectFailure: Boolean  = false): Unit = {
    val configPrefix = listenerPrefix(listener)
    val newProps = securityProps(props, KEYSTORE_PROPS, configPrefix)
    reconfigureServers(newProps, perBrokerConfig = true,
      (s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)), expectFailure)
  }

  private def alterSslKeystoreUsingConfigCommand(props: Properties, listener: String): Unit = {
    val configPrefix = listenerPrefix(listener)
    val newProps = securityProps(props, KEYSTORE_PROPS, configPrefix)

    val propsFile = TestUtils.tempFile()
    val propsWriter = new FileWriter(propsFile)
    try {
      clientProps(SecurityProtocol.SSL).asScala.foreach {
        case (k, v) => propsWriter.write(s"$k=$v\n")
      }
    } finally {
      propsWriter.close()
    }

    servers.foreach { server =>
      val args = Array("--bootstrap-server", TestUtils.bootstrapServers(servers, new ListenerName(SecureInternal)),
        "--command-config", propsFile.getAbsolutePath,
        "--alter", "--add-config", newProps.asScala.map { case (k, v) => s"$k=$v" }.mkString(","),
        "--entity-type", "brokers",
        "--entity-name", server.config.brokerId.toString)
      ConfigCommand.main(args)
    }
    waitForConfig(s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
  }

  private def serverEndpoints(adminClient: AdminClient): String = {
    val nodes = adminClient.describeCluster().nodes().get
    nodes.asScala.map { node =>
      s"${node.host}:${node.port}"
    }.mkString(",")
  }

  private def alterAdvertisedListener(adminClient: AdminClient, externalAdminClient: AdminClient, oldHost: String, newHost: String): Unit = {
    val configs = servers.map { server =>
      val resource = new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
      val newListeners = server.config.advertisedListeners.map { e =>
        if (e.listenerName.value == SecureExternal)
          s"${e.listenerName.value}://$newHost:${server.boundPort(e.listenerName)}"
        else
          s"${e.listenerName.value}://${e.host}:${server.boundPort(e.listenerName)}"
      }.mkString(",")
      val configEntry = new ConfigEntry(KafkaConfig.AdvertisedListenersProp, newListeners)
      (resource, new Config(Collections.singleton(configEntry)))
    }.toMap.asJava
    adminClient.alterConfigs(configs).all.get
    servers.foreach { server =>
      TestUtils.retry(10000) {
        val externalListener = server.config.advertisedListeners.find(_.listenerName.value == SecureExternal)
          .getOrElse(throw new IllegalStateException("External listener not found"))
        assertTrue("Config not updated", externalListener.host == newHost)
      }
    }
    val (endpoints, altered) = TestUtils.computeUntilTrue(serverEndpoints(externalAdminClient)) { endpoints =>
      !endpoints.contains(oldHost)
    }
    assertTrue(s"Advertised listener update not propagated by controller: $endpoints", altered)
  }

  private def alterConfigsOnServer(server: KafkaServer, props: Properties): Unit = {
    val configEntries = props.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = Map(new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString) -> newConfig).asJava
    adminClients.head.alterConfigs(configs).all.get
    props.asScala.foreach { case (k, v) => waitForConfigOnServer(server, k, v) }
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String), expectFailure: Boolean = false): Unit = {
    val alterResult = TestUtils.alterConfigs(servers, adminClients.head, newProps, perBrokerConfig)
    if (expectFailure) {
      val oldProps = servers.head.config.values.asScala.filterKeys(newProps.containsKey)
      val brokerResources = if (perBrokerConfig)
        servers.map(server => new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString))
      else
        Seq(new ConfigResource(ConfigResource.Type.BROKER, ""))
      brokerResources.foreach { brokerResource =>
        val exception = intercept[ExecutionException](alterResult.values.get(brokerResource).get)
        assertTrue(exception.getCause.isInstanceOf[InvalidRequestException])
      }
      servers.foreach { server => assertEquals(oldProps, server.config.values.asScala.filterKeys(newProps.containsKey)) }
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

  private def configureDynamicKeystoreInZooKeeper(kafkaConfig: KafkaConfig, sslProperties: Properties): Unit = {
    val externalListenerPrefix = listenerPrefix(SecureExternal)
    val sslStoreProps = new Properties
    sslStoreProps ++= securityProps(sslProperties, KEYSTORE_PROPS, externalListenerPrefix)
    sslStoreProps.put(KafkaConfig.PasswordEncoderSecretProp, kafkaConfig.passwordEncoderSecret.map(_.value).orNull)
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    val args = Array("--zookeeper", kafkaConfig.zkConnect,
      "--alter", "--add-config", sslStoreProps.asScala.map { case (k, v) => s"$k=$v" }.mkString(","),
      "--entity-type", "brokers",
      "--entity-name", kafkaConfig.brokerId.toString)
    ConfigCommand.main(args)

    val passwordEncoder = createPasswordEncoder(kafkaConfig, kafkaConfig.passwordEncoderSecret)
    val brokerProps = adminZkClient.fetchEntityConfig("brokers", kafkaConfig.brokerId.toString)
    assertEquals(4, brokerProps.size)
    assertEquals(sslProperties.get(SSL_KEYSTORE_TYPE_CONFIG),
      brokerProps.getProperty(s"$externalListenerPrefix$SSL_KEYSTORE_TYPE_CONFIG"))
    assertEquals(sslProperties.get(SSL_KEYSTORE_LOCATION_CONFIG),
      brokerProps.getProperty(s"$externalListenerPrefix$SSL_KEYSTORE_LOCATION_CONFIG"))
    assertEquals(sslProperties.get(SSL_KEYSTORE_PASSWORD_CONFIG),
      passwordEncoder.decode(brokerProps.getProperty(s"$externalListenerPrefix$SSL_KEYSTORE_PASSWORD_CONFIG")))
    assertEquals(sslProperties.get(SSL_KEY_PASSWORD_CONFIG),
      passwordEncoder.decode(brokerProps.getProperty(s"$externalListenerPrefix$SSL_KEY_PASSWORD_CONFIG")))
  }

  private def createPasswordEncoder(config: KafkaConfig, secret: Option[Password]): PasswordEncoder = {
    val encoderSecret = secret.getOrElse(throw new IllegalStateException("Password encoder secret not configured"))
    new PasswordEncoder(encoderSecret,
      config.passwordEncoderKeyFactoryAlgorithm,
      config.passwordEncoderCipherAlgorithm,
      config.passwordEncoderKeyLength,
      config.passwordEncoderIterations)
  }

  private def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server => waitForConfigOnServer(server, propName, propValue, maxWaitMs) }
  }

  private def waitForConfigOnServer(server: KafkaServer, propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
    }
  }

  private def configureMetricsReporters(reporters: Seq[Class[_]], props: Properties,
                                       perBrokerConfig: Boolean = false): Unit = {
    val reporterStr = reporters.map(_.getName).mkString(",")
    props.put(KafkaConfig.MetricReporterClassesProp, reporterStr)
    reconfigureServers(props, perBrokerConfig, (KafkaConfig.MetricReporterClassesProp, reporterStr))
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
    assertTrue(s"Invalid threads: expected $expectedCount, got ${threads.size}: $threads", resized)
  }

  private def startProduceConsume(retries: Int, producerClientId: String = "test-producer"): (ProducerThread, ConsumerThread) = {
    val producerThread = new ProducerThread(producerClientId, retries)
    clientThreads += producerThread
    val consumerThread = new ConsumerThread(producerThread)
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
      assertFalse("Duplicates not expected", consumerThread.duplicates)
    assertFalse("Some messages received out of order", consumerThread.outOfOrder)
  }

  private def verifyConnectionFailure(producer: KafkaProducer[String, String]): Future[_] = {
    val executor = Executors.newSingleThreadExecutor
    executors += executor
    val future = executor.submit(new Runnable() {
      def run() {
        producer.send(new ProducerRecord(topic, "key", "value")).get
      }
    })
    verifyTimeout(future)
    future
  }

  private def verifyConnectionFailure(consumer: KafkaConsumer[String, String]): Future[_] = {
    val executor = Executors.newSingleThreadExecutor
    executors += executor
    val future = executor.submit(new Runnable() {
      def run() {
        assertEquals(0, consumer.poll(100).count)
      }
    })
    verifyTimeout(future)
    future
  }

  private def verifyTimeout(future: Future[_]): Unit = {
    try {
      future.get(100, TimeUnit.MILLISECONDS)
      fail("Operation should not have completed")
    } catch {
      case _: TimeoutException => // expected exception
    }
  }

  private def configValueAsString(value: Any): String = {
    value match {
      case password: Password => password.value
      case list: util.List[_] => list.asScala.map(_.toString).mkString(",")
      case _ => value.toString
    }
  }

  private def addListenerPropsSsl(listenerName: String, props: Properties): Unit = {
    props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(listenerName))
    props ++= securityProps(sslProperties1, TRUSTSTORE_PROPS, listenerPrefix(listenerName))
  }

  private def addListenerPropsSasl(listener: String, mechanisms: Seq[String], props: Properties): Unit = {
    val listenerName = new ListenerName(listener)
    val prefix = listenerName.configPrefix
    props.put(prefix + KafkaConfig.SaslEnabledMechanismsProp, mechanisms.mkString(","))
    props.put(prefix + KafkaConfig.SaslKerberosServiceNameProp, "kafka")
    mechanisms.foreach { mechanism =>
      val jaasSection = jaasSections(Seq(mechanism), None, KafkaSasl, "").head
      val jaasConfig = jaasSection.modules.head.toString
      props.put(listenerName.saslMechanismConfigPrefix(mechanism) + KafkaConfig.SaslJaasConfigProp, jaasConfig)
    }
  }

  private abstract class ClientBuilder[T]() {
    protected var _bootstrapServers: Option[String] = None
    protected var _listenerName = SecureExternal
    protected var _securityProtocol = SecurityProtocol.SASL_SSL
    protected var _saslMechanism = kafkaClientSaslMechanism
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
    private var _retries = 0
    private var _acks = -1
    private var _requestTimeoutMs = 30000L

    def maxRetries(retries: Int): ProducerBuilder = { _retries = retries; this }
    def acks(acks: Int): ProducerBuilder = { _acks = acks; this }
    def requestTimeoutMs(timeoutMs: Long): ProducerBuilder = { _requestTimeoutMs = timeoutMs; this }

    override def build(): KafkaProducer[String, String] = {
      val producer = TestUtils.createProducer(bootstrapServers,
        acks = _acks,
        requestTimeoutMs = _requestTimeoutMs,
        retries = _retries,
        securityProtocol = _securityProtocol,
        trustStoreFile = Some(trustStoreFile1),
        keySerializer = new StringSerializer,
        valueSerializer = new StringSerializer,
        props = Some(propsOverride))
      producers += producer
      producer
    }
  }

  private case class ConsumerBuilder(group: String) extends ClientBuilder[KafkaConsumer[String, String]] {
    private var _autoOffsetReset = "earliest"
    private var _enableAutoCommit = false
    private var _topic = DynamicBrokerReconfigurationTest.this.topic

    def autoOffsetReset(reset: String): ConsumerBuilder = { _autoOffsetReset = reset; this }
    def enableAutoCommit(enable: Boolean): ConsumerBuilder = { _enableAutoCommit = enable; this }
    def topic(topic: String): ConsumerBuilder = { _topic = topic; this }

    override def build(): KafkaConsumer[String, String] = {
      val consumerProps = propsOverride
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, _enableAutoCommit.toString)
      val consumer = TestUtils.createConsumer(bootstrapServers,
        group,
        autoOffsetReset = _autoOffsetReset,
        securityProtocol = _securityProtocol,
        trustStoreFile = Some(trustStoreFile1),
        keyDeserializer = new StringDeserializer,
        valueDeserializer = new StringDeserializer,
        props = Some(consumerProps))
      consumer.subscribe(Collections.singleton(_topic))
      if (_autoOffsetReset == "latest")
        awaitInitialPositions(consumer)
      consumers += consumer
      consumer
    }
  }

  private class ProducerThread(clientId: String, retries: Int) extends
      ShutdownableThread(clientId, isInterruptible = false) {
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

  private class ConsumerThread(producerThread: ProducerThread) extends ShutdownableThread("test-consumer", isInterruptible = false) {
    private val consumer = ConsumerBuilder("group1").enableAutoCommit(true).build()
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
          val records = consumer.poll(50)
          received += records.count
          if (!records.isEmpty) {
            lastBatch = records
            records.partitions.asScala.foreach { tp =>
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
  @volatile var configureCount = 0
  @volatile var reconfigureCount = 0
  @volatile var closeCount = 0
  @volatile var clusterUpdateCount = 0
  @volatile var pollingInterval: Int = -1
  testReporters.add(this)

  override def init(metrics: util.List[KafkaMetric]): Unit = {
    kafkaMetrics ++= metrics.asScala
    initializeCount += 1
  }

  override def configure(configs: util.Map[String, _]): Unit = {
    configuredBrokers += configs.get(KafkaConfig.BrokerIdProp).toString.toInt
    configureCount += 1
    pollingInterval = configs.get(PollingIntervalProp).toString.toInt
  }

  override def metricChange(metric: KafkaMetric): Unit = {
  }

  override def metricRemoval(metric: KafkaMetric): Unit = {
    kafkaMetrics -= metric
  }

  override def onUpdate(clusterResource: ClusterResource): Unit = {
    assertNotNull("Cluster id not set", clusterResource.clusterId)
    clusterUpdateCount += 1
  }

  override def reconfigurableConfigs(): util.Set[String] = {
    Set(PollingIntervalProp).asJava
  }

  override def validateReconfiguration(configs: util.Map[String, _]): Unit = {
    val pollingInterval = configs.get(PollingIntervalProp).toString
    if (configs.get(PollingIntervalProp).toString.toInt <= 0)
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
    assertEquals(reconfigureCount, reconfigureCount)
    assertEquals(deleteCount, closeCount)
    assertEquals(1, clusterUpdateCount)
    assertEquals(pollingInterval, this.pollingInterval)
  }

  def verifyMetricValue(name: String, group: String): Unit = {
    val matchingMetrics = kafkaMetrics.filter(metric => metric.metricName.name == name && metric.metricName.group == group)
    assertTrue("Metric not found", matchingMetrics.nonEmpty)
    val total = matchingMetrics.foldLeft(0.0)((total, metric) => total + metric.metricValue.asInstanceOf[Double])
    assertTrue("Invalid metric value", total > 0.0)
  }
}
