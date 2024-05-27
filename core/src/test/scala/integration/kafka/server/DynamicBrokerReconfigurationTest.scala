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

import kafka.api.KafkaSasl

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.lang.management.ManagementFactory
import java.util.{Collections, Properties}
import java.util.concurrent._
import javax.management.ObjectName
import kafka.controller.{ControllerBrokerStateInfo, ControllerChannelManager}
import kafka.log.UnifiedLog
import kafka.network.{DataPlaneAcceptor, Processor}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.utils.TestUtils.TestControllerRequestCompletionHandler
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{TopicPartition, TopicPartitionInfo}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.MetadataRequestData
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.requests.MetadataRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramCredential
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.PasswordEncoderConfigs
import org.apache.kafka.server.config.{ConfigType, KafkaSecurityConfigs, ReplicationConfigs, ServerLogConfigs}
import org.apache.kafka.server.metrics.{KafkaYammerMetrics, MetricConfigs}
import org.apache.kafka.server.record.BrokerCompressionType
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig}
import org.apache.kafka.test.{TestSslUtils, TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{Disabled, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.security.KeyStore
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.collection._
import scala.jdk.CollectionConverters._

class DynamicBrokerReconfigurationTest extends AbstractDynamicBrokerReconfigurationTest {
  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testTrustStoreAlter(quorum: String): Unit = {
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

    def verifyBrokerToControllerCall(controller: KafkaServer): Unit = {
      val nonControllerBroker = servers.find(_.config.brokerId != controller.config.brokerId).get
      val brokerToControllerManager = nonControllerBroker.clientToControllerChannelManager
      val completionHandler = new TestControllerRequestCompletionHandler()
      brokerToControllerManager.sendRequest(new MetadataRequest.Builder(new MetadataRequestData()), completionHandler)
      TestUtils.waitUntilTrue(() => {
        completionHandler.completed.get() || completionHandler.timedOut.get()
      }, "Timed out while waiting for broker to controller API call")
      // we do not expect a timeout from broker to controller request
      assertFalse(completionHandler.timedOut.get(), "broker to controller request is timeout")
      assertTrue(completionHandler.actualResponse.isDefined, "No response recorded even though request is completed")
      val response = completionHandler.actualResponse.get
      assertNull(response.authenticationException(), s"Request failed due to authentication error ${response.authenticationException}")
      assertNull(response.versionMismatch(), s"Request failed due to unsupported version error ${response.versionMismatch}")
      assertFalse(response.wasDisconnected(), "Request failed because broker is not available")
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

    if (!isKRaftTest()) {
      val controller = servers.find(_.config.brokerId == TestUtils.waitUntilControllerElected(zkClient)).get.asInstanceOf[KafkaServer]
      val controllerChannelManager = controller.kafkaController.controllerChannelManager
      val brokerStateInfo: mutable.HashMap[Int, ControllerBrokerStateInfo] =
        JTestUtils.fieldValue(controllerChannelManager, classOf[ControllerChannelManager], "brokerStateInfo")
      brokerStateInfo(0).networkClient.disconnect("0")
      TestUtils.createTopic(zkClient, "testtopic2", numPartitions, replicationFactor = numServers, servers)

      // validate that the brokerToController request works fine
      verifyBrokerToControllerCall(controller)
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testLogCleanerConfig(quorum: String): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)

    verifyThreads("kafka-log-cleaner-thread-", countPerBroker = 1)

    val props = new Properties
    props.put(CleanerConfig.LOG_CLEANER_THREADS_PROP, "2")
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "20000000")
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_LOAD_FACTOR_PROP, "0.8")
    props.put(CleanerConfig.LOG_CLEANER_IO_BUFFER_SIZE_PROP, "300000")
    props.put(KafkaConfig.MessageMaxBytesProp, "40000")
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
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testConsecutiveConfigChange(quorum: String): Unit = {
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

  @Test
  @nowarn("cat=deprecation") // See `TopicConfig.MESSAGE_FORMAT_VERSION_CONFIG` for deprecation details
  def testDefaultTopicConfig(): Unit = {
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)

    val props = new Properties
    props.put(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, "4000")
    props.put(ServerLogConfigs.LOG_ROLL_TIME_MILLIS_CONFIG, TimeUnit.HOURS.toMillis(2).toString)
    props.put(ServerLogConfigs.LOG_ROLL_TIME_JITTER_MILLIS_CONFIG, TimeUnit.HOURS.toMillis(1).toString)
    props.put(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG, "100000")
    props.put(ServerLogConfigs.LOG_FLUSH_INTERVAL_MESSAGES_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_FLUSH_INTERVAL_MS_CONFIG, "60000")
    props.put(ServerLogConfigs.LOG_RETENTION_BYTES_CONFIG, "10000000")
    props.put(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG, TimeUnit.DAYS.toMillis(1).toString)
    props.put(KafkaConfig.MessageMaxBytesProp, "100000")
    props.put(ServerLogConfigs.LOG_INDEX_INTERVAL_BYTES_CONFIG, "10000")
    props.put(CleanerConfig.LOG_CLEANER_DELETE_RETENTION_MS_PROP, TimeUnit.DAYS.toMillis(1).toString)
    props.put(CleanerConfig.LOG_CLEANER_MIN_COMPACTION_LAG_MS_PROP, "60000")
    props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "60000")
    props.put(CleanerConfig.LOG_CLEANER_MIN_CLEAN_RATIO_PROP, "0.3")
    props.put(ServerLogConfigs.LOG_CLEANUP_POLICY_CONFIG, "delete")
    props.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false")
    props.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
    props.put(KafkaConfig.CompressionTypeProp, "gzip")
    props.put(ServerLogConfigs.LOG_PRE_ALLOCATE_CONFIG, true.toString)
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.toString)
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_DOWNCONVERSION_ENABLE_CONFIG, "false")
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
    props.asScala.foreach { case (k, v) =>
      val logConfigName = DynamicLogConfig.KafkaConfigToLogConfigName(k)
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
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG, "1000")
    props.put(ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG, "1000")
    reconfigureServers(props, perBrokerConfig = false, (ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.toString))
    consumerThread.waitForMatchingRecords(record => record.timestampType == TimestampType.CREATE_TIME)
    // Verify that invalid configs are not applied
    val invalidProps = Map(
      ServerLogConfigs.LOG_MESSAGE_TIMESTAMP_DIFFERENCE_MAX_MS_CONFIG -> "abc", // Invalid type
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
      assertEquals(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_DEFAULT, server.config.values.get(ServerLogConfigs.LOG_INDEX_SIZE_MAX_BYTES_CONFIG))
      assertEquals(1680000000L, server.config.values.get(ServerLogConfigs.LOG_RETENTION_TIME_MILLIS_CONFIG))
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
    waitForConfigOnServer(controller, ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")

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
      maybeVerifyThreadPoolSize(threadPoolSize, threadPrefix)
    }

    val config = servers.head.config
    verifyThreadPoolResize(KafkaConfig.NumIoThreadsProp, config.numIoThreads,
      requestHandlerPrefix, mayReceiveDuplicates = false)
    verifyThreadPoolResize(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, config.numReplicaFetchers,
      fetcherThreadPrefix, mayReceiveDuplicates = false)
    verifyThreadPoolResize(KafkaConfig.BackgroundThreadsProp, config.backgroundThreads,
      "kafka-scheduler-", mayReceiveDuplicates = false)
    verifyThreadPoolResize(ServerLogConfigs.NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG, config.numRecoveryThreadsPerDataDir,
      "", mayReceiveDuplicates = false)
    verifyThreadPoolResize(KafkaConfig.NumNetworkThreadsProp, config.numNetworkThreads,
      networkThreadPrefix, mayReceiveDuplicates = true)
    verifyThreads("data-plane-kafka-socket-acceptor-", config.listeners.size)

    verifyProcessorMetrics()
    verifyMarkPartitionsForTruncation()
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
    val partitions = (0 until numPartitions).map(i => new TopicPartition(topic, i)).filter { tp =>
      zkClient.getLeaderForPartition(tp).contains(leaderId)
    }
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

  @Test
  // Modifying advertised listeners is not supported in KRaft
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
    servers.foreach { server =>
      validateEndpointsInZooKeeper(server.asInstanceOf[KafkaServer], endpoints => endpoints.contains(invalidHost))
    }

    // Trigger session expiry and ensure that controller registers new advertised listener after expiry
    val controllerEpoch = zkClient.getControllerEpoch
    val controllerServer = servers(zkClient.getControllerId.getOrElse(throw new IllegalStateException("No controller"))).asInstanceOf[KafkaServer]
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
    servers.foreach { server =>
      validateEndpointsInZooKeeper(server.asInstanceOf[KafkaServer], endpoints => !endpoints.contains(invalidHost))
    }

    // Verify that produce/consume work now
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions, replicationFactor = numServers, servers)
    val producer = ProducerBuilder().trustStoreProps(sslProperties1).maxRetries(0).build()
    val consumer = ConsumerBuilder("group2").trustStoreProps(sslProperties1).topic(topic2).build()
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify updating inter-broker listener
    val props = new Properties
    props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, SecureExternal)
    val e = assertThrows(classOf[ExecutionException], () => reconfigureServers(props, perBrokerConfig = true, (ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, SecureExternal)))
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
      val props = adminZkClient.fetchEntityConfig(ConfigType.BROKER, server.config.brokerId.toString)
      val propsEncodedWithOldSecret = props.clone().asInstanceOf[Properties]
      val config = server.config
      val oldSecret = "old-dynamic-config-secret"
      config.dynamicConfig.staticBrokerConfigs.put(PasswordEncoderConfigs.PASSWORD_ENCODER_OLD_SECRET_CONFIG, oldSecret)
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
      val updatedProps = adminZkClient.fetchEntityConfig(ConfigType.BROKER, brokerId.toString)
      passwordConfigs.foreach { case (name, value) => assertNotEquals(props.get(value), updatedProps.get(name)) }

      server.startup()
      TestUtils.retry(10000) {
        val newProps = adminZkClient.fetchEntityConfig(ConfigType.BROKER, brokerId.toString)
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

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testReconfigureRemovedListener(quorum: String): Unit = {
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

  private def addListener(servers: Seq[KafkaBroker], listenerName: String, securityProtocol: SecurityProtocol,
                          saslMechanisms: Seq[String]): Unit = {
    val config = servers.head.config
    val existingListenerCount = config.listeners.size
    val listeners = config.listeners
      .map(e => s"${e.listenerName.value}://${e.host}:${e.port}")
      .mkString(",") + s",$listenerName://localhost:0"
    val listenerMap = config.effectiveListenerSecurityProtocolMap
      .map { case (name, protocol) => s"${name.value}:${protocol.name}" }
      .mkString(",") + s",$listenerName:${securityProtocol.name}"

    val props = fetchBrokerConfigsFromZooKeeper(servers.head)
    props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, listenerMap)
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

    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props, perBrokerConfig = true).all.get

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
        assertNull(entry.value, s"Password or unknown config returned $entry")
      else
        assertEquals(value, entry.value)
    }
  }

  private def verifyAddListener(listenerName: String, securityProtocol: SecurityProtocol,
                                saslMechanisms: Seq[String]): Unit = {
    addListener(servers, listenerName, securityProtocol, saslMechanisms)
    TestUtils.waitUntilTrue(() => servers.forall(hasListenerMetric(_, listenerName)),
      "Processors not started for new listener")
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

    val config = servers.head.config
    val existingListenerCount = config.listeners.size
    val listeners = config.listeners
      .filter(e => e.listenerName.value != securityProtocol.name)
      .map(e => s"${e.listenerName.value}://${e.host}:${e.port}")
      .mkString(",")
    val listenerMap = config.effectiveListenerSecurityProtocolMap
      .filter { case (listenerName, _) => listenerName.value != securityProtocol.name }
      .map { case (listenerName, protocol) => s"${listenerName.value}:${protocol.name}" }
      .mkString(",")

    val props = fetchBrokerConfigsFromZooKeeper(servers.head)
    val deleteListenerProps = new Properties()
    deleteListenerProps ++= props.asScala.filter(entry => entry._1.startsWith(listenerPrefix(listenerName)))
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, deleteListenerProps, perBrokerConfig = true, opType = OpType.DELETE).all.get

    props.clear()
    props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, listenerMap)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props, perBrokerConfig = true).all.get

    TestUtils.waitUntilTrue(() => servers.forall(server => server.config.listeners.size == existingListenerCount - 1),
      "Listeners not updated")
    // Wait until metrics of the listener have been removed to ensure that processors have been shutdown before
    // verifying that connections to the removed listener fail.
    TestUtils.waitUntilTrue(() => !servers.exists(hasListenerMetric(_, listenerName)),
      "Processors not shutdown for removed listener")

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
    val consumer = ConsumerBuilder(groupId)
      .listenerName(securityProtocol.name)
      .securityProtocol(securityProtocol)
      .saslMechanism(mechanism)
      .autoOffsetReset("latest")
      .build()
    verifyProduceConsume(producer, consumer, numRecords = 10, topic)
  }

  private def hasListenerMetric(server: KafkaBroker, listenerName: String): Boolean = {
    server.socketServer.metrics.metrics.keySet.asScala.exists(_.tags.get("listener") == listenerName)
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

  private def serverEndpoints(adminClient: Admin): String = {
    val nodes = adminClient.describeCluster().nodes().get
    nodes.asScala.map { node =>
      s"${node.host}:${node.port}"
    }.mkString(",")
  }

  @nowarn("cat=deprecation")
  private def alterAdvertisedListener(adminClient: Admin, externalAdminClient: Admin, oldHost: String, newHost: String): Unit = {
    val configs = servers.map { server =>
      val resource = new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
      val newListeners = server.config.effectiveAdvertisedListeners.map { e =>
        if (e.listenerName.value == SecureExternal)
          s"${e.listenerName.value}://$newHost:${server.boundPort(e.listenerName)}"
        else
          s"${e.listenerName.value}://${e.host}:${server.boundPort(e.listenerName)}"
      }.mkString(",")
      val configEntry = new ConfigEntry(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, newListeners)
      (resource, new Config(Collections.singleton(configEntry)))
    }.toMap.asJava
    adminClient.alterConfigs(configs).all.get
    servers.foreach { server =>
      TestUtils.retry(10000) {
        val externalListener = server.config.effectiveAdvertisedListeners.find(_.listenerName.value == SecureExternal)
          .getOrElse(throw new IllegalStateException("External listener not found"))
        assertEquals(newHost, externalListener.host, "Config not updated")
      }
    }
    val (endpoints, altered) = TestUtils.computeUntilTrue(serverEndpoints(externalAdminClient)) { endpoints =>
      !endpoints.contains(oldHost)
    }
    assertTrue(altered, s"Advertised listener update not propagated by controller: $endpoints")
  }

  @nowarn("cat=deprecation")
  private def alterConfigsOnServer(server: KafkaBroker, props: Properties): Unit = {
    val configEntries = props.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = Map(new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString) -> newConfig).asJava
    adminClients.head.alterConfigs(configs).all.get
    props.asScala.foreach { case (k, v) => waitForConfigOnServer(server, k, v) }
  }

  private def configureMetricsReporters(reporters: Seq[Class[_]], props: Properties,
                                        perBrokerConfig: Boolean = false): Unit = {
    val reporterStr = reporters.map(_.getName).mkString(",")
    props.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, reporterStr)
    reconfigureServers(props, perBrokerConfig, (MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, reporterStr))
  }

  private def verifyConnectionFailure(producer: KafkaProducer[String, String]): Future[_] = {
    val executor = Executors.newSingleThreadExecutor
    executors += executor
    val future = executor.submit(new Runnable() {
      def run(): Unit = {
        producer.send(new ProducerRecord(topic, "key", "value")).get
      }
    })
    verifyTimeout(future)
    future
  }

  private def verifyConnectionFailure(consumer: Consumer[String, String]): Future[_] = {
    val executor = Executors.newSingleThreadExecutor
    executors += executor
    val future = executor.submit(new Runnable() {
      def run(): Unit = {
        consumer.commitSync()
      }
    })
    verifyTimeout(future)
    future
  }

  private def verifyTimeout(future: Future[_]): Unit = {
    assertThrows(classOf[TimeoutException], () => future.get(100, TimeUnit.MILLISECONDS))
  }

  private def addListenerPropsSsl(listenerName: String, props: Properties): Unit = {
    props ++= securityProps(sslProperties1, KEYSTORE_PROPS, listenerPrefix(listenerName))
    props ++= securityProps(sslProperties1, TRUSTSTORE_PROPS, listenerPrefix(listenerName))
  }

  private def addListenerPropsSasl(listener: String, mechanisms: Seq[String], props: Properties): Unit = {
    val listenerName = new ListenerName(listener)
    val prefix = listenerName.configPrefix
    props.put(prefix + KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, mechanisms.mkString(","))
    props.put(prefix + KafkaSecurityConfigs.SASL_KERBEROS_SERVICE_NAME_CONFIG, "kafka")
    mechanisms.foreach { mechanism =>
      val jaasSection = jaasSections(Seq(mechanism), None, KafkaSasl, "").head
      val jaasConfig = jaasSection.modules.head.toString
      props.put(listenerName.saslMechanismConfigPrefix(mechanism) + KafkaSecurityConfigs.SASL_JAAS_CONFIG, jaasConfig)
    }
  }

}
