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

package kafka.admin

import kafka.server.{AbstractDynamicBrokerReconfigurationTest, TestMetricsReporter}
import kafka.utils.TestUtils
import kafka.utils.Implicits._
import org.apache.kafka.clients.admin.{Config, ConfigEntry}
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.SslConfigs.{SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, SSL_KEYSTORE_KEY_CONFIG, SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_TYPE_CONFIG}
import org.apache.kafka.common.network.CertStores.KEYSTORE_PROPS
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.coordinator.transaction.TransactionLogConfigs
import org.apache.kafka.server.config.ServerLogConfigs
import org.apache.kafka.server.metrics.MetricConfigs
import org.apache.kafka.storage.internals.log.CleanerConfig
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNull, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util
import java.util.Properties
import scala.jdk.CollectionConverters._

class DynamicBrokerReconfigurationTest extends AbstractDynamicBrokerReconfigurationTest {
  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testConfigDescribeUsingAdminClient(quorum: String): Unit = {
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

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testUpdatesUsingConfigProvider(quorum: String): Unit = {
    val PollingIntervalVal = f"$${file:polling.interval:interval}"
    val PollingIntervalUpdateVal = f"$${file:polling.interval:updinterval}"
    val SslTruststoreTypeVal = f"$${file:ssl.truststore.type:storetype}"
    val SslKeystorePasswordVal = f"$${file:ssl.keystore.password:password}"

    val configPrefix = listenerPrefix(SecureExternal)
    val brokerConfigs = describeConfig(adminClients.head, servers).entries.asScala
    // the following are values before updated
    assertFalse(brokerConfigs.exists(_.name == TestMetricsReporter.PollingIntervalProp), "Initial value of polling interval")
    assertFalse(brokerConfigs.exists(_.name == configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG), "Initial value of ssl truststore type")
    assertNull(brokerConfigs.find(_.name == configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).get.value, "Initial value of ssl keystore password")

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
    updatedProps.put(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, SslTruststoreTypeVal)

    // merge two properties
    updatedProps ++= secProps

    // 3. update password property using config provider
    updatedProps.put(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SslKeystorePasswordVal)

    alterConfigsUsingConfigCommand(updatedProps)
    waitForConfig(TestMetricsReporter.PollingIntervalProp, "1000")
    waitForConfig(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
    waitForConfig(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "ServerPassword")

    // wait for MetricsReporter
    val reporters = TestMetricsReporter.waitForReporters(servers.size)
    reporters.foreach { reporter =>
      reporter.verifyState(reconfigureCount = 0, deleteCount = 0, pollingInterval = 1000)
      assertFalse(reporter.kafkaMetrics.isEmpty, "No metrics found")
    }

    if (!isKRaftTest()) {
      // fetch from ZK, values should be unresolved
      val props = fetchBrokerConfigsFromZooKeeper(servers.head)
      assertTrue(props.getProperty(TestMetricsReporter.PollingIntervalProp) == PollingIntervalVal, "polling interval is not updated in ZK")
      assertTrue(props.getProperty(configPrefix + SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG) == SslTruststoreTypeVal, "store type is not updated in ZK")
      assertTrue(props.getProperty(configPrefix + SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG) == SslKeystorePasswordVal, "keystore password is not updated in ZK")
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

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testKeyStoreAlter(quorum: String): Unit = {
    val topic2 = "testtopic2"
    TestUtils.createTopicWithAdmin(adminClients.head, topic2, servers, controllerServers, numPartitions, replicationFactor = numServers)

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
    // Start the new consumer in a separate group than the continuous consumer started at the beginning of the test so
    // that it is not disrupted by rebalance.
    val consumer = ConsumerBuilder("group2").trustStoreProps(sslProperties2).topic(topic2).build()
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

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testTransactionVerificationEnable(quorum: String): Unit = {
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
    updatedProps.put(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "false")
    alterConfigsUsingConfigCommand(updatedProps)
    verifyConfiguration(false)

    // Ensure it remains off after shutdown.
    val shutdownServer = servers.head
    shutdownServer.shutdown()
    shutdownServer.awaitShutdown()
    shutdownServer.startup()
    verifyConfiguration(false)

    // Turn verification back on.
    updatedProps.put(TransactionLogConfigs.TRANSACTION_PARTITION_VERIFICATION_ENABLE_CONFIG, "true")
    alterConfigsUsingConfigCommand(updatedProps)
    verifyConfiguration(true)
  }

  private def configEntry(configDesc: Config, configName: String): ConfigEntry = {
    configDesc.entries.asScala.find(cfg => cfg.name == configName)
      .getOrElse(throw new IllegalStateException(s"Config not found $configName"))
  }

  private def alterSslKeystore(props: Properties, listener: String, expectFailure: Boolean  = false): Unit = {
    val configPrefix = listenerPrefix(listener)
    val newProps = securityProps(props, KEYSTORE_PROPS, configPrefix)
    reconfigureServers(newProps, perBrokerConfig = true,
      (s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)), expectFailure)
  }

  def alterSslKeystoreUsingConfigCommand(props: Properties, listener: String): Unit = {
    val configPrefix = listenerPrefix(listener)
    val newProps = securityProps(props, KEYSTORE_PROPS, configPrefix)
    alterConfigsUsingConfigCommand(newProps)
    waitForConfig(s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
  }

  def alterConfigsUsingConfigCommand(props: Properties): Unit = {
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
}
