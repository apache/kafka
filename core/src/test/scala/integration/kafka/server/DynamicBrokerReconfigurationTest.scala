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
import java.nio.file.{Files, StandardCopyOption}
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}

import kafka.api.SaslSetup
import kafka.coordinator.group.OffsetConfig
import kafka.utils.{ShutdownableThread, TestUtils}
import kafka.utils.Implicits._
import kafka.zk.{ConfigEntityChangeNotificationZNode, ZooKeeperTestHarness}
import org.apache.kafka.clients.admin.ConfigEntry.{ConfigSource, ConfigSynonym}
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.{AuthenticationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.Assert._
import org.junit.{After, Before, Test}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object DynamicBrokerReconfigurationTest {
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"
}

class DynamicBrokerReconfigurationTest extends ZooKeeperTestHarness with SaslSetup {

  import DynamicBrokerReconfigurationTest._

  private var servers = new ArrayBuffer[KafkaServer]
  private val numServers = 3
  private val producers = new ArrayBuffer[KafkaProducer[String, String]]
  private val consumers = new ArrayBuffer[KafkaConsumer[String, String]]
  private val adminClients = new ArrayBuffer[AdminClient]()
  private val clientThreads = new ArrayBuffer[ShutdownableThread]()
  private val topic = "testtopic"

  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")
  private val clientSaslProps = kafkaClientSaslProperties(kafkaClientSaslMechanism, dynamicJaasConfig = true)

  private val trustStoreFile1 = File.createTempFile("truststore", ".jks")
  private val trustStoreFile2 = File.createTempFile("truststore", ".jks")
  private val sslProperties1 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile1), "kafka")
  private val sslProperties2 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile2), "kafka")
  private val invalidSslProperties = invalidSslConfigs

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism)))
    super.setUp()

    (0 until numServers).foreach { brokerId =>

      val props = TestUtils.createBrokerConfig(brokerId, zkConnect, trustStoreFile = Some(trustStoreFile1))
      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
      props.put(KafkaConfig.ListenersProp, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
      props.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"$SecureInternal:SSL, $SecureExternal:SASL_SSL")
      props.put(KafkaConfig.InterBrokerListenerNameProp, SecureInternal)
      props.put(KafkaConfig.ZkEnableSecureAclsProp, "true")
      props.put(KafkaConfig.SaslEnabledMechanismsProp, kafkaServerSaslMechanisms.mkString(","))

      props ++= sslProperties1
      addKeystoreWithListenerPrefix(sslProperties1, props, SecureInternal)

      // Set invalid static properties to ensure that dynamic config is used
      props ++= invalidSslProperties
      addKeystoreWithListenerPrefix(invalidSslProperties, props, SecureExternal)

      val kafkaConfig = KafkaConfig.fromProps(props)
      configureDynamicKeystoreInZooKeeper(kafkaConfig, Seq(brokerId), sslProperties1)

      servers += TestUtils.createServer(kafkaConfig)
    }

    TestUtils.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, OffsetConfig.DefaultOffsetsTopicNumPartitions,
      replicationFactor = numServers, servers, servers.head.groupCoordinator.offsetsTopicConfigs)

    TestUtils.createTopic(zkClient, topic, numPartitions = 10, replicationFactor = numServers, servers)
    createAdminClient(SecurityProtocol.SSL, SecureInternal)
  }

  @After
  override def tearDown() {
    clientThreads.foreach(_.interrupt())
    clientThreads.foreach(_.initiateShutdown())
    clientThreads.foreach(_.join(5 * 1000))
    producers.foreach(_.close())
    consumers.foreach(_.close())
    adminClients.foreach(_.close())
    TestUtils.shutdownServers(servers)
    super.tearDown()
    closeSasl()
  }

  @Test
  def testKeystoreUpdate(): Unit = {
    val producer = createProducer(trustStoreFile1, retries = 0)
    val consumer = createConsumer("group1", trustStoreFile1)
    verifyProduceConsume(producer, consumer, 10)

    // Producer with new truststore should fail to connect before keystore update
    val producer2 = createProducer(trustStoreFile2, retries = 0)
    verifyAuthenticationFailure(producer2)

    // Update broker keystore
    configureDynamicKeystoreInZooKeeper(servers.head.config, servers.map(_.config.brokerId), sslProperties2)
    waitForKeystore(sslProperties2)

    // New producer with old truststore should fail to connect
    val producer1 = createProducer(trustStoreFile1, retries = 0)
    verifyAuthenticationFailure(producer1)

    // New producer with new truststore should work
    val producer3 = createProducer(trustStoreFile2, retries = 0)
    verifyProduceConsume(producer3, consumer, 10)

    // Old producer with old truststore should continue to work (with their old connections)
    verifyProduceConsume(producer, consumer, 10)
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
        verifySynonym(configName, synonyms.get(1), isSensitive, listenerPrefix, ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties)
      }
      verifySynonym(configName, synonyms.get(overrideCount), isSensitive, "ssl.", ConfigSource.STATIC_BROKER_CONFIG, invalidSslProperties)
      defaultValue.foreach { value =>
        val defaultProps = new Properties
        defaultProps.setProperty(configName, value)
        verifySynonym(configName, synonyms.get(overrideCount + 1), isSensitive, "ssl.", ConfigSource.DEFAULT_CONFIG, defaultProps)
      }
    }

    def verifySslConfig(prefix: String, expectedProps: Properties, configDesc: Config): Unit = {
      Seq(SSL_KEYSTORE_LOCATION_CONFIG, SSL_KEYSTORE_TYPE_CONFIG, SSL_KEYSTORE_PASSWORD_CONFIG, SSL_KEY_PASSWORD_CONFIG).foreach { configName =>
        val desc = configEntry(configDesc, s"$prefix$configName")
        val isSensitive = configName.contains("password")
        verifyConfig(configName, desc, isSensitive, if (prefix.isEmpty) invalidSslProperties else sslProperties1)
        val defaultValue = if (configName == SSL_KEYSTORE_TYPE_CONFIG) Some("JKS") else None
        verifySynonyms(configName, desc.synonyms, isSensitive, prefix, defaultValue)
      }
    }

    val adminClient = adminClients.head

    val configDesc = describeConfig(adminClient)
    verifySslConfig("listener.name.external.", sslProperties1, configDesc)
    verifySslConfig("", invalidSslProperties, configDesc)
  }

  @Test
  def testKeyStoreAlterUsingAdminClient(): Unit = {
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions = 10, replicationFactor = numServers, servers)

    // Start a producer and consumer that work with the current truststore.
    // This should continue working while changes are made
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)
    TestUtils.waitUntilTrue(() => consumerThread.received >= 10, "Messages not received")

    // Update broker keystore for external listener
    val adminClient = adminClients.head
    alterSslKeystore(adminClient, sslProperties2, SecureExternal)

    // Produce/consume should work with new truststore
    val producer = createProducer(trustStoreFile2, retries = 0)
    val consumer = createConsumer("group1", trustStoreFile2, topic2)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should fail without update
    alterSslKeystore(adminClient, sslProperties2, SecureInternal, expectFailure = true)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should succeed
    val sslPropertiesCopy = sslProperties1.clone().asInstanceOf[Properties]
    val oldFile = new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
    val newFile = File.createTempFile("keystore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    sslPropertiesCopy.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, newFile.getPath)
    alterSslKeystore(adminClient, sslPropertiesCopy, SecureInternal)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify that all messages sent with retries=0 while keystores were being altered were consumed
    stopAndVerifyProduceConsume(producerThread, consumerThread, mayFailRequests = false)
  }

  private def createProducer(trustStore: File, retries: Int,
                             clientId: String = "test-producer"): KafkaProducer[String, String] = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(SecureExternal))
    val propsOverride = new Properties
    propsOverride.put(ProducerConfig.CLIENT_ID_CONFIG, clientId)
    val producer = TestUtils.createNewProducer(
      bootstrapServers,
      acks = -1,
      retries = retries,
      securityProtocol = SecurityProtocol.SASL_SSL,
      trustStoreFile = Some(trustStore),
      saslProperties = Some(clientSaslProps),
      keySerializer = new StringSerializer,
      valueSerializer = new StringSerializer,
      props = Some(propsOverride))
    producers += producer
    producer
  }

  private def createConsumer(groupId: String, trustStore: File, topic: String = topic):KafkaConsumer[String, String] = {
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(SecureExternal))
    val consumer = TestUtils.createNewConsumer(
      bootstrapServers,
      groupId,
      securityProtocol = SecurityProtocol.SASL_SSL,
      trustStoreFile = Some(trustStore),
      saslProperties = Some(clientSaslProps),
      keyDeserializer = new StringDeserializer,
      valueDeserializer = new StringDeserializer)
    consumer.subscribe(Collections.singleton(topic))
    consumers += consumer
    consumer
  }

  private def createAdminClient(securityProtocol: SecurityProtocol, listenerName: String): AdminClient = {
    val config = new util.HashMap[String, Object]
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(listenerName))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, Some(trustStoreFile1), Some(clientSaslProps))
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    val adminClient = AdminClient.create(config)
    adminClients += adminClient
    adminClient
  }

  private def verifyProduceConsume(producer: KafkaProducer[String, String],
                                   consumer: KafkaConsumer[String, String],
                                   numRecords: Int,
                                   topic: String = topic): Unit = {
    val producerRecords = (1 to numRecords).map(i => new ProducerRecord(topic, s"key$i", s"value$i"))
    producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))

    val records = new ArrayBuffer[ConsumerRecord[String, String]]
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50).asScala
      records.size == numRecords
    }, s"Consumed ${records.size} records until timeout instead of the expected $numRecords records")
  }

  private def verifyAuthenticationFailure(producer: KafkaProducer[_, _]): Unit = {
    try {
      producer.partitionsFor(topic)
      fail("Producer connection did not fail with invalid keystore")
    } catch {
      case _:AuthenticationException => // expected exception
    }
  }

  private def describeConfig(adminClient: AdminClient): Config = {
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

  private def alterSslKeystore(adminClient: AdminClient, props: Properties, listener: String, expectFailure: Boolean  = false): Unit = {
    val newProps = new Properties
    val configPrefix = new ListenerName(listener).configPrefix
    val keystoreLocation = props.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)
    newProps.setProperty(s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", keystoreLocation)
    newProps.setProperty(s"$configPrefix$SSL_KEYSTORE_TYPE_CONFIG", props.getProperty(SSL_KEYSTORE_TYPE_CONFIG))
    newProps.setProperty(s"$configPrefix$SSL_KEYSTORE_PASSWORD_CONFIG", props.get(SSL_KEYSTORE_PASSWORD_CONFIG).asInstanceOf[Password].value)
    newProps.setProperty(s"$configPrefix$SSL_KEY_PASSWORD_CONFIG", props.get(SSL_KEY_PASSWORD_CONFIG).asInstanceOf[Password].value)
    reconfigureServers(newProps, perBrokerConfig = true, (s"$configPrefix$SSL_KEYSTORE_LOCATION_CONFIG", keystoreLocation), expectFailure)
  }

  private def alterConfigs(adminClient: AdminClient, props: Properties, perBrokerConfig: Boolean): AlterConfigsResult = {
    val configEntries = props.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = if (perBrokerConfig) {
      servers.map { server =>
        val resource = new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
        (resource, newConfig)
      }.toMap.asJava
    } else {
      Map(new ConfigResource(ConfigResource.Type.BROKER, "") -> newConfig).asJava
    }
    adminClient.alterConfigs(configs)
  }

  private def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String), expectFailure: Boolean = false): Unit = {
    val alterResult = alterConfigs(adminClients.head, newProps, perBrokerConfig)
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
      assertEquals(oldProps, servers.head.config.values.asScala.filterKeys(newProps.containsKey))
    } else {
      alterResult.all.get
      waitForConfig(aPropToVerify._1, aPropToVerify._2)
    }
  }

  private def configEntry(configDesc: Config, configName: String): ConfigEntry = {
    configDesc.entries.asScala.find(cfg => cfg.name == configName)
      .getOrElse(throw new IllegalStateException(s"Config not found $configName"))
  }

  private def addKeystoreWithListenerPrefix(srcProps: Properties, destProps: Properties, listener: String): Unit = {
    val listenerPrefix = new ListenerName(listener).configPrefix
    destProps.put(listenerPrefix + SSL_KEYSTORE_TYPE_CONFIG, srcProps.get(SSL_KEYSTORE_TYPE_CONFIG))
    destProps.put(listenerPrefix + SSL_KEYSTORE_LOCATION_CONFIG, srcProps.get(SSL_KEYSTORE_LOCATION_CONFIG))
    destProps.put(listenerPrefix + SSL_KEYSTORE_PASSWORD_CONFIG, srcProps.get(SSL_KEYSTORE_PASSWORD_CONFIG).asInstanceOf[Password].value)
    destProps.put(listenerPrefix + SSL_KEY_PASSWORD_CONFIG, srcProps.get(SSL_KEY_PASSWORD_CONFIG).asInstanceOf[Password].value)
  }

  private def configureDynamicKeystoreInZooKeeper(kafkaConfig: KafkaConfig, brokers: Seq[Int], sslProperties: Properties): Unit = {
    val keystoreProps = new Properties
    addKeystoreWithListenerPrefix(sslProperties, keystoreProps, SecureExternal)
    kafkaConfig.dynamicConfig.toPersistentProps(keystoreProps, perBrokerConfig = true)
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    adminZkClient.changeBrokerConfig(brokers, keystoreProps)
  }

  private def waitForKeystore(sslProperties: Properties, maxWaitMs: Long = 10000): Unit = {
    waitForConfig(new ListenerName(SecureExternal).configPrefix + SSL_KEYSTORE_LOCATION_CONFIG,
      sslProperties.getProperty(SSL_KEYSTORE_LOCATION_CONFIG), maxWaitMs)

  }

  private def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server =>
      TestUtils.retry(maxWaitMs) {
        assertEquals(propValue, server.config.currentConfig.props.get(propName))
      }
    }
  }

  private def invalidSslConfigs: Properties = {
    val props = new Properties
    props.put(SSL_KEYSTORE_LOCATION_CONFIG, "invalid/file/path")
    props.put(SSL_KEYSTORE_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEY_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
    props
  }

  private def startProduceConsume(retries: Int): (ProducerThread, ConsumerThread) = {
    val producerThread = new ProducerThread(retries)
    clientThreads += producerThread
    val consumerThread = new ConsumerThread(producerThread)
    clientThreads += consumerThread
    consumerThread.start()
    producerThread.start()
    (producerThread, consumerThread)
  }

  private def stopAndVerifyProduceConsume(producerThread: ProducerThread, consumerThread: ConsumerThread,
                                                                                   mayFailRequests: Boolean): Unit = {
    producerThread.shutdown()
    consumerThread.initiateShutdown()
    consumerThread.awaitShutdown()
    if (!mayFailRequests)
      assertEquals(producerThread.sent, consumerThread.received)
    else {
      assertTrue(s"Some messages not received, sent=${producerThread.sent} received=${consumerThread.received}",
        consumerThread.received >= producerThread.sent)
    }
  }

  private class ProducerThread(retries: Int) extends ShutdownableThread("test-producer", isInterruptible = false) {
    private val producer = createProducer(trustStoreFile1, retries)
    @volatile var sent = 0
    override def doWork(): Unit = {
        try {
            while (isRunning.get) {
                sent += 1
                val record = new ProducerRecord(topic, s"key$sent", s"value$sent")
                producer.send(record).get(10, TimeUnit.SECONDS)
              }
          } finally {
            producer.close()
          }
      }
  }

  private class ConsumerThread(producerThread: ProducerThread) extends ShutdownableThread("test-consumer", isInterruptible = false) {
    private val consumer = createConsumer("group1", trustStoreFile1)
    @volatile private var endTimeMs = Long.MaxValue
    var received = 0
    override def doWork(): Unit = {
      try {
        while (isRunning.get || (received < producerThread.sent && System.currentTimeMillis < endTimeMs)) {
          received += consumer.poll(50).count
        }
      } finally {
        consumer.close()
      }
    }
    override def initiateShutdown(): Boolean = {
      endTimeMs = System.currentTimeMillis + 10 * 1000
      super.initiateShutdown()
    }
  }
}
