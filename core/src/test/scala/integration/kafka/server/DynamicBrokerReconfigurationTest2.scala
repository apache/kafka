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
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.security.KeyStore
import java.time.Duration
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent._
import com.yammer.metrics.core.MetricName
import kafka.api.SaslSetup
import kafka.controller.{ControllerBrokerStateInfo, ControllerChannelManager}
import kafka.metrics.KafkaYammerMetrics
import kafka.network.{Processor, RequestChannel}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.{ConfigEntityChangeNotificationZNode, ZooKeeperTestHarness}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.{AuthenticationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.test.{TestSslUtils, TestUtils => JTestUtils}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import scala.annotation.nowarn
import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.collection.Seq

object DynamicBrokerReconfigurationTest2 {
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"
}

class DynamicBrokerReconfigurationTest2 extends ZooKeeperTestHarness with SaslSetup {

  import DynamicBrokerReconfigurationTest2._

  private val servers = new ArrayBuffer[KafkaServer]
  private val numServers = 1
  private val numPartitions = 10
  private val producers = new ArrayBuffer[KafkaProducer[String, String]]
  private val consumers = new ArrayBuffer[KafkaConsumer[String, String]]
  private val adminClients = new ArrayBuffer[Admin]()
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

  def addExtraProps(props: Properties): Unit = {
  }

  @BeforeEach
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
      props.put(KafkaConfig.PasswordEncoderSecretProp, "dynamic-config-secret")
      props.put(KafkaConfig.LogRetentionTimeMillisProp, 1680000000.toString)
      props.put(KafkaConfig.LogRetentionTimeHoursProp, 168.toString)
      addExtraProps(props)

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

    TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor = numServers, servers)
    TestUtils.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, servers.head.config.offsetsTopicPartitions,
      replicationFactor = numServers, servers, servers.head.groupCoordinator.offsetsTopicConfigs)

    createAdminClient(SecurityProtocol.SSL, SecureInternal)
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

  private def isProcessorMetric(metricName: MetricName): Boolean = {
    val mbeanName = metricName.getMBeanName
    mbeanName.contains(s"${Processor.NetworkProcessorMetricTag}=") || mbeanName.contains(s"${RequestChannel.ProcessorMetricTag}=")
  }

  private def clearLeftOverProcessorMetrics(): Unit = {
    val metricsFromOldTests = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.filter(isProcessorMetric)
    metricsFromOldTests.foreach(KafkaYammerMetrics.defaultRegistry.removeMetric)
  }

  private def awaitInitialPositions(consumer: KafkaConsumer[_, _]): Unit = {
    TestUtils.pollUntilTrue(consumer, () => !consumer.assignment.isEmpty, "Timed out while waiting for assignment")
    consumer.assignment.forEach(consumer.position(_))
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
                                   consumer: KafkaConsumer[String, String],
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
        case e: Error => false
      }
    }, "Did not fail authentication with invalid config")
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

    val combinedStorePath = File.createTempFile("truststore", ".jks").getAbsolutePath
    val password = trustStore1Props.get(SSL_TRUSTSTORE_PASSWORD_CONFIG).asInstanceOf[Password]
    TestSslUtils.createTrustStore(combinedStorePath, password, certs.asJava)
    val newStoreProps = new Properties
    newStoreProps.put(SSL_TRUSTSTORE_LOCATION_CONFIG, combinedStorePath)
    newStoreProps.put(SSL_TRUSTSTORE_PASSWORD_CONFIG, password)
    newStoreProps.put(SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
    newStoreProps
  }

  @nowarn("cat=deprecation")
  private def alterConfigs(servers: Seq[KafkaServer], adminClient: Admin, props: Properties,
                   perBrokerConfig: Boolean): AlterConfigsResult = {
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
    val alterResult = alterConfigs(servers, adminClients.head, newProps, perBrokerConfig)
    if (expectFailure) {
      val oldProps = servers.head.config.values.asScala.filter { case (k, _) => newProps.containsKey(k) }
      val brokerResources = if (perBrokerConfig)
        servers.map(server => new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString))
      else
        Seq(new ConfigResource(ConfigResource.Type.BROKER, ""))
      brokerResources.foreach { brokerResource =>
        val exception = assertThrows(classOf[ExecutionException], () => alterResult.values.get(brokerResource).get)
        assertTrue(exception.getCause.isInstanceOf[InvalidRequestException])
      }
      servers.foreach { server =>
        assertEquals(oldProps, server.config.values.asScala.filter { case (k, _) => newProps.containsKey(k) })
      }
    } else {
      alterResult.all.get
      waitForConfig(aPropToVerify._1, aPropToVerify._2)
    }
  }

  private def listenerPrefix(name: String): String = new ListenerName(name).configPrefix

  private def configureDynamicKeystoreInZooKeeper(kafkaConfig: KafkaConfig, sslProperties: Properties): Unit = {
    val externalListenerPrefix = listenerPrefix(SecureExternal)
    val sslStoreProps = new Properties
    sslStoreProps ++= securityProps(sslProperties, KEYSTORE_PROPS, externalListenerPrefix)
    sslStoreProps.put(KafkaConfig.PasswordEncoderSecretProp, kafkaConfig.passwordEncoderSecret.map(_.value).orNull)
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    val entityType = ConfigType.Broker
    val entityName = kafkaConfig.brokerId.toString

    val passwordConfigs = sslStoreProps.asScala.keySet.filter(DynamicBrokerConfig.isPasswordConfig)
    val passwordEncoder = createPasswordEncoder(kafkaConfig, kafkaConfig.passwordEncoderSecret)

    if (passwordConfigs.nonEmpty) {
      passwordConfigs.foreach { configName =>
        val encodedValue = passwordEncoder.encode(new Password(sslStoreProps.getProperty(configName)))
        sslStoreProps.setProperty(configName, encodedValue)
      }
    }
    sslStoreProps.remove(KafkaConfig.PasswordEncoderSecretProp)
    adminZkClient.changeConfigs(entityType, entityName, sslStoreProps)

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

  private def waitForConfigOnServer(server: KafkaServer, propName: String, propValue: String, maxWaitMs: Long): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
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

  private def configValueAsString(value: Any): String = {
    value match {
      case password: Password => password.value
      case list: util.List[_] => list.asScala.map(_.toString).mkString(",")
      case _ => value.toString
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

      val producer = new KafkaProducer[String, String](producerProps, new StringSerializer, new StringSerializer)
      producers += producer
      producer
    }
  }

  private case class ConsumerBuilder(group: String) extends ClientBuilder[KafkaConsumer[String, String]] {
    private var _autoOffsetReset = "earliest"
    private var _enableAutoCommit = false
    private var _topic = DynamicBrokerReconfigurationTest2.this.topic

    def autoOffsetReset(reset: String): ConsumerBuilder = { _autoOffsetReset = reset; this }
    def enableAutoCommit(enable: Boolean): ConsumerBuilder = { _enableAutoCommit = enable; this }
    def topic(topic: String): ConsumerBuilder = { _topic = topic; this }

    override def build(): KafkaConsumer[String, String] = {
      val consumerProps = propsOverride
      consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, _autoOffsetReset)
      consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group)
      consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, _enableAutoCommit.toString)

      val consumer = new KafkaConsumer[String, String](consumerProps, new StringDeserializer, new StringDeserializer)
      consumers += consumer

      consumer.subscribe(Collections.singleton(_topic))
      if (_autoOffsetReset == "latest")
        awaitInitialPositions(consumer)
      consumer
    }
  }
}

