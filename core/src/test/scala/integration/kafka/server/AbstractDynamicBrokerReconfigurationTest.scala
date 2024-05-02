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

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent._
import com.yammer.metrics.core.MetricName
import kafka.api.SaslSetup
import kafka.network.{Processor, RequestChannel}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.{PasswordEncoder, PasswordEncoderConfigs}
import org.apache.kafka.server.config.{ConfigType, KafkaSecurityConfigs, ReplicationConfigs, ServerLogConfigs, ZkConfigs}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.ShutdownableThread
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.collection.Seq

abstract class AbstractDynamicBrokerReconfigurationTest extends QuorumTestHarness with SaslSetup {
  val Plain = "PLAIN"
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"

  val servers = new ArrayBuffer[KafkaBroker]
  val numServers = 3
  val numPartitions = 10
  val producers = new ArrayBuffer[KafkaProducer[String, String]]
  val consumers = new ArrayBuffer[Consumer[String, String]]
  val adminClients = new ArrayBuffer[Admin]()
  val clientThreads = new ArrayBuffer[ShutdownableThread]()
  val executors = new ArrayBuffer[ExecutorService]
  val topic = "testtopic"

  val kafkaClientSaslMechanism = "PLAIN"
  val kafkaServerSaslMechanisms = List("PLAIN")

  val trustStoreFile1 = TestUtils.tempFile("truststore", ".jks")
  val trustStoreFile2 = TestUtils.tempFile("truststore", ".jks")
  val sslProperties1 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile1), "kafka")
  val sslProperties2 = TestUtils.sslConfigs(Mode.SERVER, clientCert = false, Some(trustStoreFile2), "kafka")
  val invalidSslProperties = invalidSslConfigs

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism)))
    super.setUp(testInfo)

    clearLeftOverProcessorMetrics() // clear metrics left over from other tests so that new ones can be tested

    (0 until numServers).foreach { brokerId =>

      val props = if (isKRaftTest()) {
        val properties = TestUtils.createBrokerConfig(brokerId, null)
        properties.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
        properties
      } else {
        val properties = TestUtils.createBrokerConfig(brokerId, zkConnect)
        properties.put(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, "true")
        properties
      }
      props ++= securityProps(sslProperties1, TRUSTSTORE_PROPS)
      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
      props.put(SocketServerConfigs.LISTENERS_CONFIG, s"$SecureInternal://localhost:0, $SecureExternal://localhost:0")
      props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, s"PLAINTEXT:PLAINTEXT, $SecureInternal:SSL, $SecureExternal:SASL_SSL, CONTROLLER:$controllerListenerSecurityProtocol")
      props.put(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, SecureInternal)
      props.put(KafkaSecurityConfigs.SSL_CLIENT_AUTH_CONFIG, "requested")
      props.put(KafkaSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, "PLAIN")
      props.put(KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, kafkaServerSaslMechanisms.mkString(","))
      props.put(ServerLogConfigs.LOG_SEGMENT_BYTES_CONFIG, "2000") // low value to test log rolling on config update
      props.put(ReplicationConfigs.NUM_REPLICA_FETCHERS_CONFIG, "2") // greater than one to test reducing threads
      props.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, "dynamic-config-secret")
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
      if (!isKRaftTest()) {
        configureDynamicKeystoreInZooKeeper(kafkaConfig, sslProperties1)
      }

      servers += createBroker(kafkaConfig)
    }

    createAdminClient(SecurityProtocol.SSL, SecureInternal)

    TestUtils.createTopicWithAdmin(adminClients.head, topic, servers, controllerServers, numPartitions, replicationFactor = numServers)
    TestUtils.createTopicWithAdmin(adminClients.head, Topic.GROUP_METADATA_TOPIC_NAME, servers, controllerServers,
      numPartitions = servers.head.config.offsetsTopicPartitions,
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

  private def clearLeftOverProcessorMetrics(): Unit = {
    val metricsFromOldTests = KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala.filter(isProcessorMetric)
    metricsFromOldTests.foreach(KafkaYammerMetrics.defaultRegistry.removeMetric)
  }

  def isProcessorMetric(metricName: MetricName): Boolean = {
    val mbeanName = metricName.getMBeanName
    mbeanName.contains(s"${Processor.NetworkProcessorMetricTag}=") || mbeanName.contains(s"${RequestChannel.ProcessorMetricTag}=")
  }

  private def configureDynamicKeystoreInZooKeeper(kafkaConfig: KafkaConfig, sslProperties: Properties): Unit = {
    val externalListenerPrefix = listenerPrefix(SecureExternal)
    val sslStoreProps = new Properties
    sslStoreProps ++= securityProps(sslProperties, KEYSTORE_PROPS, externalListenerPrefix)
    sslStoreProps.put(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG, kafkaConfig.passwordEncoderSecret.map(_.value).orNull)
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)

    val entityType = ConfigType.BROKER
    val entityName = kafkaConfig.brokerId.toString

    val passwordConfigs = sslStoreProps.asScala.keySet.filter(DynamicBrokerConfig.isPasswordConfig)
    val passwordEncoder = createPasswordEncoder(kafkaConfig, kafkaConfig.passwordEncoderSecret)

    if (passwordConfigs.nonEmpty) {
      passwordConfigs.foreach { configName =>
        val encodedValue = passwordEncoder.encode(new Password(sslStoreProps.getProperty(configName)))
        sslStoreProps.setProperty(configName, encodedValue)
      }
    }
    sslStoreProps.remove(PasswordEncoderConfigs.PASSWORD_ENCODER_SECRET_CONFIG)
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

  def createPasswordEncoder(config: KafkaConfig, secret: Option[Password]): PasswordEncoder = {
    val encoderSecret = secret.getOrElse(throw new IllegalStateException("Password encoder secret not configured"))
    PasswordEncoder.encrypting(encoderSecret,
      config.passwordEncoderKeyFactoryAlgorithm,
      config.passwordEncoderCipherAlgorithm,
      config.passwordEncoderKeyLength,
      config.passwordEncoderIterations)
  }

  def createAdminClient(securityProtocol: SecurityProtocol, listenerName: String): Admin = {
    val config = clientProps(securityProtocol)
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(listenerName))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = Admin.create(config)
    adminClients += adminClient
    adminClient
  }

  def fetchBrokerConfigsFromZooKeeper(server: KafkaBroker): Properties = {
    val props = adminZkClient.fetchEntityConfig(ConfigType.BROKER, server.config.brokerId.toString)
    server.config.dynamicConfig.fromPersistentProps(props, perBrokerConfig = true)
  }

  def clientProps(securityProtocol: SecurityProtocol, saslMechanism: Option[String] = None): Properties = {
    val props = new Properties
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    props.put(SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "HTTPS")
    if (securityProtocol == SecurityProtocol.SASL_PLAINTEXT || securityProtocol == SecurityProtocol.SASL_SSL)
      props ++= kafkaClientSaslProperties(saslMechanism.getOrElse(kafkaClientSaslMechanism), dynamicJaasConfig = true)
    props ++= sslProperties1
    securityProps(props, props.keySet)
  }

  def describeConfig(adminClient: Admin, servers: Seq[KafkaBroker] = this.servers): Config = {
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

  def securityProps(srcProps: Properties, propNames: util.Set[_], listenerPrefix: String = ""): Properties = {
    val resultProps = new Properties
    propNames.asScala.filter(srcProps.containsKey).foreach { propName =>
      resultProps.setProperty(s"$listenerPrefix$propName", configValueAsString(srcProps.get(propName)))
    }
    resultProps
  }

  def configEntry(configDesc: Config, configName: String): ConfigEntry = {
    configDesc.entries.asScala.find(cfg => cfg.name == configName)
      .getOrElse(throw new IllegalStateException(s"Config not found $configName"))
  }

  def listenerPrefix(name: String): String = new ListenerName(name).configPrefix

  def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server => waitForConfigOnServer(server, propName, propValue, maxWaitMs) }
  }

  def waitForConfigOnServer(server: KafkaBroker, propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
    }
  }

  def invalidSslConfigs: Properties = {
    val props = new Properties
    props.put(SSL_KEYSTORE_LOCATION_CONFIG, "invalid/file/path")
    props.put(SSL_KEYSTORE_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEY_PASSWORD_CONFIG, new Password("invalid"))
    props.put(SSL_KEYSTORE_TYPE_CONFIG, "PKCS12")
    props
  }

  def currentThreads: List[String] = {
    Thread.getAllStackTraces.keySet.asScala.toList.map(_.getName)
  }

  def matchingThreads(threadPrefix: String): List[String] = {
    currentThreads.filter(_.startsWith(threadPrefix))
  }

  def verifyThreads(threadPrefix: String, countPerBroker: Int, leftOverThreads: Int = 0): Unit = {
    val expectedCount = countPerBroker * servers.size
    val (threads, resized) = TestUtils.computeUntilTrue(matchingThreads(threadPrefix)) { matching =>
      matching.size >= expectedCount &&  matching.size <= expectedCount + leftOverThreads
    }
    assertTrue(resized, s"Invalid threads: expected $expectedCount, got ${threads.size}: $threads")
  }

  private def configValueAsString(value: Any): String = {
    value match {
      case password: Password => password.value
      case list: util.List[_] => list.asScala.map(_.toString).mkString(",")
      case _ => value.toString
    }
  }
}

