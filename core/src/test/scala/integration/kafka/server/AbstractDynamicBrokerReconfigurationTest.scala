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
import java.util.{Collections, Properties}
import java.util.concurrent._
import com.yammer.metrics.core.MetricName
import kafka.admin.ConfigCommand
import kafka.api.{KafkaSasl, SaslSetup}
import kafka.network.{Processor, RequestChannel}
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{ClusterResource, ClusterResourceListener, Reconfigurable, TopicPartition}
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.config.SslConfigs._
import org.apache.kafka.common.config.provider.FileConfigProvider
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.errors.{AuthenticationException, InvalidRequestException}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.metrics.{KafkaMetric, MetricsContext, MetricsReporter}
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.security.{PasswordEncoder, PasswordEncoderConfigs}
import org.apache.kafka.server.config.{ConfigType, KafkaSecurityConfigs, ReplicationConfigs, ServerLogConfigs, ZkConfigs}
import org.apache.kafka.server.metrics.{KafkaYammerMetrics, MetricConfigs}
import org.apache.kafka.server.util.ShutdownableThread
import org.apache.kafka.test.TestSslUtils
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

import java.io.{Closeable, File, IOException, Reader, StringReader}
import java.nio.file.{Files, Path, Paths}
import java.security.KeyStore
import scala.annotation.nowarn
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.collection.{Map, Seq, Set, mutable}

abstract class AbstractDynamicBrokerReconfigurationTest extends QuorumTestHarness with SaslSetup {
  val Plain = "PLAIN"
  val SecureInternal = "INTERNAL"
  val SecureExternal = "EXTERNAL"

  val servers = new ArrayBuffer[KafkaBroker]
  val numServers = 3
  val numPartitions = 10
  private val producers = new ArrayBuffer[KafkaProducer[String, String]]
  private val consumers = new ArrayBuffer[Consumer[String, String]]
  val adminClients = new ArrayBuffer[Admin]()
  private val clientThreads = new ArrayBuffer[ShutdownableThread]()
  private val executors = new ArrayBuffer[ExecutorService]
  val topic = "testtopic"

  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List("PLAIN")

  private val trustStoreFile1 = TestUtils.tempFile("truststore", ".jks")
  private val trustStoreFile2 = TestUtils.tempFile("truststore", ".jks")
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
  def verifyProcessorMetrics(): Unit = {
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
  def verifyMarkPartitionsForTruncation(): Unit = {
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

  def addListener(servers: Seq[KafkaBroker], listenerName: String, securityProtocol: SecurityProtocol,
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

  def verifyAddListener(listenerName: String, securityProtocol: SecurityProtocol,
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

  def verifyRemoveListener(listenerName: String, securityProtocol: SecurityProtocol,
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

  def verifyListener(securityProtocol: SecurityProtocol, saslMechanism: Option[String], groupId: String): Unit = {
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

  def fetchBrokerConfigsFromZooKeeper(server: KafkaBroker): Properties = {
    val props = adminZkClient.fetchEntityConfig(ConfigType.BROKER, server.config.brokerId.toString)
    server.config.dynamicConfig.fromPersistentProps(props, perBrokerConfig = true)
  }

  private def awaitInitialPositions(consumer: Consumer[_, _]): Unit = {
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

  def createAdminClient(securityProtocol: SecurityProtocol, listenerName: String): Admin = {
    val config = clientProps(securityProtocol)
    val bootstrapServers = TestUtils.bootstrapServers(servers, new ListenerName(listenerName))
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(AdminClientConfig.METADATA_MAX_AGE_CONFIG, "10")
    val adminClient = Admin.create(config)
    adminClients += adminClient
    adminClient
  }

  def verifyProduceConsume(producer: KafkaProducer[String, String],
                                   consumer: Consumer[String, String],
                                   numRecords: Int,
                                   topic: String): Unit = {
    val producerRecords = (1 to numRecords).map(i => new ProducerRecord(topic, s"key$i", s"value$i"))
    producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))
    TestUtils.pollUntilAtLeastNumRecords(consumer, numRecords)
  }

  def verifyAuthenticationFailure(producer: KafkaProducer[_, _]): Unit = {
    assertThrows(classOf[AuthenticationException], () => producer.partitionsFor(topic))
  }

  def waitForAuthenticationFailure(producerBuilder: ProducerBuilder): Unit = {
    TestUtils.waitUntilTrue(() => {
      try {
        verifyAuthenticationFailure(producerBuilder.build())
        true
      } catch {
        case _: Error => false
      }
    }, "Did not fail authentication with invalid config")
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

  // Creates a new truststore with certificates from the provided stores and returns the properties of the new store
  def mergeTrustStores(trustStore1Props: Properties, trustStore2Props: Properties): Properties = {

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

  def alterSslKeystore(props: Properties, listener: String, expectFailure: Boolean  = false): Unit = {
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

  private def serverEndpoints(adminClient: Admin): String = {
    val nodes = adminClient.describeCluster().nodes().get
    nodes.asScala.map { node =>
      s"${node.host}:${node.port}"
    }.mkString(",")
  }

  @nowarn("cat=deprecation")
  def alterAdvertisedListener(adminClient: Admin, externalAdminClient: Admin, oldHost: String, newHost: String): Unit = {
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
  def alterConfigsOnServer(server: KafkaBroker, props: Properties): Unit = {
    val configEntries = props.asScala.map { case (k, v) => new ConfigEntry(k, v) }.toList.asJava
    val newConfig = new Config(configEntries)
    val configs = Map(new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString) -> newConfig).asJava
    adminClients.head.alterConfigs(configs).all.get
    props.asScala.foreach { case (k, v) => waitForConfigOnServer(server, k, v) }
  }

  @nowarn("cat=deprecation")
  private def alterConfigs(servers: Seq[KafkaBroker], adminClient: Admin, props: Properties,
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

  def reconfigureServers(newProps: Properties, perBrokerConfig: Boolean, aPropToVerify: (String, String), expectFailure: Boolean = false): Unit = {
    val alterResult = alterConfigs(servers, adminClients.head, newProps, perBrokerConfig)
    if (expectFailure) {
      val oldProps = servers.head.config.values.asScala.filter { case (k, _) => newProps.containsKey(k) }
      val brokerResources = if (perBrokerConfig)
        servers.map(server => new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString))
      else
        Seq(new ConfigResource(ConfigResource.Type.BROKER, ""))
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

  def configEntry(configDesc: Config, configName: String): ConfigEntry = {
    configDesc.entries.asScala.find(cfg => cfg.name == configName)
      .getOrElse(throw new IllegalStateException(s"Config not found $configName"))
  }

  def listenerPrefix(name: String): String = new ListenerName(name).configPrefix

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

  def waitForConfig(propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    servers.foreach { server => waitForConfigOnServer(server, propName, propValue, maxWaitMs) }
  }

  def waitForConfigOnServer(server: KafkaBroker, propName: String, propValue: String, maxWaitMs: Long = 10000): Unit = {
    TestUtils.retry(maxWaitMs) {
      assertEquals(propValue, server.config.originals.get(propName))
    }
  }

  def configureMetricsReporters(reporters: Seq[Class[_]], props: Properties,
                                        perBrokerConfig: Boolean = false): Unit = {
    val reporterStr = reporters.map(_.getName).mkString(",")
    props.put(MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, reporterStr)
    reconfigureServers(props, perBrokerConfig, (MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, reporterStr))
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

  def startProduceConsume(retries: Int, producerClientId: String = "test-producer"): (ProducerThread, ConsumerThread) = {
    val producerThread = new ProducerThread(producerClientId, retries)
    clientThreads += producerThread
    val consumerThread = new ConsumerThread(producerThread)
    clientThreads += consumerThread
    consumerThread.start()
    producerThread.start()
    TestUtils.waitUntilTrue(() => producerThread.sent >= 10, "Messages not sent")
    (producerThread, consumerThread)
  }

  def stopAndVerifyProduceConsume(producerThread: ProducerThread, consumerThread: ConsumerThread,
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
    props.put(prefix + KafkaSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, mechanisms.mkString(","))
    props.put(prefix + KafkaSecurityConfigs.SASL_KERBEROS_SERVICE_NAME_CONFIG, "kafka")
    mechanisms.foreach { mechanism =>
      val jaasSection = jaasSections(Seq(mechanism), None, KafkaSasl, "").head
      val jaasConfig = jaasSection.modules.head.toString
      props.put(listenerName.saslMechanismConfigPrefix(mechanism) + KafkaSecurityConfigs.SASL_JAAS_CONFIG, jaasConfig)
    }
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

  abstract class ClientBuilder[T]() {
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

  case class ProducerBuilder() extends ClientBuilder[KafkaProducer[String, String]] {
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

  case class ConsumerBuilder(group: String) extends ClientBuilder[Consumer[String, String]] {
    private var _autoOffsetReset = "earliest"
    private var _enableAutoCommit = false
    private var _topic = AbstractDynamicBrokerReconfigurationTest.this.topic

    def autoOffsetReset(reset: String): ConsumerBuilder = { _autoOffsetReset = reset; this }
    def enableAutoCommit(enable: Boolean): ConsumerBuilder = { _enableAutoCommit = enable; this }
    def topic(topic: String): ConsumerBuilder = { _topic = topic; this }

    override def build(): Consumer[String, String] = {
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

  class ProducerThread(clientId: String, retries: Int)
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

  class ConsumerThread(producerThread: ProducerThread) extends ShutdownableThread("test-consumer", false) {
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
    assertTrue(total > 0.0, "Invalid metric value")
  }
}

class MockFileConfigProvider extends FileConfigProvider {
  @throws(classOf[IOException])
  override def reader(path: Path): Reader = {
    new StringReader("key=testKey\npassword=ServerPassword\ninterval=1000\nupdinterval=2000\nstoretype=JKS")
  }
}
