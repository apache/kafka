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

package kafka.api

import java.time.Duration
import java.util.Properties

import kafka.integration.MultiClusterKafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer, Serializer}
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import scala.collection.{Seq, mutable}
import scala.collection.mutable.{ArrayBuffer, Buffer}

/**
 * A helper class for writing integration tests that involve producers, consumers, and servers
 */
abstract class MultiClusterIntegrationTestHarness extends MultiClusterKafkaServerTestHarness {
  protected def brokerCountPerCluster: Int
  protected def logDirCount: Int = 1

  val producerConfigs: Buffer[Properties] = new ArrayBuffer(numClusters) // referenced in MultiClusterAbstractConsumerTest
  val consumerConfigs: Buffer[Properties] = new ArrayBuffer(numClusters) // ditto
  val adminClientConfigs: Buffer[Properties] = new ArrayBuffer(numClusters)
  val serverConfig = new Properties // unconditional extra configs for all brokers

  private val consumers = mutable.Buffer[KafkaConsumer[_, _]]()
  private val producers = mutable.Buffer[KafkaProducer[_, _]]()
  private val adminClients = mutable.Buffer[Admin]()

  override def generateConfigsByCluster(clusterIndex: Int): Seq[KafkaConfig] = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCountPerCluster, zkConnect(clusterIndex),
      interBrokerSecurityProtocol = Some(securityProtocol), trustStoreFile = trustStoreFile,
      saslProperties = serverSaslProperties, logDirCount = logDirCount)
    modifyConfigs(cfgs, clusterIndex)
    cfgs.map(KafkaConfig.fromProps)
  }

  protected def modifyConfigs(props: Seq[Properties], clusterIndex: Int): Unit = {
    configureListeners(props)
    props.foreach(_ ++= serverConfig)
  }

  protected def configureListeners(props: Seq[Properties]): Unit = {
    props.foreach { config =>
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, interBrokerListenerName.value)

      val listenerNames = Set(listenerName, interBrokerListenerName)
      val listeners = listenerNames.map(listenerName => s"${listenerName.value}://localhost:${TestUtils.RandomPort}").mkString(",")
      val listenerSecurityMap = listenerNames.map(listenerName => s"${listenerName.value}:${securityProtocol.name}").mkString(",")

      config.setProperty(KafkaConfig.ListenersProp, listeners)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, listenerSecurityMap)
    }
  }

  protected def interBrokerListenerName: ListenerName = listenerName

  @BeforeEach
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = true)
  }

  def doSetup(createOffsetsTopic: Boolean): Unit = {
    super.setUp()

    (0 until numClusters).foreach { i =>
      producerConfigs += new Properties
      consumerConfigs += new Properties
      adminClientConfigs += new Properties

      producerConfigs(i) ++= clientSecurityProps("producer")
      consumerConfigs(i) ++= clientSecurityProps("consumer")
      adminClientConfigs(i) ++= clientSecurityProps("adminClient")

      producerConfigs(i).put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList(i))
      producerConfigs(i).putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1")
      producerConfigs(i).putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerConfigs(i).putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerConfigs(i).putIfAbsent(ProducerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, boolean2Boolean(true))

      consumerConfigs(i).put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList(i))
      consumerConfigs(i).putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      consumerConfigs(i).putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "group")
      consumerConfigs(i).putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerConfigs(i).putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerConfigs(i).putIfAbsent(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, boolean2Boolean(true))

      adminClientConfigs(i).put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList(i))

      if (createOffsetsTopic)
        TestUtils.createOffsetsTopic(zkClient(i), serversByCluster(i))
    }
  }

  def clientSecurityProps(certAlias: String): Properties = {
    TestUtils.securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, certAlias, TestUtils.SslCertificateCn,
      clientSaslProperties)
  }

  // TODO:  currently cluster 0 only
  def createProducer[K, V](keySerializer: Serializer[K] = new ByteArraySerializer,
                           valueSerializer: Serializer[V] = new ByteArraySerializer,
                           configOverrides: Properties = new Properties): KafkaProducer[K, V] = {
    val props = new Properties
    props ++= producerConfigs(0)
    props ++= configOverrides
    val producer = new KafkaProducer[K, V](props, keySerializer, valueSerializer)
    producers += producer
    producer
  }

  // TODO:  currently cluster 0 only
  def createConsumer[K, V](keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
                           valueDeserializer: Deserializer[V] = new ByteArrayDeserializer,
                           configOverrides: Properties = new Properties,
                           configsToRemove: List[String] = List()): KafkaConsumer[K, V] = {
    val props = new Properties
    props ++= consumerConfigs(0)
    props ++= configOverrides
    configsToRemove.foreach(props.remove(_))
    val consumer = new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)
    consumers += consumer
    consumer
  }

  // TODO:  currently cluster 0 only
  def createAdminClient(configOverrides: Properties = new Properties): Admin = {
    val props = new Properties
    props ++= adminClientConfigs(0)
    props ++= configOverrides
    val adminClient = AdminClient.create(props)
    adminClients += adminClient
    adminClient
  }

  @AfterEach
  override def tearDown(): Unit = {
    // TODO:  figure out how want to store and shut down per-cluster clients
    producers.foreach(_.close(Duration.ZERO))
    consumers.foreach(_.wakeup())
    consumers.foreach(_.close(Duration.ZERO))
    adminClients.foreach(_.close(Duration.ZERO))

    producers.clear()
    consumers.clear()
    adminClients.clear()

    super.tearDown()
  }

}
