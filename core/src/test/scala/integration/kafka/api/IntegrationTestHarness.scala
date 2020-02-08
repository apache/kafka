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

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import kafka.utils.TestUtils
import kafka.utils.Implicits._
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import kafka.server.KafkaConfig
import kafka.integration.KafkaServerTestHarness
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer, Serializer}
import org.junit.{After, Before}

import scala.collection.mutable
import scala.collection.Seq

/**
 * A helper class for writing integration tests that involve producers, consumers, and servers
 */
abstract class IntegrationTestHarness extends KafkaServerTestHarness {
  protected def brokerCount: Int
  protected def logDirCount: Int = 1

  val producerConfig = new Properties
  val consumerConfig = new Properties
  val adminClientConfig = new Properties
  val serverConfig = new Properties

  private val consumers = mutable.Buffer[KafkaConsumer[_, _]]()
  private val producers = mutable.Buffer[KafkaProducer[_, _]]()
  private val adminClients = mutable.Buffer[Admin]()

  protected def interBrokerListenerName: ListenerName = listenerName

  protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(_ ++= serverConfig)
  }

  override def generateConfigs: Seq[KafkaConfig] = {
    val cfgs = TestUtils.createBrokerConfigs(brokerCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, logDirCount = logDirCount)
    configureListeners(cfgs)
    modifyConfigs(cfgs)
    cfgs.map(KafkaConfig.fromProps)
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

  @Before
  override def setUp(): Unit = {
    doSetup(createOffsetsTopic = true)
  }

  def doSetup(createOffsetsTopic: Boolean): Unit = {
    // Generate client security properties before starting the brokers in case certs are needed
    producerConfig ++= clientSecurityProps("producer")
    consumerConfig ++= clientSecurityProps("consumer")
    adminClientConfig ++= clientSecurityProps("adminClient")

    super.setUp()

    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "-1")
    producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)

    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerConfig.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "group")
    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)

    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)

    if (createOffsetsTopic)
      TestUtils.createOffsetsTopic(zkClient, servers)
  }

  def clientSecurityProps(certAlias: String): Properties = {
    TestUtils.securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, certAlias, TestUtils.SslCertificateCn,
      clientSaslProperties)
  }

  def createProducer[K, V](keySerializer: Serializer[K] = new ByteArraySerializer,
                           valueSerializer: Serializer[V] = new ByteArraySerializer,
                           configOverrides: Properties = new Properties): KafkaProducer[K, V] = {
    val props = new Properties
    props ++= producerConfig
    props ++= configOverrides
    val producer = new KafkaProducer[K, V](props, keySerializer, valueSerializer)
    producers += producer
    producer
  }

  def createConsumer[K, V](keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
                           valueDeserializer: Deserializer[V] = new ByteArrayDeserializer,
                           configOverrides: Properties = new Properties,
                           configsToRemove: List[String] = List()): KafkaConsumer[K, V] = {
    val props = new Properties
    props ++= consumerConfig
    props ++= configOverrides
    configsToRemove.foreach(props.remove(_))
    val consumer = new KafkaConsumer[K, V](props, keyDeserializer, valueDeserializer)
    consumers += consumer
    consumer
  }

  def createAdminClient(configOverrides: Properties = new Properties): Admin = {
    val props = new Properties
    props ++= adminClientConfig
    props ++= configOverrides
    val adminClient = Admin.create(props)
    adminClients += adminClient
    adminClient
  }

  @After
  override def tearDown(): Unit = {
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
