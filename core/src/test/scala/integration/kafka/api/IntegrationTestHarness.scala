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

import org.apache.kafka.clients.consumer.{KafkaConsumer, RangeAssignor}
import kafka.utils.TestUtils
import kafka.utils.Implicits._
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.KafkaProducer
import kafka.server.KafkaConfig
import kafka.integration.KafkaServerTestHarness
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer, Serializer}
import org.junit.{After, Before}

import scala.collection.mutable

/**
 * A helper class for writing integration tests that involve producers, consumers, and servers
 */
abstract class IntegrationTestHarness extends KafkaServerTestHarness {

  val serverCount: Int
  var logDirCount: Int = 1
  val producerConfig = new Properties
  val consumerConfig = new Properties
  val serverConfig = new Properties

  private val consumers = mutable.Buffer[KafkaConsumer[_, _]]()
  private val producers = mutable.Buffer[KafkaProducer[_, _]]()

  protected def interBrokerListenerName: ListenerName = listenerName

  override def generateConfigs = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile, saslProperties = serverSaslProperties, logDirCount = logDirCount)
    cfgs.foreach { config =>
      config.remove(KafkaConfig.InterBrokerSecurityProtocolProp)
      config.setProperty(KafkaConfig.InterBrokerListenerNameProp, interBrokerListenerName.value)

      val listenerNames = Set(listenerName, interBrokerListenerName)
      val listeners = listenerNames.map(listenerName => s"${listenerName.value}://localhost:${TestUtils.RandomPort}").mkString(",")
      val listenerSecurityMap = listenerNames.map(listenerName => s"${listenerName.value}:${securityProtocol.name}").mkString(",")

      config.setProperty(KafkaConfig.ListenersProp, listeners)
      config.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, listenerSecurityMap)
    }
    cfgs.foreach(_ ++= serverConfig)
    cfgs.map(KafkaConfig.fromProps)
  }

  @Before
  override def setUp() {
    // Client certs if needed, are generated before the brokers startup
    producerConfig.putAll(clientSecurityProps("producer"))
    consumerConfig.putAll(clientSecurityProps("consumer"))

    super.setUp()

    TestUtils.createOffsetsTopic(zkClient, servers)
  }

  def clientSecurityProps(certAlias: String): Properties = {
    TestUtils.securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, certAlias, TestUtils.SslCertificateCn,
      clientSaslProperties)
  }

  def createProducer[K, V](acks: Int = -1,
                           maxBlockMs: Long = 60 * 1000L,
                           bufferSize: Long = 1024L * 1024L,
                           retries: Int = Int.MaxValue,
                           deliveryTimeoutMs: Int = 30 * 1000,
                           lingerMs: Int = 0,
                           requestTimeoutMs: Int = 20 * 1000,
                           keySerializer: Serializer[K] = new ByteArraySerializer,
                           valueSerializer: Serializer[V] = new ByteArraySerializer,
                           configOverrides: Properties = new Properties()): KafkaProducer[K, V] = {
    val props = new Properties()
    props.putAll(producerConfig)
    props.putAll(configOverrides)

    val producer = TestUtils.createProducer(brokerList,
      acks = acks,
      maxBlockMs = maxBlockMs,
      bufferSize = bufferSize,
      retries = retries,
      deliveryTimeoutMs = deliveryTimeoutMs,
      lingerMs = lingerMs,
      requestTimeoutMs = requestTimeoutMs,
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      overrides = Some(props))
    producers += producer
    producer
  }

  def createConsumer[K, V](groupId: String = "group",
                           autoOffsetReset: String = "earliest",
                           partitionFetchSize: Long = 4096L,
                           partitionAssignmentStrategy: String = classOf[RangeAssignor].getName,
                           sessionTimeout: Int = 30000,
                           keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
                           valueDeserializer: Deserializer[V] = new ByteArrayDeserializer,
                           configOverrides: Properties = new Properties()): KafkaConsumer[K, V] = {
    val props = new Properties()
    props.putAll(consumerConfig)
    props.putAll(configOverrides)

    val consumer = TestUtils.createConsumer(brokerList,
      groupId = groupId,
      autoOffsetReset = autoOffsetReset,
      partitionFetchSize = partitionFetchSize,
      partitionAssignmentStrategy = partitionAssignmentStrategy,
      sessionTimeout = sessionTimeout,
      keyDeserializer = keyDeserializer,
      valueDeserializer = valueDeserializer,
      overrides = Some(props))
    consumers += consumer
    consumer
  }

  @After
  override def tearDown() {
    producers.foreach(_.close(0, TimeUnit.MILLISECONDS))
    consumers.foreach(_.wakeup())
    consumers.foreach(_.close(Duration.ZERO))
    producers.clear()
    consumers.clear()
    super.tearDown()
  }

}
