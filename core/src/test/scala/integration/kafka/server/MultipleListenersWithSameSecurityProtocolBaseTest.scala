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
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit

import kafka.api.SaslSetup
import kafka.coordinator.group.OffsetConfig
import kafka.utils.JaasTestUtils.JaasSection
import kafka.utils.TestUtils
import kafka.utils.Implicits._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object MultipleListenersWithSameSecurityProtocolBaseTest {
  val SecureInternal = "SECURE_INTERNAL"
  val SecureExternal = "SECURE_EXTERNAL"
  val Internal = "INTERNAL"
  val External = "EXTERNAL"
  val GssApi = "GSSAPI"
  val Plain = "PLAIN"
}

abstract class MultipleListenersWithSameSecurityProtocolBaseTest extends ZooKeeperTestHarness with SaslSetup {

  import MultipleListenersWithSameSecurityProtocolBaseTest._

  private val trustStoreFile = File.createTempFile("truststore", ".jks")
  private val servers = new ArrayBuffer[KafkaServer]
  private val producers = mutable.Map[ListenerName, KafkaProducer[Array[Byte], Array[Byte]]]()
  private val consumers = mutable.Map[ListenerName, KafkaConsumer[Array[Byte], Array[Byte]]]()

  protected val kafkaClientSaslMechanism = Plain
  protected val kafkaServerSaslMechanisms = List(GssApi, Plain)

  protected def saslProperties(listenerName: ListenerName): Properties
  protected def jaasSections: Seq[JaasSection]

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections)
    super.setUp()
    // 2 brokers so that we can test that the data propagates correctly via UpdateMetadadaRequest
    val numServers = 2

    (0 until numServers).foreach { brokerId =>

      val props = TestUtils.createBrokerConfig(brokerId, zkConnect, trustStoreFile = Some(trustStoreFile))
      // Ensure that we can support multiple listeners per security protocol and multiple security protocols
      props.put(KafkaConfig.ListenersProp, s"$SecureInternal://localhost:0, $Internal://localhost:0, " +
        s"$SecureExternal://localhost:0, $External://localhost:0")
      props.put(KafkaConfig.ListenerSecurityProtocolMapProp, s"$Internal:PLAINTEXT, $SecureInternal:SASL_SSL," +
        s"$External:PLAINTEXT, $SecureExternal:SASL_SSL")
      props.put(KafkaConfig.InterBrokerListenerNameProp, Internal)
      props.put(KafkaConfig.ZkEnableSecureAclsProp, "true")
      props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, kafkaClientSaslMechanism)
      props.put(KafkaConfig.SaslEnabledMechanismsProp, kafkaServerSaslMechanisms.mkString(","))
      props.put(KafkaConfig.SaslKerberosServiceNameProp, "kafka")

      props ++= TestUtils.sslConfigs(Mode.SERVER, false, Some(trustStoreFile), s"server$brokerId")

      // set listener-specific configs and set an invalid path for the global config to verify that the overrides work
      Seq(SecureInternal, SecureExternal).foreach { listenerName =>
        props.put(new ListenerName(listenerName).configPrefix + SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,
          props.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
      }
      props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "invalid/file/path")

      servers += TestUtils.createServer(KafkaConfig.fromProps(props))
    }

    servers.map(_.config).foreach { config =>
      assertEquals(s"Unexpected listener count for broker ${config.brokerId}", 4, config.listeners.size)
      // KAFKA-5184 seems to show that this value can sometimes be PLAINTEXT, so verify it here
      assertEquals(s"Unexpected ${KafkaConfig.InterBrokerListenerNameProp} for broker ${config.brokerId}",
        Internal, config.interBrokerListenerName.value)
    }

    TestUtils.createTopic(zkUtils, Topic.GROUP_METADATA_TOPIC_NAME, OffsetConfig.DefaultOffsetsTopicNumPartitions,
      replicationFactor = 2, servers, servers.head.groupCoordinator.offsetsTopicConfigs)

    servers.head.config.listeners.foreach { endPoint =>
      val listenerName = endPoint.listenerName

      TestUtils.createTopic(zkUtils, listenerName.value, 2, 2, servers)

      val trustStoreFile =
        if (TestUtils.usesSslTransportLayer(endPoint.securityProtocol)) Some(this.trustStoreFile)
        else None

      val saslProps =
        if (TestUtils.usesSaslAuthentication(endPoint.securityProtocol)) Some(saslProperties(listenerName))
        else None

      val bootstrapServers = TestUtils.bootstrapServers(servers, listenerName)

      producers(listenerName) = TestUtils.createNewProducer(bootstrapServers, acks = -1,
        securityProtocol = endPoint.securityProtocol, trustStoreFile = trustStoreFile, saslProperties = saslProps)

      consumers(listenerName) = TestUtils.createNewConsumer(bootstrapServers, groupId = listenerName.value,
        securityProtocol = endPoint.securityProtocol, trustStoreFile = trustStoreFile, saslProperties = saslProps)
    }
  }

  @After
  override def tearDown() {
    producers.values.foreach(_.close())
    consumers.values.foreach(_.close())
    TestUtils.shutdownServers(servers)
    super.tearDown()
    closeSasl()
  }

  /**
    * Tests that we can produce and consume to/from all broker-defined listeners and security protocols. We produce
    * with acks=-1 to ensure that replication is also working.
    */
  @Test
  def testProduceConsume(): Unit = {
    producers.foreach { case (listenerName, producer) =>
      val producerRecords = (1 to 10).map(i => new ProducerRecord(listenerName.value, s"key$i".getBytes,
        s"value$i".getBytes))
      producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))

      val consumer = consumers(listenerName)
      consumer.subscribe(Collections.singleton(listenerName.value))
      val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
      TestUtils.waitUntilTrue(() => {
        records ++= consumer.poll(50).asScala
        records.size == producerRecords.size
      }, s"Consumed ${records.size} records until timeout instead of the expected ${producerRecords.size} records")
    }
  }
}
