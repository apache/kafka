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

import java.util.{Collections, Objects, Properties}
import java.util.concurrent.TimeUnit
import kafka.api.SaslSetup
import kafka.utils.JaasTestUtils.JaasSection
import kafka.utils.{JaasTestUtils, TestUtils}
import kafka.utils.Implicits._
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.coordinator.group.OffsetConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test, TestInfo}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.Seq

object MultipleListenersWithSameSecurityProtocolBaseTest {
  val SecureInternal = "SECURE_INTERNAL"
  val SecureExternal = "SECURE_EXTERNAL"
  val Internal = "INTERNAL"
  val External = "EXTERNAL"
  val GssApi = "GSSAPI"
  val Plain = "PLAIN"
}

abstract class MultipleListenersWithSameSecurityProtocolBaseTest extends QuorumTestHarness with SaslSetup {

  import MultipleListenersWithSameSecurityProtocolBaseTest._

  private val trustStoreFile = TestUtils.tempFile("truststore", ".jks")
  private val servers = new ArrayBuffer[KafkaServer]
  private val producers = mutable.Map[ClientMetadata, KafkaProducer[Array[Byte], Array[Byte]]]()
  private val consumers = mutable.Map[ClientMetadata, Consumer[Array[Byte], Array[Byte]]]()

  protected val kafkaClientSaslMechanism = Plain
  protected val kafkaServerSaslMechanisms = Map(
    SecureExternal -> Seq("SCRAM-SHA-256", GssApi),
    SecureInternal -> Seq(Plain, "SCRAM-SHA-512"))

  protected def staticJaasSections: Seq[JaasSection]
  protected def dynamicJaasSections: Properties

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(staticJaasSections)
    super.setUp(testInfo)
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
      props.put(s"${new ListenerName(SecureInternal).configPrefix}${KafkaConfig.SaslEnabledMechanismsProp}",
        kafkaServerSaslMechanisms(SecureInternal).mkString(","))
      props.put(s"${new ListenerName(SecureExternal).configPrefix}${KafkaConfig.SaslEnabledMechanismsProp}",
        kafkaServerSaslMechanisms(SecureExternal).mkString(","))
      props.put(KafkaConfig.SaslKerberosServiceNameProp, "kafka")
      props ++= dynamicJaasSections

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
      assertEquals(4, config.listeners.size, s"Unexpected listener count for broker ${config.brokerId}")
      // KAFKA-5184 seems to show that this value can sometimes be PLAINTEXT, so verify it here
      assertEquals(Internal, config.interBrokerListenerName.value,
        s"Unexpected ${KafkaConfig.InterBrokerListenerNameProp} for broker ${config.brokerId}")
    }

    TestUtils.createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME, OffsetConfig.DEFAULT_OFFSETS_TOPIC_NUM_PARTITIONS,
      replicationFactor = 2, servers, servers.head.groupCoordinator.groupMetadataTopicConfigs)

    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)

    servers.head.config.listeners.foreach { endPoint =>
      val listenerName = endPoint.listenerName

      val trustStoreFile =
        if (TestUtils.usesSslTransportLayer(endPoint.securityProtocol)) Some(this.trustStoreFile)
        else None

      val bootstrapServers = TestUtils.bootstrapServers(servers, listenerName)

      def addProducerConsumer(listenerName: ListenerName, mechanism: String, saslProps: Option[Properties]): Unit = {

        val topic = s"${listenerName.value}${producers.size}"
        TestUtils.createTopic(zkClient, topic, 2, 2, servers)
        val clientMetadata = ClientMetadata(listenerName, mechanism, topic)

        producers(clientMetadata) = TestUtils.createProducer(bootstrapServers, acks = -1,
          securityProtocol = endPoint.securityProtocol, trustStoreFile = trustStoreFile, saslProperties = saslProps)

        consumers(clientMetadata) = TestUtils.createConsumer(bootstrapServers, groupId = clientMetadata.toString,
          securityProtocol = endPoint.securityProtocol, trustStoreFile = trustStoreFile, saslProperties = saslProps)
      }

      if (TestUtils.usesSaslAuthentication(endPoint.securityProtocol)) {
        kafkaServerSaslMechanisms(endPoint.listenerName.value).foreach { mechanism =>
          addProducerConsumer(listenerName, mechanism, Some(kafkaClientSaslProperties(mechanism, dynamicJaasConfig = true)))
        }
      } else {
        addProducerConsumer(listenerName, "", saslProps = None)
      }
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
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
    producers.foreach { case (clientMetadata, producer) =>
      val producerRecords = (1 to 10).map(i => new ProducerRecord(clientMetadata.topic, s"key$i".getBytes,
        s"value$i".getBytes))
      producerRecords.map(producer.send).map(_.get(10, TimeUnit.SECONDS))

      val consumer = consumers(clientMetadata)
      consumer.subscribe(Collections.singleton(clientMetadata.topic))
      TestUtils.consumeRecords(consumer, producerRecords.size)
    }
  }

  protected def addDynamicJaasSection(props: Properties, listener: String, mechanism: String, jaasSection: JaasSection): Unit = {
    val listenerName = new ListenerName(listener)
    val prefix = listenerName.saslMechanismConfigPrefix(mechanism)
    val jaasConfig = jaasSection.modules.head.toString
    props.put(s"${prefix}${KafkaConfig.SaslJaasConfigProp}", jaasConfig)
  }

  case class ClientMetadata(listenerName: ListenerName, saslMechanism: String, topic: String) {
    override def hashCode: Int = Objects.hash(listenerName, saslMechanism)
    override def equals(obj: Any): Boolean = obj match {
      case other: ClientMetadata => listenerName == other.listenerName && saslMechanism == other.saslMechanism && topic == other.topic
      case _ => false
    }
    override def toString: String = s"${listenerName.value}:$saslMechanism:$topic"
  }
}
