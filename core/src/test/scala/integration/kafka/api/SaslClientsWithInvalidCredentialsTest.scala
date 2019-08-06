/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.nio.file.Files
import java.time.Duration
import java.util.Collections
import java.util.concurrent.{ExecutionException, TimeUnit}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.junit.{After, Before, Test}
import org.junit.Assert._
import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import kafka.server.KafkaConfig
import kafka.utils.{JaasTestUtils, TestUtils}
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.common.security.auth.SecurityProtocol

class SaslClientsWithInvalidCredentialsTest extends IntegrationTestHarness with SaslSetup {
  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  override protected val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  val consumerCount = 1
  val producerCount = 1
  val brokerCount = 1

  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  this.serverConfig.setProperty(KafkaConfig.TransactionsTopicReplicationFactorProp, "1")
  this.serverConfig.setProperty(KafkaConfig.TransactionsTopicMinISRProp, "1")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val topic = "topic"
  val numPartitions = 1
  val tp = new TopicPartition(topic, 0)

  override def configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    // Create broker credentials before starting brokers
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramAdmin, JaasTestUtils.KafkaScramAdminPassword)
  }

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), Both,
      JaasTestUtils.KafkaServerContextName))
    super.setUp()
    createTopic(topic, numPartitions, brokerCount)
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @Test
  def testProducerWithAuthenticationFailure() {
    val producer = createProducer()
    verifyAuthenticationException(sendOneRecord(producer, maxWaitMs = 10000))
    verifyAuthenticationException(producer.partitionsFor(topic))

    createClientCredential()
    verifyWithRetry(sendOneRecord(producer))
  }

  @Test
  def testTransactionalProducerWithAuthenticationFailure() {
    val txProducer = createTransactionalProducer()
    verifyAuthenticationException(txProducer.initTransactions())

    createClientCredential()
    try {
      txProducer.initTransactions()
      fail("Transaction initialization should fail after authentication failure")
    } catch {
      case _: KafkaException => // expected exception
    }
  }

  @Test
  def testConsumerWithAuthenticationFailure() {
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    verifyConsumerWithAuthenticationFailure(consumer)
  }

  @Test
  def testManualAssignmentConsumerWithAuthenticationFailure() {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    verifyConsumerWithAuthenticationFailure(consumer)
  }

  @Test
  def testManualAssignmentConsumerWithAutoCommitDisabledWithAuthenticationFailure() {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    verifyConsumerWithAuthenticationFailure(consumer)
  }

  private def verifyConsumerWithAuthenticationFailure(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) {
    verifyAuthenticationException(consumer.poll(Duration.ofMillis(1000)))
    verifyAuthenticationException(consumer.partitionsFor(topic))

    createClientCredential()
    val producer = createProducer()
    verifyWithRetry(sendOneRecord(producer))
    verifyWithRetry(assertEquals(1, consumer.poll(Duration.ofMillis(1000)).count))
  }

  @Test
  def testKafkaAdminClientWithAuthenticationFailure() {
    val props = TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val adminClient = AdminClient.create(props)

    def describeTopic(): Unit = {
      try {
        val response = adminClient.describeTopics(Collections.singleton(topic)).all.get
        assertEquals(1, response.size)
        response.asScala.foreach { case (topic, description) =>
          assertEquals(numPartitions, description.partitions.size)
        }
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }

    try {
      verifyAuthenticationException(describeTopic())

      createClientCredential()
      verifyWithRetry(describeTopic())
    } finally {
      adminClient.close()
    }
  }

  @Test
  def testConsumerGroupServiceWithAuthenticationFailure() {
    val consumerGroupService: ConsumerGroupService = prepareConsumerGroupService

    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)

    verifyAuthenticationException(consumerGroupService.listGroups)
    consumerGroupService.close()
  }

  @Test
  def testConsumerGroupServiceWithAuthenticationSuccess() {
    createClientCredential()
    val consumerGroupService: ConsumerGroupService = prepareConsumerGroupService

    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)

    verifyWithRetry(consumer.poll(Duration.ofMillis(1000)))
    assertEquals(1, consumerGroupService.listGroups.size)
    consumerGroupService.close()
  }

  private def prepareConsumerGroupService = {
    val propsFile = TestUtils.tempFile()
    val propsStream = Files.newOutputStream(propsFile.toPath)
    propsStream.write("security.protocol=SASL_PLAINTEXT\n".getBytes())
    propsStream.write(s"sasl.mechanism=$kafkaClientSaslMechanism".getBytes())
    propsStream.close()

    val cgcArgs = Array("--bootstrap-server", brokerList,
                        "--describe",
                        "--group", "test.group",
                        "--command-config", propsFile.getAbsolutePath)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    val consumerGroupService = new ConsumerGroupService(opts)
    consumerGroupService
  }

  private def createClientCredential(): Unit = {
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser2, JaasTestUtils.KafkaScramPassword2)
  }

  private def sendOneRecord(producer: KafkaProducer[Array[Byte], Array[Byte]], maxWaitMs: Long = 15000): Unit = {
    val record = new ProducerRecord(tp.topic(), tp.partition(), 0L, "key".getBytes, "value".getBytes)
    val future = producer.send(record)
    producer.flush()
    try {
      val recordMetadata = future.get(maxWaitMs, TimeUnit.MILLISECONDS)
      assertTrue(s"Invalid offset $recordMetadata", recordMetadata.offset >= 0)
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def verifyAuthenticationException(action: => Unit): Unit = {
    val startMs = System.currentTimeMillis
    try {
      action
      fail("Expected an authentication exception")
    } catch {
      case e : Exception =>
        // expected exception
        val elapsedMs = System.currentTimeMillis - startMs
        assertTrue(s"Poll took too long, elapsed=$elapsedMs", elapsedMs <= 5000)
    }
  }

  private def verifyWithRetry(action: => Unit): Unit = {
    var attempts = 0
    TestUtils.waitUntilTrue(() => {
      try {
        attempts += 1
        action
        true
      } catch {
        case _: SaslAuthenticationException => false
      }
    }, s"Operation did not succeed within timeout after $attempts")
  }

  private def createTransactionalProducer(): KafkaProducer[Array[Byte], Array[Byte]] = {
    producerConfig.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "txclient-1")
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    createProducer()
  }
}
