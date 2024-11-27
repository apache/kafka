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

import kafka.security.JaasTestUtils

import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.{ExecutionException, TimeUnit}
import scala.jdk.CollectionConverters._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.api.Assertions._
import kafka.utils.{TestInfoUtils, TestUtils}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.storage.Formatter
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{MethodSource, ValueSource}

import scala.jdk.javaapi.OptionConverters
import scala.util.Using


class SaslClientsWithInvalidCredentialsTest extends AbstractSaslTest {
  private val kafkaClientSaslMechanism = "SCRAM-SHA-256"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  override protected val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))
  val consumerCount = 1
  val producerCount = 1
  val brokerCount = 1

  this.serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
  this.serverConfig.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
  this.serverConfig.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, "1")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val topic = "topic"
  val numPartitions = 1
  val tp = new TopicPartition(topic, 0)

  override def configureSecurityBeforeServersStart(testInfo: TestInfo): Unit = {
    super.configureSecurityBeforeServersStart(testInfo)
  }

  override def addFormatterSettings(formatter: Formatter): Unit = {
    formatter.setScramArguments(
      List(s"SCRAM-SHA-256=[name=${JaasTestUtils.KAFKA_SCRAM_ADMIN},password=${JaasTestUtils.KAFKA_SCRAM_ADMIN_PASSWORD}]").asJava)
  }

  override def createPrivilegedAdminClient() = {
    createAdminClient(bootstrapServers(), securityProtocol, trustStoreFile, clientSaslProperties,
      kafkaClientSaslMechanism, JaasTestUtils.KAFKA_SCRAM_ADMIN, JaasTestUtils.KAFKA_SCRAM_ADMIN_PASSWORD)
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), Both,
      JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME))
    val superuserLoginContext = jaasAdminLoginModule(kafkaClientSaslMechanism)
    superuserClientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, superuserLoginContext)
    super.setUp(testInfo)
    Using.resource(createPrivilegedAdminClient()) { superuserAdminClient =>
      TestUtils.createTopicWithAdmin(
        superuserAdminClient, topic, brokers, controllerServers, numPartitions
      )
    }
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  @ParameterizedTest(name="{displayName}.quorum=kraft.isIdempotenceEnabled={0}")
  @ValueSource(booleans = Array(true, false))
  def testProducerWithAuthenticationFailure(isIdempotenceEnabled: Boolean): Unit = {
    val prop = new Properties()
    prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, isIdempotenceEnabled.toString)
    val producer = createProducer(configOverrides = prop)

    verifyAuthenticationException(sendOneRecord(producer, maxWaitMs = 10000))
    verifyAuthenticationException(producer.partitionsFor(topic))

    createClientCredential()
    // in idempotence producer, we need to create another producer because the previous one is in FATEL_ERROR state (due to authentication error)
    // If the transaction state in FATAL_ERROR, it'll never transit to other state. check TransactionManager#isTransitionValid for detail
    val producer2 = if (isIdempotenceEnabled)
      createProducer(configOverrides = prop)
    else
      producer
    verifyWithRetry(sendOneRecord(producer2))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testTransactionalProducerWithAuthenticationFailure(quorum: String, groupProtocol: String): Unit = {
    val txProducer = createTransactionalProducer()
    verifyAuthenticationException(txProducer.initTransactions())

    createClientCredential()
    assertThrows(classOf[KafkaException], () => txProducer.initTransactions())
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testConsumerWithAuthenticationFailure(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.subscribe(List(topic).asJava)
    verifyConsumerWithAuthenticationFailure(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testManualAssignmentConsumerWithAuthenticationFailure(quorum: String, groupProtocol: String): Unit = {
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    verifyConsumerWithAuthenticationFailure(consumer)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testManualAssignmentConsumerWithAutoCommitDisabledWithAuthenticationFailure(quorum: String, groupProtocol: String): Unit = {
    this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false.toString)
    val consumer = createConsumer()
    consumer.assign(List(tp).asJava)
    consumer.seek(tp, 0)
    verifyConsumerWithAuthenticationFailure(consumer)
  }

  private def verifyConsumerWithAuthenticationFailure(consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {
    verifyAuthenticationException(consumer.poll(Duration.ofMillis(1000)))
    verifyAuthenticationException(consumer.partitionsFor(topic))

    createClientCredential()
    val producer = createProducer()
    verifyWithRetry(sendOneRecord(producer))
    verifyWithRetry(assertEquals(1, consumer.poll(Duration.ofMillis(1000)).count))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersClassicGroupProtocolOnly"))
  def testKafkaAdminClientWithAuthenticationFailure(quorum: String, groupProtocol: String): Unit = {
    val props = JaasTestUtils.adminClientSecurityConfigs(securityProtocol, OptionConverters.toJava(trustStoreFile), OptionConverters.toJava(clientSaslProperties))
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
    val adminClient = Admin.create(props)

    def describeTopic(): Unit = {
      try {
        val response = adminClient.describeTopics(Collections.singleton(topic)).allTopicNames.get
        assertEquals(1, response.size)
        response.forEach { (_, description) =>
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

  private def createClientCredential(): Unit = {
    createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KAFKA_SCRAM_USER_2, JaasTestUtils.KAFKA_SCRAM_PASSWORD_2)
  }

  private def sendOneRecord(producer: KafkaProducer[Array[Byte], Array[Byte]], maxWaitMs: Long = 15000): Unit = {
    val record = new ProducerRecord(tp.topic(), tp.partition(), 0L, "key".getBytes, "value".getBytes)
    val future = producer.send(record)
    producer.flush()
    try {
      val recordMetadata = future.get(maxWaitMs, TimeUnit.MILLISECONDS)
      assertTrue(recordMetadata.offset >= 0, s"Invalid offset $recordMetadata")
    } catch {
      case e: ExecutionException => throw e.getCause
    }
  }

  private def verifyAuthenticationException(action: => Unit): Unit = {
    val startMs = System.currentTimeMillis
    assertThrows(classOf[Exception], () => action)
    val elapsedMs = System.currentTimeMillis - startMs
    assertTrue(elapsedMs <= 5000, s"Poll took too long, elapsed=$elapsedMs")
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
