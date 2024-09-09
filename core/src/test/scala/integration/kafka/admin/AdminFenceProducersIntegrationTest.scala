/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.kafka.admin

import kafka.api.IntegrationTestHarness
import kafka.security.minikdc.MiniKdc.createConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.{InvalidProducerEpochException, ProducerFencedException, TimeoutException}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag, TestInfo, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}
import scala.collection.Seq

@Tag("integration")
class AdminFenceProducersIntegrationTest extends IntegrationTestHarness {
  override def brokerCount = 1

  private val topicName = "mytopic"
  private val txnId = "mytxnid"
  private val record = new ProducerRecord[Array[Byte], Array[Byte]](topicName, null, new Array[Byte](1))

  private var adminClient: Admin = _
  private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    val producerProps = new Properties
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txnId)
    producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "2000")
    producer = createProducer(configOverrides = producerProps)
    adminClient = createAdminClient()
    createTopic(topicName)
  }

  def overridingProps(): Properties = {
    val props = new Properties()
    props.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)
    // Set a smaller value for the number of partitions for speed
    props.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, 1.toString)
    props.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, 1.toString)
    props.put(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, 1.toString)
    props.put(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "2000")
    props
  }

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overridingProps()))
  }

  override protected def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    Seq(overridingProps())
  }

  @AfterEach
  override def tearDown(): Unit = {
    Utils.closeQuietly(adminClient, "AdminFenceProducersIntegrationTest")
    Utils.closeQuietly(producer, "AdminFenceProducersIntegrationTest")
    super.tearDown()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testFenceAfterProducerCommit(quorum: String): Unit = {
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(record).get()
    producer.commitTransaction()

    adminClient.fenceProducers(Collections.singletonList(txnId)).all().get()

    producer.beginTransaction()
    try {
        producer.send(record).get()
        fail("expected ProducerFencedException")
    } catch {
      case _: ProducerFencedException => //ok
      case ee: ExecutionException =>
        assertInstanceOf(classOf[ProducerFencedException], ee.getCause) //ok
      case e: Exception =>
        throw e
    }

    assertThrows(classOf[ProducerFencedException], () => producer.commitTransaction())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  @Timeout(value = 30)
  def testFenceProducerTimeoutMs(quorum: String): Unit = {
    adminClient = {
      val config = createConfig
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${TestUtils.IncorrectBrokerPort}")
      Admin.create(config)
    }
    try {
      val e = assertThrows(classOf[ExecutionException], () => adminClient.fenceProducers(Collections.singletonList(txnId),
        new FenceProducersOptions().timeoutMs(0)).all().get())
      assertInstanceOf(classOf[TimeoutException], e.getCause)
    } finally adminClient.close(Duration.ofSeconds(0))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testFenceBeforeProducerCommit(quorum: String): Unit = {
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(record).get()

    adminClient.fenceProducers(Collections.singletonList(txnId)).all().get()

    try {
      producer.send(record).get()
      fail("expected Exception")
    } catch {
      case ee: ExecutionException =>
        assertTrue(ee.getCause.isInstanceOf[ProducerFencedException] ||
                   ee.getCause.isInstanceOf[InvalidProducerEpochException],
                   "Unexpected ExecutionException cause " + ee.getCause)
      case e: Exception =>
        throw e
    }

    try {
      producer.commitTransaction()
      fail("expected Exception")
    } catch {
      case _: ProducerFencedException =>
      case _: InvalidProducerEpochException =>
      case e: Exception =>
        throw e
    }
  }
}

