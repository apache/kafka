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
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.coordinator.transaction.{TransactionLogConfigs, TransactionStateManagerConfigs}
import org.apache.kafka.server.config.ServerLogConfigs
import org.junit.jupiter.api.Assertions.{assertInstanceOf, assertThrows, fail}
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.concurrent.ExecutionException
import java.util.{Collections, Properties}
import scala.collection.Seq

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
    producerProps.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 200)
    producer = createProducer(configOverrides = producerProps)
    adminClient = createAdminClient()
    createTopic(topicName)
  }

  def overridingProps(): Properties = {
    val props = new Properties()
    props.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)
    // Set a smaller value for the number of partitions for
    props.put(TransactionLogConfigs.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, 1.toString)
    props.put(TransactionLogConfigs.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, 1.toString)
    props.put(TransactionLogConfigs.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, 1.toString)
    props.put(TransactionStateManagerConfigs.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "200")
    props
  }

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overridingProps()))
  }

  override protected def kraftControllerConfigs(): Seq[Properties] = {
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
  def testFenceProducerAfterCommit(quorum: String): Unit = {
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(record).get()
    producer.commitTransaction()

    adminClient.fenceProducers(Collections.singletonList(txnId)).all().get()

    var retry : Int = 0
    try {
      while(retry < 100) {
        producer.beginTransaction()
        producer.send(record).get()
        producer.commitTransaction()
        retry += 1
      }
      fail("expected ProducerFencedException")
    } catch {
      case _: ProducerFencedException => //ok
      case ee: ExecutionException =>
        assertInstanceOf(classOf[ProducerFencedException], ee.getCause) //ok
        println("ProducerFencedException on retry " + retry)
      case e: Exception =>
        throw e
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testFenceProducerWhenTxnInProgress(quorum: String): Unit = {
    producer.initTransactions()
    producer.beginTransaction()
    producer.send(record).get()
    adminClient.fenceProducers(Collections.singletonList(txnId)).all().get()

    assertThrows(classOf[ProducerFencedException], () => producer.commitTransaction())
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft","zk"))
  def testInitAfterFencing(quorum: String): Unit = {
    adminClient.fenceProducers(Collections.singletonList(txnId)).all().get()

    producer.initTransactions()
    producer.beginTransaction()
    producer.send(record).get()
    producer.commitTransaction();

    adminClient.fenceProducers(Collections.singletonList(txnId)).all().get()

    try {
      for (retry <- 0 to 100) {
        try {
          producer.beginTransaction()
          val record2 = new ProducerRecord[Array[Byte], Array[Byte]](topicName, null, "2".getBytes)
          producer.send(record2).get()
          producer.commitTransaction();
        } catch {
          case pfe: ProducerFencedException => {
            println("OK PFE on retry " + retry)
            throw pfe
          }
        }
      }
      fail("expected ProducerFencedException")
    } catch {
      case _: ProducerFencedException => //ok
      case ee: ExecutionException =>
        assertInstanceOf(classOf[ProducerFencedException], ee.getCause) //ok
      case e: Exception =>
        throw e
    }
  }
}

