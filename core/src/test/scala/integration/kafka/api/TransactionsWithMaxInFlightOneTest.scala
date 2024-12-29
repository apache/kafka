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

import java.util.Properties
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{TestInfoUtils, TestUtils}
import kafka.utils.TestUtils.consumeRecords
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.server.config.{ReplicationConfigs, ServerConfigs, ServerLogConfigs}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

/**
 * This is used to test transactions with one broker and `max.in.flight.requests.per.connection=1`.
 * A single broker is used to verify edge cases where different requests are queued on the same connection.
 */
class TransactionsWithMaxInFlightOneTest extends KafkaServerTestHarness {
  val numBrokers = 1

  val topic1 = "topic1"
  val topic2 = "topic2"
  val numPartitions = 4

  val transactionalProducers = mutable.Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()
  val transactionalConsumers = mutable.Buffer[Consumer[Array[Byte], Array[Byte]]]()

  override def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(numBrokers, null).map(KafkaConfig.fromProps(_, serverProps()))
  }

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)
    val topicConfig = new Properties()
    topicConfig.put(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, 1.toString)
    createTopic(topic1, numPartitions, numBrokers, topicConfig)
    createTopic(topic2, numPartitions, numBrokers, topicConfig)

    createTransactionalProducer("transactional-producer")
    createReadCommittedConsumer("transactional-group")
  }

  @AfterEach
  override def tearDown(): Unit = {
    transactionalProducers.foreach(_.close())
    transactionalConsumers.foreach(_.close())
    super.tearDown()
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumAndGroupProtocolNames)
  @MethodSource(Array("getTestQuorumAndGroupProtocolParametersAll"))
  def testTransactionalProducerSingleBrokerMaxInFlightOne(quorum: String, groupProtocol: String): Unit = {
    // We want to test with one broker to verify multiple requests queued on a connection
    assertEquals(1, brokers.size)

    val producer = transactionalProducers.head
    val consumer = transactionalConsumers.head

    producer.initTransactions()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "2", "2", willBeCommitted = false))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "4", "4", willBeCommitted = false))
    producer.flush()
    producer.abortTransaction()

    producer.beginTransaction()
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic1, null, "1", "1", willBeCommitted = true))
    producer.send(TestUtils.producerRecordWithExpectedTransactionStatus(topic2, null, "3", "3", willBeCommitted = true))
    producer.commitTransaction()

    consumer.subscribe(List(topic1, topic2).asJava)

    val records = consumeRecords(consumer, 2)
    records.foreach { record =>
      TestUtils.assertCommittedAndGetValue(record)
    }
  }

  private def serverProps() = {
    val serverProps = new Properties()
    serverProps.put(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, false.toString)
    serverProps.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, 1.toString)
    serverProps.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, 1.toString)
    serverProps.put(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, 1.toString)
    serverProps.put(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, 1.toString)
    serverProps.put(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, 1.toString)
    serverProps.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, true.toString)
    serverProps.put(ReplicationConfigs.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, false.toString)
    serverProps.put(ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG, false.toString)
    serverProps.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0")
    serverProps.put(TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, "200")
    serverProps
  }

  private def createReadCommittedConsumer(group: String) = {
    val consumer = TestUtils.createConsumer(bootstrapServers(),
      groupProtocolFromTestParameters(),
      groupId = group,
      enableAutoCommit = false,
      readCommitted = true)
    transactionalConsumers += consumer
    consumer
  }

  private def createTransactionalProducer(transactionalId: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val producer = TestUtils.createTransactionalProducer(transactionalId, brokers, maxInFlight = 1)
    transactionalProducers += producer
    producer
  }
}
