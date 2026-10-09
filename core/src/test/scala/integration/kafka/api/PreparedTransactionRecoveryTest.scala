/*
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

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import kafka.utils.Implicits._
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, PreparedTxnState, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.server.config.ServerConfigs
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

class PreparedTransactionRecoveryTest extends IntegrationTestHarness {
  private val topic = "prepared-transaction-recovery"

  serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "1")
  serverConfig.setProperty(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
  serverConfig.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, "1")
  serverConfig.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
  serverConfig.setProperty(TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, "1")
  serverConfig.setProperty(TransactionStateManagerConfig.TRANSACTIONS_2PC_ENABLED_CONFIG, "true")
  serverConfig.setProperty(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
  serverConfig.setProperty(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")

  override protected def brokerCount: Int = 1

  @Test
  def testPreparedTransactionCanBeCommittedAfterProducerRecovery(): Unit = {
    createTopic(topic, numPartitions = 1, replicationFactor = 1)
    val transactionalId = "prepared-recovery-" + UUID.randomUUID()
    val preparedState = prepareTransaction(transactionalId, "committed-value")

    val recoveredProducer = transactionalProducer(transactionalId)
    try {
      recoveredProducer.initTransactions(true)
      recoveredProducer.completeTransaction(new PreparedTxnState(preparedState.toString))
    } finally {
      recoveredProducer.close()
    }

    assertReadCommittedValues(Seq("committed-value"))
  }

  @Test
  def testPreparedTransactionCanBeAbortedAfterProducerRecovery(): Unit = {
    createTopic(topic, numPartitions = 1, replicationFactor = 1)
    val transactionalId = "prepared-recovery-" + UUID.randomUUID()
    val preparedState = prepareTransaction(transactionalId, "aborted-value")

    val recoveredProducer = transactionalProducer(transactionalId)
    try {
      recoveredProducer.initTransactions(true)
      recoveredProducer.completeTransaction(new PreparedTxnState(s"${preparedState.txnOwnerId + 1}:${preparedState.txnOwnerEpoch}"))
    } finally {
      recoveredProducer.close()
    }

    assertReadCommittedValues(Seq.empty)
  }

  private def prepareTransaction(transactionalId: String, value: String): PreparedTxnState = {
    val producer = transactionalProducer(transactionalId)
    try {
      producer.initTransactions()
      producer.beginTransaction()
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, value.getBytes(UTF_8))).get()
      producer.prepareTransaction()
    } finally {
      producer.close()
    }
  }

  private def transactionalProducer(transactionalId: String): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props ++= producerConfig
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    props.put(ProducerConfig.TRANSACTION_TWO_PHASE_COMMIT_ENABLE_CONFIG, "true")
    new KafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer, new ByteArraySerializer)
  }

  private def assertReadCommittedValues(expectedValues: Seq[String]): Unit = {
    val props = new Properties()
    props ++= consumerConfig
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "prepared-recovery-reader-" + UUID.randomUUID())
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](props, new ByteArrayDeserializer, new ByteArrayDeserializer)
    try {
      val topicPartition = new TopicPartition(topic, 0)
      consumer.assign(Collections.singletonList(topicPartition))
      consumer.seekToBeginning(Collections.singletonList(topicPartition))
      val observedValues = ArrayBuffer.empty[String]
      if (expectedValues.isEmpty) {
        observedValues ++= consumer.poll(Duration.ofSeconds(2)).asScala
          .map(record => new String(record.value(), UTF_8))
      } else {
        TestUtils.waitUntilTrue(
          () => {
            observedValues ++= consumer.poll(Duration.ofMillis(100)).asScala
              .map(record => new String(record.value(), UTF_8))
            observedValues.toSeq == expectedValues
          },
          s"Did not read expected committed values $expectedValues"
        )
      }
      assertEquals(expectedValues, observedValues.toSeq)
    } finally {
      consumer.close()
    }
  }
}
