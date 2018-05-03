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

import java.util

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import kafka.utils.ShutdownableThread
import kafka.server.KafkaConfig
import org.junit.Assert._
import org.junit.{Before, Test}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.internals.Topic

/**
 * Integration tests for the new consumer that cover basic usage as well as server failures
 */
abstract class BaseConsumerTest extends IntegrationTestHarness {

  val epsilon = 0.1
  val producerCount = 1
  val consumerCount = 2
  val serverCount = 3

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val part2 = 1
  val tp2 = new TopicPartition(topic, part2)
  val producerClientId = "ConsumerTestProducer"
  val consumerClientId = "ConsumerTestConsumer"

  // configure the servers and clients
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100") // set small enough session timeout
  this.serverConfig.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, "30000")
  this.serverConfig.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")

  @Before
  override def setUp(): Unit = {
    super.setUp()

    // create the test topic with all the brokers as replicas
    createTopic(topic, 2, serverCount)
  }

  @Test
  def testSimpleConsumption(): Unit = {
    val numRecords = 10000
    sendRecords(numRecords)

    assertEquals(0, this.consumers.head.assignment.size)
    this.consumers.head.assign(List(tp).asJava)
    assertEquals(1, this.consumers.head.assignment.size)

    this.consumers.head.seek(tp, 0)
    consumeAndVerifyRecords(consumer = this.consumers.head, numRecords = numRecords, startingOffset = 0)

    // check async commit callbacks
    val commitCallback = new CountConsumerCommitCallback()
    this.consumers.head.commitAsync(commitCallback)
    awaitCommitCallback(this.consumers.head, commitCallback)
  }

  @Test
  def testCoordinatorFailover(): Unit = {
    val listener = new TestConsumerReassignmentListener()
    this.consumerConfig.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "5000")
    this.consumerConfig.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "2000")
    val consumer0 = new KafkaConsumer(this.consumerConfig, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    consumers += consumer0

    consumer0.subscribe(List(topic).asJava, listener)

    // the initial subscription should cause a callback execution
    consumer0.poll(2000)

    assertEquals(1, listener.callsToAssigned)

    // get metadata for the topic
    var parts: Seq[PartitionInfo] = null
    while (parts == null)
      parts = consumer0.partitionsFor(Topic.GROUP_METADATA_TOPIC_NAME).asScala
    assertEquals(1, parts.size)
    assertNotNull(parts.head.leader())

    // shutdown the coordinator
    val coordinator = parts.head.leader().id()
    this.servers(coordinator).shutdown()

    consumer0.poll(5000)

    // the failover should not cause a rebalance
    assertEquals(1, listener.callsToAssigned)
    assertEquals(1, listener.callsToRevoked)
  }

  protected class TestConsumerReassignmentListener extends ConsumerRebalanceListener {
    var callsToAssigned = 0
    var callsToRevoked = 0

    def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = {
      info("onPartitionsAssigned called.")
      callsToAssigned += 1
    }

    def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = {
      info("onPartitionsRevoked called.")
      callsToRevoked += 1
    }
  }

  protected def sendRecords(numRecords: Int): Seq[ProducerRecord[Array[Byte], Array[Byte]]] =
    sendRecords(numRecords, tp)

  protected def sendRecords(numRecords: Int, tp: TopicPartition): Seq[ProducerRecord[Array[Byte], Array[Byte]]] =
    sendRecords(this.producers.head, numRecords, tp)

  protected def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
                            tp: TopicPartition): Seq[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val records = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
      record
    }
    producer.flush()

    records
  }

  protected def consumeAndVerifyRecords(consumer: Consumer[Array[Byte], Array[Byte]],
                                        numRecords: Int,
                                        startingOffset: Int,
                                        startingKeyAndValueIndex: Int = 0,
                                        startingTimestamp: Long = 0L,
                                        timestampType: TimestampType = TimestampType.CREATE_TIME,
                                        tp: TopicPartition = tp,
                                        maxPollRecords: Int = Int.MaxValue): Unit = {
    val records = consumeRecords(consumer, numRecords, maxPollRecords = maxPollRecords)
    val now = System.currentTimeMillis()
    for (i <- 0 until numRecords) {
      val record = records(i)
      val offset = startingOffset + i
      assertEquals(tp.topic, record.topic)
      assertEquals(tp.partition, record.partition)
      if (timestampType == TimestampType.CREATE_TIME) {
        assertEquals(timestampType, record.timestampType)
        val timestamp = startingTimestamp + i
        assertEquals(timestamp.toLong, record.timestamp)
      } else
        assertTrue(s"Got unexpected timestamp ${record.timestamp}. Timestamp should be between [$startingTimestamp, $now}]",
          record.timestamp >= startingTimestamp && record.timestamp <= now)
      assertEquals(offset.toLong, record.offset)
      val keyAndValueIndex = startingKeyAndValueIndex + i
      assertEquals(s"key $keyAndValueIndex", new String(record.key))
      assertEquals(s"value $keyAndValueIndex", new String(record.value))
      // this is true only because K and V are byte arrays
      assertEquals(s"key $keyAndValueIndex".length, record.serializedKeySize)
      assertEquals(s"value $keyAndValueIndex".length, record.serializedValueSize)
    }
  }

  protected def consumeRecords[K, V](consumer: Consumer[K, V],
                                     numRecords: Int,
                                     maxPollRecords: Int = Int.MaxValue): ArrayBuffer[ConsumerRecord[K, V]] = {
    val records = new ArrayBuffer[ConsumerRecord[K, V]]
    val maxIters = numRecords * 300
    var iters = 0
    while (records.size < numRecords) {
      val polledRecords = consumer.poll(50).asScala
      assertTrue(polledRecords.size <= maxPollRecords)
      for (record <- polledRecords)
        records += record
      if (iters > maxIters)
        throw new IllegalStateException("Failed to consume the expected records after " + iters + " iterations.")
      iters += 1
    }
    records
  }

  protected def awaitCommitCallback[K, V](consumer: Consumer[K, V],
                                          commitCallback: CountConsumerCommitCallback,
                                          count: Int = 1): Unit = {
    val started = System.currentTimeMillis()
    while (commitCallback.successCount < count && System.currentTimeMillis() - started < 10000)
      consumer.poll(50)
    assertEquals(count, commitCallback.successCount)
  }

  protected class CountConsumerCommitCallback extends OffsetCommitCallback {
    var successCount = 0
    var failCount = 0

    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      if (exception == null)
        successCount += 1
      else
        failCount += 1
    }
  }

  protected class ConsumerAssignmentPoller(consumer: Consumer[Array[Byte], Array[Byte]],
                                           topicsToSubscribe: List[String]) extends ShutdownableThread("daemon-consumer-assignment", false)
  {
    @volatile private var partitionAssignment: Set[TopicPartition] = Set.empty[TopicPartition]
    private var topicsSubscription = topicsToSubscribe
    @volatile private var subscriptionChanged = false

    val rebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
        partitionAssignment = collection.immutable.Set(consumer.assignment().asScala.toArray: _*)
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {
        partitionAssignment = Set.empty[TopicPartition]
      }
    }
    consumer.subscribe(topicsToSubscribe.asJava, rebalanceListener)

    def consumerAssignment(): Set[TopicPartition] = {
      partitionAssignment
    }

    /**
     * Subscribe consumer to a new set of topics.
     * Since this method most likely be called from a different thread, this function
     * just "schedules" the subscription change, and actual call to consumer.subscribe is done
     * in the doWork() method
     *
     * This method does not allow to change subscription until doWork processes the previous call
     * to this method. This is just to avoid race conditions and enough functionality for testing purposes
     * @param newTopicsToSubscribe
     */
    def subscribe(newTopicsToSubscribe: List[String]): Unit = {
      if (subscriptionChanged) {
        throw new IllegalStateException("Do not call subscribe until the previous subscribe request is processed.")
      }
      topicsSubscription = newTopicsToSubscribe
      subscriptionChanged = true
    }

    def isSubscribeRequestProcessed(): Boolean = {
      !subscriptionChanged
    }

    override def initiateShutdown(): Boolean = {
      val res = super.initiateShutdown()
      consumer.wakeup()
      res
    }

    override def doWork(): Unit = {
      if (subscriptionChanged) {
        consumer.subscribe(topicsSubscription.asJava, rebalanceListener)
        subscriptionChanged = false
      }
      try {
        consumer.poll(50)
      } catch {
        case _: WakeupException => // ignore for shutdown
      }
    }
  }

  /**
   * Check whether partition assignment is valid
   * Assumes partition assignment is valid iff
   * 1. Every consumer got assigned at least one partition
   * 2. Each partition is assigned to only one consumer
   * 3. Every partition is assigned to one of the consumers
   *
   * @param assignments set of consumer assignments; one per each consumer
   * @param partitions set of partitions that consumers subscribed to
   * @return true if partition assignment is valid
   */
  def isPartitionAssignmentValid(assignments: Buffer[Set[TopicPartition]],
                                 partitions: Set[TopicPartition]): Boolean = {
    val allNonEmptyAssignments = assignments forall (assignment => assignment.nonEmpty)
    if (!allNonEmptyAssignments) {
      // at least one consumer got empty assignment
      return false
    }

    // make sure that sum of all partitions to all consumers equals total number of partitions
    val totalPartitionsInAssignments = (0 /: assignments) (_ + _.size)
    if (totalPartitionsInAssignments != partitions.size) {
      // either same partitions got assigned to more than one consumer or some
      // partitions were not assigned
      return false
    }

    // The above checks could miss the case where one or more partitions were assigned to more
    // than one consumer and the same number of partitions were missing from assignments.
    // Make sure that all unique assignments are the same as 'partitions'
    val uniqueAssignedPartitions = (Set[TopicPartition]() /: assignments) (_ ++ _)
    uniqueAssignedPartitions == partitions
  }

}
