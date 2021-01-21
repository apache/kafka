/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.TopicPartition
import kafka.utils.{ShutdownableThread, TestUtils}
import kafka.server.{BaseRequestTest, KafkaConfig}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.BeforeEach

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ArrayBuffer, Buffer}
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.errors.WakeupException

import scala.collection.mutable

/**
 * Extension point for consumer integration tests.
 */
abstract class AbstractConsumerTest extends BaseRequestTest {

  val epsilon = 0.1
  override def brokerCount: Int = 3

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val part2 = 1
  val tp2 = new TopicPartition(topic, part2)
  val group = "my-test"
  val producerClientId = "ConsumerTestProducer"
  val consumerClientId = "ConsumerTestConsumer"
  val groupMaxSessionTimeoutMs = 30000L

  this.producerConfig.setProperty(ProducerConfig.ACKS_CONFIG, "all")
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")
  this.consumerConfig.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "6000")


  override protected def brokerPropertyOverrides(properties: Properties): Unit = {
    properties.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
    properties.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "3") // don't want to lose offset
    properties.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
    properties.setProperty(KafkaConfig.GroupMinSessionTimeoutMsProp, "100") // set small enough session timeout
    properties.setProperty(KafkaConfig.GroupMaxSessionTimeoutMsProp, groupMaxSessionTimeoutMs.toString)
    properties.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "10")
  }

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()

    // create the test topic with all the brokers as replicas
    createTopic(topic, 2, brokerCount)
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

  protected def createConsumerWithGroupId(groupId: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val groupOverrideConfig = new Properties
    groupOverrideConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    createConsumer(configOverrides = groupOverrideConfig)
  }

  protected def sendRecords(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
                            tp: TopicPartition,
                            startingTimestamp: Long = System.currentTimeMillis()): Seq[ProducerRecord[Array[Byte], Array[Byte]]] = {
    val records = (0 until numRecords).map { i =>
      val timestamp = startingTimestamp + i.toLong
      val record = new ProducerRecord(tp.topic(), tp.partition(), timestamp, s"key $i".getBytes, s"value $i".getBytes)
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
        assertTrue(record.timestamp >= startingTimestamp && record.timestamp <= now,
          s"Got unexpected timestamp ${record.timestamp}. Timestamp should be between [$startingTimestamp, $now}]")
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
    def pollAction(polledRecords: ConsumerRecords[K, V]): Boolean = {
      assertTrue(polledRecords.asScala.size <= maxPollRecords)
      records ++= polledRecords.asScala
      records.size >= numRecords
    }
    TestUtils.pollRecordsUntilTrue(consumer, pollAction, waitTimeMs = 60000,
      msg = s"Timed out before consuming expected $numRecords records. " +
        s"The number consumed was ${records.size}.")
    records
  }

  protected def sendAndAwaitAsyncCommit[K, V](consumer: Consumer[K, V],
                                              offsetsOpt: Option[Map[TopicPartition, OffsetAndMetadata]] = None): Unit = {

    def sendAsyncCommit(callback: OffsetCommitCallback) = {
      offsetsOpt match {
        case Some(offsets) => consumer.commitAsync(offsets.asJava, callback)
        case None => consumer.commitAsync(callback)
      }
    }

    class RetryCommitCallback extends OffsetCommitCallback {
      var isComplete = false
      var error: Option[Exception] = None

      override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
        exception match {
          case e: RetriableCommitFailedException =>
            sendAsyncCommit(this)
          case e =>
            isComplete = true
            error = Option(e)
        }
      }
    }

    val commitCallback = new RetryCommitCallback

    sendAsyncCommit(commitCallback)
    TestUtils.pollUntilTrue(consumer, () => commitCallback.isComplete,
      "Failed to observe commit callback before timeout", waitTimeMs = 10000)

    assertEquals(None, commitCallback.error)
  }

  /**
    * Create 'numOfConsumersToAdd' consumers add then to the consumer group 'consumerGroup', and create corresponding
    * pollers for these consumers. Wait for partition re-assignment and validate.
    *
    * Currently, assignment validation requires that total number of partitions is greater or equal to
    * number of consumers, so subscriptions.size must be greater or equal the resulting number of consumers in the group
    *
    * @param numOfConsumersToAdd number of consumers to create and add to the consumer group
    * @param consumerGroup current consumer group
    * @param consumerPollers current consumer pollers
    * @param topicsToSubscribe topics to which new consumers will subscribe to
    * @param subscriptions set of all topic partitions
    */
  def addConsumersToGroupAndWaitForGroupAssignment(numOfConsumersToAdd: Int,
                                                   consumerGroup: mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                                                   consumerPollers: mutable.Buffer[ConsumerAssignmentPoller],
                                                   topicsToSubscribe: List[String],
                                                   subscriptions: Set[TopicPartition],
                                                   group: String = group): (mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]], mutable.Buffer[ConsumerAssignmentPoller]) = {
    assertTrue(consumerGroup.size + numOfConsumersToAdd <= subscriptions.size)
    addConsumersToGroup(numOfConsumersToAdd, consumerGroup, consumerPollers, topicsToSubscribe, subscriptions, group)
    // wait until topics get re-assigned and validate assignment
    validateGroupAssignment(consumerPollers, subscriptions)

    (consumerGroup, consumerPollers)
  }

  /**
    * Create 'numOfConsumersToAdd' consumers add then to the consumer group 'consumerGroup', and create corresponding
    * pollers for these consumers.
    *
    *
    * @param numOfConsumersToAdd number of consumers to create and add to the consumer group
    * @param consumerGroup current consumer group
    * @param consumerPollers current consumer pollers
    * @param topicsToSubscribe topics to which new consumers will subscribe to
    * @param subscriptions set of all topic partitions
    */
  def addConsumersToGroup(numOfConsumersToAdd: Int,
                          consumerGroup: mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]],
                          consumerPollers: mutable.Buffer[ConsumerAssignmentPoller],
                          topicsToSubscribe: List[String],
                          subscriptions: Set[TopicPartition],
                          group: String = group): (mutable.Buffer[KafkaConsumer[Array[Byte], Array[Byte]]], mutable.Buffer[ConsumerAssignmentPoller]) = {
    for (_ <- 0 until numOfConsumersToAdd) {
      val consumer = createConsumerWithGroupId(group)
      consumerGroup += consumer
      consumerPollers += subscribeConsumerAndStartPolling(consumer, topicsToSubscribe)
    }

    (consumerGroup, consumerPollers)
  }

  /**
    * Wait for consumers to get partition assignment and validate it.
    *
    * @param consumerPollers consumer pollers corresponding to the consumer group we are testing
    * @param subscriptions set of all topic partitions
    * @param msg message to print when waiting for/validating assignment fails
    */
  def validateGroupAssignment(consumerPollers: mutable.Buffer[ConsumerAssignmentPoller],
                              subscriptions: Set[TopicPartition],
                              msg: Option[String] = None,
                              waitTime: Long = 10000L): Unit = {
    val assignments = mutable.Buffer[Set[TopicPartition]]()
    TestUtils.waitUntilTrue(() => {
      assignments.clear()
      consumerPollers.foreach(assignments += _.consumerAssignment())
      isPartitionAssignmentValid(assignments, subscriptions)
    }, msg.getOrElse(s"Did not get valid assignment for partitions $subscriptions. Instead, got $assignments"), waitTime)
  }

  /**
    * Subscribes consumer 'consumer' to a given list of topics 'topicsToSubscribe', creates
    * consumer poller and starts polling.
    * Assumes that the consumer is not subscribed to any topics yet
    *
    * @param consumer consumer
    * @param topicsToSubscribe topics that this consumer will subscribe to
    * @return consumer poller for the given consumer
    */
  def subscribeConsumerAndStartPolling(consumer: Consumer[Array[Byte], Array[Byte]],
                                       topicsToSubscribe: List[String],
                                       partitionsToAssign: Set[TopicPartition] = Set.empty[TopicPartition]): ConsumerAssignmentPoller = {
    assertEquals(0, consumer.assignment().size)
    val consumerPoller = if (topicsToSubscribe.nonEmpty)
      new ConsumerAssignmentPoller(consumer, topicsToSubscribe)
    else
      new ConsumerAssignmentPoller(consumer, partitionsToAssign)

    consumerPoller.start()
    consumerPoller
  }

  protected def awaitRebalance(consumer: Consumer[_, _], rebalanceListener: TestConsumerReassignmentListener): Unit = {
    val numReassignments = rebalanceListener.callsToAssigned
    TestUtils.pollUntilTrue(consumer, () => rebalanceListener.callsToAssigned > numReassignments,
      "Timed out before expected rebalance completed")
  }

  protected def ensureNoRebalance(consumer: Consumer[_, _], rebalanceListener: TestConsumerReassignmentListener): Unit = {
    // The best way to verify that the current membership is still active is to commit offsets.
    // This would fail if the group had rebalanced.
    val initialRevokeCalls = rebalanceListener.callsToRevoked
    sendAndAwaitAsyncCommit(consumer)
    assertEquals(initialRevokeCalls, rebalanceListener.callsToRevoked)
  }

  protected class CountConsumerCommitCallback extends OffsetCommitCallback {
    var successCount = 0
    var failCount = 0
    var lastError: Option[Exception] = None

    override def onComplete(offsets: util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      if (exception == null) {
        successCount += 1
      } else {
        failCount += 1
        lastError = Some(exception)
      }
    }
  }

  protected class ConsumerAssignmentPoller(consumer: Consumer[Array[Byte], Array[Byte]],
                                           topicsToSubscribe: List[String],
                                           partitionsToAssign: Set[TopicPartition])
    extends ShutdownableThread("daemon-consumer-assignment", false) {

    def this(consumer: Consumer[Array[Byte], Array[Byte]], topicsToSubscribe: List[String]) = {
      this(consumer, topicsToSubscribe, Set.empty[TopicPartition])
    }

    def this(consumer: Consumer[Array[Byte], Array[Byte]], partitionsToAssign: Set[TopicPartition]) = {
      this(consumer, List.empty[String], partitionsToAssign)
    }

    @volatile var thrownException: Option[Throwable] = None
    @volatile var receivedMessages = 0

    private val partitionAssignment = mutable.Set[TopicPartition]()
    @volatile private var subscriptionChanged = false
    private var topicsSubscription = topicsToSubscribe

    val rebalanceListener: ConsumerRebalanceListener = new ConsumerRebalanceListener {
      override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) = {
        partitionAssignment ++= partitions.toArray(new Array[TopicPartition](0))
      }

      override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) = {
        partitionAssignment --= partitions.toArray(new Array[TopicPartition](0))
      }
    }

    if (partitionsToAssign.isEmpty) {
      consumer.subscribe(topicsToSubscribe.asJava, rebalanceListener)
    } else {
      consumer.assign(partitionsToAssign.asJava)
    }

    def consumerAssignment(): Set[TopicPartition] = {
      partitionAssignment.toSet
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
      if (subscriptionChanged)
        throw new IllegalStateException("Do not call subscribe until the previous subscribe request is processed.")
      if (partitionsToAssign.nonEmpty)
        throw new IllegalStateException("Cannot call subscribe when configured to use manual partition assignment")

      topicsSubscription = newTopicsToSubscribe
      subscriptionChanged = true
    }

    def isSubscribeRequestProcessed: Boolean = {
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
        receivedMessages += consumer.poll(Duration.ofMillis(50)).count()
      } catch {
        case _: WakeupException => // ignore for shutdown
        case e: Throwable =>
          thrownException = Some(e)
          throw e
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
    val allNonEmptyAssignments = assignments.forall(assignment => assignment.nonEmpty)
    if (!allNonEmptyAssignments) {
      // at least one consumer got empty assignment
      return false
    }

    // make sure that sum of all partitions to all consumers equals total number of partitions
    val totalPartitionsInAssignments = assignments.foldLeft(0)(_ + _.size)
    if (totalPartitionsInAssignments != partitions.size) {
      // either same partitions got assigned to more than one consumer or some
      // partitions were not assigned
      return false
    }

    // The above checks could miss the case where one or more partitions were assigned to more
    // than one consumer and the same number of partitions were missing from assignments.
    // Make sure that all unique assignments are the same as 'partitions'
    val uniqueAssignedPartitions = assignments.foldLeft(Set.empty[TopicPartition])(_ ++ _)
    uniqueAssignedPartitions == partitions
  }

}
