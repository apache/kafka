/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.consumer

import java.util.{Collections, Properties}
import java.util.regex.Pattern

import kafka.api.OffsetRequest
import kafka.common.StreamEndException
import kafka.message.Message
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders

/**
 * A base consumer used to abstract both old and new consumer
 * this class should be removed (along with BaseProducer)
 * once we deprecate old consumer
 */
@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0")
trait BaseConsumer {
  def receive(): BaseConsumerRecord
  def stop(): Unit
  def cleanup(): Unit
  def commit(): Unit
}

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.ConsumerRecord instead.", "0.11.0.0")
case class BaseConsumerRecord(topic: String,
                              partition: Int,
                              offset: Long,
                              timestamp: Long = Message.NoTimestamp,
                              timestampType: TimestampType = TimestampType.NO_TIMESTAMP_TYPE,
                              key: Array[Byte],
                              value: Array[Byte],
                              headers: Headers = new RecordHeaders())

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0")
class NewShinyConsumer(topic: Option[String], partitionId: Option[Int], offset: Option[Long], whitelist: Option[String],
                       consumer: Consumer[Array[Byte], Array[Byte]], val timeoutMs: Long = Long.MaxValue) extends BaseConsumer {
  consumerInit()
  var recordIter = consumer.poll(0).iterator

  def consumerInit(): Unit = {
    (topic, partitionId, offset, whitelist) match {
      case (Some(topic), Some(partitionId), Some(offset), None) =>
        seek(topic, partitionId, offset)
      case (Some(topic), Some(partitionId), None, None) =>
        // default to latest if no offset is provided
        seek(topic, partitionId, OffsetRequest.LatestTime)
      case (Some(topic), None, None, None) =>
        consumer.subscribe(Collections.singletonList(topic))
      case (None, None, None, Some(whitelist)) =>
        consumer.subscribe(Pattern.compile(whitelist))
      case _ =>
        throw new IllegalArgumentException("An invalid combination of arguments is provided. " +
            "Exactly one of 'topic' or 'whitelist' must be provided. " +
            "If 'topic' is provided, an optional 'partition' may also be provided. " +
            "If 'partition' is provided, an optional 'offset' may also be provided, otherwise, consumption starts from the end of the partition.")
    }
  }

  def seek(topic: String, partitionId: Int, offset: Long): Unit = {
    val topicPartition = new TopicPartition(topic, partitionId)
    consumer.assign(Collections.singletonList(topicPartition))
    offset match {
      case OffsetRequest.EarliestTime => consumer.seekToBeginning(Collections.singletonList(topicPartition))
      case OffsetRequest.LatestTime => consumer.seekToEnd(Collections.singletonList(topicPartition))
      case _ => consumer.seek(topicPartition, offset)
    }
  }

  def resetUnconsumedOffsets(): Unit = {
    val smallestUnconsumedOffsets = collection.mutable.Map[TopicPartition, Long]()
    while (recordIter.hasNext) {
      val record = recordIter.next()
      val tp = new TopicPartition(record.topic, record.partition)
      // avoid auto-committing offsets which haven't been consumed
      smallestUnconsumedOffsets.getOrElseUpdate(tp, record.offset)
    }
    smallestUnconsumedOffsets.foreach { case (tp, offset) => consumer.seek(tp, offset) }
  }

  override def receive(): BaseConsumerRecord = {
    if (!recordIter.hasNext) {
      recordIter = consumer.poll(timeoutMs).iterator
      if (!recordIter.hasNext)
        throw new ConsumerTimeoutException
    }

    val record = recordIter.next
    BaseConsumerRecord(record.topic,
                       record.partition,
                       record.offset,
                       record.timestamp,
                       record.timestampType,
                       record.key,
                       record.value,
                       record.headers)
  }

  override def stop(): Unit = {
    this.consumer.wakeup()
  }

  override def cleanup(): Unit = {
    resetUnconsumedOffsets()
    this.consumer.close()
  }

  override def commit(): Unit = {
    this.consumer.commitSync()
  }
}

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.consumer.KafkaConsumer instead.", "0.11.0.0")
class OldConsumer(topicFilter: TopicFilter, consumerProps: Properties) extends BaseConsumer {
  import kafka.serializer.DefaultDecoder

  val consumerConnector = Consumer.create(new ConsumerConfig(consumerProps))
  val stream: KafkaStream[Array[Byte], Array[Byte]] =
    consumerConnector.createMessageStreamsByFilter(topicFilter, 1, new DefaultDecoder(), new DefaultDecoder()).head
  val iter = stream.iterator

  override def receive(): BaseConsumerRecord = {
    if (!iter.hasNext())
      throw new StreamEndException

    val messageAndMetadata = iter.next
    BaseConsumerRecord(messageAndMetadata.topic,
                       messageAndMetadata.partition,
                       messageAndMetadata.offset,
                       messageAndMetadata.timestamp,
                       messageAndMetadata.timestampType,
                       messageAndMetadata.key,
                       messageAndMetadata.message, 
                       new RecordHeaders())
  }

  override def stop(): Unit = {
    this.consumerConnector.shutdown()
  }

  override def cleanup(): Unit = {
    this.consumerConnector.shutdown()
  }

  override def commit(): Unit = {
    this.consumerConnector.commitOffsets
  }
}
