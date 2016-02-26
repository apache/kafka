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

import java.util.Properties
import java.util.regex.Pattern

import kafka.common.StreamEndException
import kafka.message.Message
import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener
import org.apache.kafka.common.record.TimestampType

/**
 * A base consumer used to abstract both old and new consumer
 * this class should be removed (along with BaseProducer) be removed
 * once we deprecate old consumer
 */
trait BaseConsumer {
  def receive(): BaseConsumerRecord
  def stop()
  def cleanup()
  def commit()
}

case class BaseConsumerRecord(topic: String,
                              partition: Int,
                              offset: Long,
                              timestamp: Long = Message.NoTimestamp,
                              timestampType: TimestampType = TimestampType.NO_TIMESTAMP_TYPE,
                              key: Array[Byte],
                              value: Array[Byte])

class NewShinyConsumer(topic: Option[String], whitelist: Option[String], consumerProps: Properties, val timeoutMs: Long = Long.MaxValue) extends BaseConsumer {
  import org.apache.kafka.clients.consumer.KafkaConsumer

  import scala.collection.JavaConversions._

  val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
  if (topic.isDefined)
    consumer.subscribe(List(topic.get))
  else if (whitelist.isDefined)
    consumer.subscribe(Pattern.compile(whitelist.get), new NoOpConsumerRebalanceListener())
  else
    throw new IllegalArgumentException("Exactly one of topic or whitelist has to be provided.")

  var recordIter = consumer.poll(0).iterator

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
                       record.value)
  }

  override def stop() {
    this.consumer.wakeup()
  }

  override def cleanup() {
    this.consumer.close()
  }

  override def commit() {
    this.consumer.commitSync()
  }
}

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
                       messageAndMetadata.message)
  }

  override def stop() {
    this.consumerConnector.shutdown()
  }

  override def cleanup() {
    this.consumerConnector.shutdown()
  }

  override def commit() {
    this.consumerConnector.commitOffsets
  }
}

