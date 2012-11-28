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

package kafka.producer.async

import collection.mutable.HashMap
import collection.mutable.Map
import kafka.api.ProducerRequest
import kafka.serializer.Encoder
import java.util.Properties
import kafka.utils.Logging
import kafka.producer.{ProducerConfig, SyncProducer}
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet}


private[kafka] class DefaultEventHandler[T](val config: ProducerConfig,
                                            val cbkHandler: CallbackHandler[T]) extends EventHandler[T] with Logging {

  override def init(props: Properties) { }

  override def handle(events: Seq[QueueItem[T]], syncProducer: SyncProducer, serializer: Encoder[T]) {
    var processedEvents = events
    if(cbkHandler != null)
      processedEvents = cbkHandler.beforeSendingData(events)

    if(logger.isTraceEnabled)
      processedEvents.foreach(event => trace("Handling event for Topic: %s, Partition: %d"
        .format(event.getTopic, event.getPartition)))

    send(serialize(collate(processedEvents), serializer), syncProducer)
  }

  private def send(messagesPerTopic: Map[(String, Int), ByteBufferMessageSet], syncProducer: SyncProducer) {
    if(messagesPerTopic.size > 0) {
      val requests = messagesPerTopic.map(f => new ProducerRequest(f._1._1, f._1._2, f._2)).toArray

      val maxAttempts = config.numRetries + 1
      var attemptsRemaining = maxAttempts
      var sent = false

      while (attemptsRemaining > 0 && !sent) {
        attemptsRemaining -= 1
        try {
          syncProducer.multiSend(requests)
          trace("kafka producer sent messages for topics %s to broker %s:%d (on attempt %d)"
                        .format(messagesPerTopic, syncProducer.config.host, syncProducer.config.port, maxAttempts - attemptsRemaining))
          sent = true
        }
        catch {
          case e => warn("Error sending messages, %d attempts remaining".format(attemptsRemaining), e)
          if (attemptsRemaining == 0)
            throw e
        }
      }
    }
  }

  private def serialize(eventsPerTopic: Map[(String,Int), Seq[T]],
                        serializer: Encoder[T]): Map[(String, Int), ByteBufferMessageSet] = {
    val eventsPerTopicMap = eventsPerTopic.map(e => ((e._1._1, e._1._2) , e._2.map(l => serializer.toMessage(l))))
    /** enforce the compressed.topics config here.
     *  If the compression codec is anything other than NoCompressionCodec,
     *    Enable compression only for specified topics if any
     *    If the list of compressed topics is empty, then enable the specified compression codec for all topics
     *  If the compression codec is NoCompressionCodec, compression is disabled for all topics
     */

    val messagesPerTopicPartition = eventsPerTopicMap.map { topicAndEvents =>
      ((topicAndEvents._1._1, topicAndEvents._1._2),
        config.compressionCodec match {
          case NoCompressionCodec =>
            trace("Sending %d messages with no compression to topic %s on partition %d"
                .format(topicAndEvents._2.size, topicAndEvents._1._1, topicAndEvents._1._2))
            new ByteBufferMessageSet(NoCompressionCodec, topicAndEvents._2: _*)
          case _ =>
            config.compressedTopics.size match {
              case 0 =>
                trace("Sending %d messages with compression codec %d to topic %s on partition %d"
                    .format(topicAndEvents._2.size, config.compressionCodec.codec, topicAndEvents._1._1, topicAndEvents._1._2))
                new ByteBufferMessageSet(config.compressionCodec, topicAndEvents._2: _*)
              case _ =>
                if(config.compressedTopics.contains(topicAndEvents._1._1)) {
                  trace("Sending %d messages with compression codec %d to topic %s on partition %d"
                      .format(topicAndEvents._2.size, config.compressionCodec.codec, topicAndEvents._1._1, topicAndEvents._1._2))
                  new ByteBufferMessageSet(config.compressionCodec, topicAndEvents._2: _*)
                }
                else {
                  trace("Sending %d messages to topic %s and partition %d with no compression as %s is not in compressed.topics - %s"
                      .format(topicAndEvents._2.size, topicAndEvents._1._1, topicAndEvents._1._2, topicAndEvents._1._1,
                      config.compressedTopics.toString))
                  new ByteBufferMessageSet(NoCompressionCodec, topicAndEvents._2: _*)
                }
            }
        })
    }
    messagesPerTopicPartition
  }

  private def collate(events: Seq[QueueItem[T]]): Map[(String,Int), Seq[T]] = {
    val collatedEvents = new HashMap[(String, Int), Seq[T]]
    val distinctTopics = events.map(e => e.getTopic).toSeq.distinct
    val distinctPartitions = events.map(e => e.getPartition).distinct

    var remainingEvents = events
    distinctTopics foreach { topic =>
      val topicEvents = remainingEvents partition (e => e.getTopic.equals(topic))
      remainingEvents = topicEvents._2
      distinctPartitions.foreach { p =>
        val topicPartitionEvents = (topicEvents._1 partition (e => (e.getPartition == p)))._1
        if(topicPartitionEvents.size > 0)
          collatedEvents += ((topic, p) -> topicPartitionEvents.map(q => q.getData))
      }
    }
    collatedEvents
  }

  override def close = {
  }
}
