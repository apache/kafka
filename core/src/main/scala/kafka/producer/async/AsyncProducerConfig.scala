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

import kafka.utils.VerifiableProperties

@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.ProducerConfig instead.", "0.10.0.0")
trait AsyncProducerConfig {
  val props: VerifiableProperties

  /* maximum time, in milliseconds, for buffering data on the producer queue */
  val queueBufferingMaxMs = props.getInt("queue.buffering.max.ms", 5000)

  /** the maximum size of the blocking queue for buffering on the producer */
  val queueBufferingMaxMessages = props.getInt("queue.buffering.max.messages", 10000)

  /**
   * Timeout for event enqueue:
   * 0: events will be enqueued immediately or dropped if the queue is full
   * -ve: enqueue will block indefinitely if the queue is full
   * +ve: enqueue will block up to this many milliseconds if the queue is full
   */
  val queueEnqueueTimeoutMs = props.getInt("queue.enqueue.timeout.ms", -1)

  /** the number of messages batched at the producer */
  val batchNumMessages = props.getInt("batch.num.messages", 200)

  /** the serializer class for values */
  val serializerClass = props.getString("serializer.class", "kafka.serializer.DefaultEncoder")
  
  /** the serializer class for keys (defaults to the same as for values) */
  val keySerializerClass = props.getString("key.serializer.class", serializerClass)
  
}
