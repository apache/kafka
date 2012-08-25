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

trait AsyncProducerConfig {
  val props: VerifiableProperties

  /* maximum time, in milliseconds, for buffering data on the producer queue */
  val queueTime = props.getInt("queue.time", 5000)

  /** the maximum size of the blocking queue for buffering on the producer */
  val queueSize = props.getInt("queue.size", 10000)

  /**
   * Timeout for event enqueue:
   * 0: events will be enqueued immediately or dropped if the queue is full
   * -ve: enqueue will block indefinitely if the queue is full
   * +ve: enqueue will block up to this many milliseconds if the queue is full
   */
  val enqueueTimeoutMs = props.getInt("queue.enqueueTimeout.ms", 0)

  /** the number of messages batched at the producer */
  val batchSize = props.getInt("batch.size", 200)

  /** the serializer class for events */
  val serializerClass = props.getString("serializer.class", "kafka.serializer.DefaultEncoder")
}
