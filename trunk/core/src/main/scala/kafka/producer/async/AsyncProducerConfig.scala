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

import java.util.Properties
import kafka.utils.Utils
import kafka.producer.SyncProducerConfig

class AsyncProducerConfig(override val props: Properties) extends SyncProducerConfig(props)
        with AsyncProducerConfigShared {
}

trait AsyncProducerConfigShared {
  val props: Properties

  /* maximum time, in milliseconds, for buffering data on the producer queue */
  val queueTime = Utils.getInt(props, "queue.time", 5000)

  /** the maximum size of the blocking queue for buffering on the producer */
  val queueSize = Utils.getInt(props, "queue.size", 10000)

  /**
   * Timeout for event enqueue:
   * 0: events will be enqueued immediately or dropped if the queue is full
   * -ve: enqueue will block indefinitely if the queue is full
   * +ve: enqueue will block up to this many milliseconds if the queue is full
   */
  val enqueueTimeoutMs = Utils.getInt(props, "queue.enqueueTimeout.ms", 0)

  /** the number of messages batched at the producer */
  val batchSize = Utils.getInt(props, "batch.size", 200)

  /** the serializer class for events */
  val serializerClass = Utils.getString(props, "serializer.class", "kafka.serializer.DefaultEncoder")

  /** the callback handler for one or multiple events */
  val cbkHandler = Utils.getString(props, "callback.handler", null)

  /** properties required to initialize the callback handler */
  val cbkHandlerProps = Utils.getProps(props, "callback.handler.props", null)

  /** the handler for events */
  val eventHandler = Utils.getString(props, "event.handler", null)

  /** properties required to initialize the callback handler */
  val eventHandlerProps = Utils.getProps(props, "event.handler.props", null)
}
