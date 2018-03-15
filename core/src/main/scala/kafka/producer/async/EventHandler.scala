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

import kafka.producer.KeyedMessage

/**
 * Handler that dispatches the batched data from the queue.
 */
@deprecated("This trait has been deprecated and will be removed in a future release.", "0.10.0.0")
trait EventHandler[K,V] {

  /**
   * Callback to dispatch the batched data and send it to a Kafka server
   * @param events the data sent to the producer
  */
  def handle(events: Seq[KeyedMessage[K,V]])

  /**
   * Cleans up and shuts down the event handler
  */
  def close(): Unit
}
