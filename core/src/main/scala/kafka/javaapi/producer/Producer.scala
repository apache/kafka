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

package kafka.javaapi.producer

import kafka.producer.ProducerConfig
import kafka.producer.KeyedMessage
import scala.collection.mutable

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.KafkaProducer instead.", "0.10.0.0")
class Producer[K,V](private val underlying: kafka.producer.Producer[K,V]) // for testing only
{
  def this(config: ProducerConfig) = this(new kafka.producer.Producer[K,V](config))
  /**
   * Sends the data to a single topic, partitioned by key, using either the
   * synchronous or the asynchronous producer
   * @param message the producer data object that encapsulates the topic, key and message data
   */
  def send(message: KeyedMessage[K,V]) {
    underlying.send(message)
  }

  /**
   * Use this API to send data to multiple topics
   * @param messages list of producer data objects that encapsulate the topic, key and message data
   */
  def send(messages: java.util.List[KeyedMessage[K,V]]) {
    import collection.JavaConversions._
    underlying.send((messages: mutable.Buffer[KeyedMessage[K,V]]).toSeq: _*)
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close = underlying.close
}
