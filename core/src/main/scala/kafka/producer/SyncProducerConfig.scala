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

package kafka.producer

import java.util.Properties
import kafka.utils.VerifiableProperties

@deprecated("This class has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.ProducerConfig instead.", "0.10.0.0")
class SyncProducerConfig private (val props: VerifiableProperties) extends SyncProducerConfigShared {
  def this(originalProps: Properties) {
    this(new VerifiableProperties(originalProps))
    // no need to verify the property since SyncProducerConfig is supposed to be used internally
  }

  /** the broker to which the producer sends events */
  val host = props.getString("host")

  /** the port on which the broker is running */
  val port = props.getInt("port")
}

@deprecated("This trait has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.ProducerConfig instead.", "0.10.0.0")
trait SyncProducerConfigShared {
  val props: VerifiableProperties
  
  val sendBufferBytes = props.getInt("send.buffer.bytes", 100*1024)

  /* the client application sending the producer requests */
  val clientId = props.getString("client.id", SyncProducerConfig.DefaultClientId)

  /*
   * The number of acknowledgments the producer requires the leader to have received before considering a request complete.
   * This controls the durability of the messages sent by the producer.
   *
   * request.required.acks = 0 - means the producer will not wait for any acknowledgement from the leader.
   * request.required.acks = 1 - means the leader will write the message to its local log and immediately acknowledge
   * request.required.acks = -1 - means the leader will wait for acknowledgement from all in-sync replicas before acknowledging the write
   */

  val requestRequiredAcks = props.getShortInRange("request.required.acks", SyncProducerConfig.DefaultRequiredAcks,(-1,1))

  /*
   * The ack timeout of the producer requests. Value must be non-negative and non-zero
   */
  val requestTimeoutMs = props.getIntInRange("request.timeout.ms", SyncProducerConfig.DefaultAckTimeoutMs,
                                             (1, Integer.MAX_VALUE))
}

@deprecated("This object has been deprecated and will be removed in a future release. " +
            "Please use org.apache.kafka.clients.producer.ProducerConfig instead.", "0.10.0.0")
object SyncProducerConfig {
  val DefaultClientId = ""
  val DefaultRequiredAcks : Short = 0
  val DefaultAckTimeoutMs = 10000
}
