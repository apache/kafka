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

import kafka.producer.SyncProducerConfig
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.api.{ProducerResponse, PartitionData, TopicData}

class SyncProducer(syncProducer: kafka.producer.SyncProducer) {

  def this(config: SyncProducerConfig) = this(new kafka.producer.SyncProducer(config))

  val underlying = syncProducer

  def send(producerRequest: kafka.javaapi.ProducerRequest): ProducerResponse = {
    underlying.send(producerRequest.underlying)
  }

  def send(topic: String, messages: ByteBufferMessageSet): ProducerResponse = {
    val partitionData = Array[PartitionData]( new PartitionData(-1, messages.underlying) )
    val data = Array[TopicData]( new TopicData(topic, partitionData) )
    val producerRequest = new kafka.api.ProducerRequest(-1, "", 0, 0, data)
    underlying.send(producerRequest)
  }

  def close() {
    underlying.close
  }
}
