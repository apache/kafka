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

package kafka.consumer


import java.util.concurrent.BlockingQueue
import kafka.serializer.Decoder
import kafka.message.MessageAndMetadata

class KafkaStream[K,V](private val queue: BlockingQueue[FetchedDataChunk],
                        consumerTimeoutMs: Int,
                        private val keyDecoder: Decoder[K],
                        private val valueDecoder: Decoder[V],
                        val clientId: String)
   extends Iterable[MessageAndMetadata[K,V]] with java.lang.Iterable[MessageAndMetadata[K,V]] {

  private val iter: ConsumerIterator[K,V] =
    new ConsumerIterator[K,V](queue, consumerTimeoutMs, keyDecoder, valueDecoder, clientId)

  /**
   *  Create an iterator over messages in the stream.
   */
  def iterator(): ConsumerIterator[K,V] = iter

  /**
   * This method clears the queue being iterated during the consumer rebalancing. This is mainly
   * to reduce the number of duplicates received by the consumer
   */
  def clear() {
    iter.clearCurrentChunk()
  }

  override def toString: String = {
     "%s kafka stream".format(clientId)
  }
}
