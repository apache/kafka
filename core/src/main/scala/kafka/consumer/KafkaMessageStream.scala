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

/**
 * All calls to elements should produce the same thread-safe iterator? Should have a separate thread
 * that feeds messages into a blocking queue for processing.
 */
class KafkaMessageStream[T](val topic: String,
                            private val queue: BlockingQueue[FetchedDataChunk],
                            consumerTimeoutMs: Int,
                            private val decoder: Decoder[T])
   extends Iterable[T] with java.lang.Iterable[T]{

  private val iter: ConsumerIterator[T] =
    new ConsumerIterator[T](topic, queue, consumerTimeoutMs, decoder)
    
  /**
   *  Create an iterator over messages in the stream.
   */
  def iterator(): ConsumerIterator[T] = iter

  /**
   * This method clears the queue being iterated during the consumer rebalancing. This is mainly
   * to reduce the number of duplicates received by the consumer
   */
  def clear() {
    iter.clearCurrentChunk()
  }

}
