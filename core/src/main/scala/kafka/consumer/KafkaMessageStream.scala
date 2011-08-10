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
import org.apache.log4j.Logger
import kafka.message.Message


/**
 * All calls to elements should produce the same thread-safe iterator? Should have a seperate thread
 * that feeds messages into a blocking queue for processing.
 */
class KafkaMessageStream(private val queue: BlockingQueue[FetchedDataChunk], consumerTimeoutMs: Int)
   extends Iterable[Message] with java.lang.Iterable[Message]{

  private val logger = Logger.getLogger(getClass())
  private val iter: ConsumerIterator = new ConsumerIterator(queue, consumerTimeoutMs)
    
  /**
   *  Create an iterator over messages in the stream.
   */
  def iterator(): ConsumerIterator = iter
}
