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

/**
 * Callback handler APIs for use in the async producer. The purpose is to
 * give the user some callback handles to insert custom functionality at
 * various stages as the data flows through the pipeline of the async producer
 */
trait CallbackHandler[T] {
  /**
   * Initializes the callback handler using a Properties object
   * @param props properties used to initialize the callback handler
   */
  def init(props: Properties)

  /**
   * Callback to process the data before it enters the batching queue
   * of the asynchronous producer
   * @param data the data sent to the producer
   * @return the processed data that enters the queue
   */
  def beforeEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]]): QueueItem[T]

  /**
   * Callback to process the data right after it enters the batching queue
   * of the asynchronous producer
   * @param data the data sent to the producer
   * @param added flag that indicates if the data was successfully added to the queue
   */
  def afterEnqueue(data: QueueItem[T] = null.asInstanceOf[QueueItem[T]], added: Boolean)

  /**
   * Callback to process the data item right after it has been dequeued by the
   * background sender thread of the asynchronous producer
   * @param data the data item dequeued from the async producer queue
   * @return the processed list of data items that gets added to the data handled by the event handler
   */
  def afterDequeuingExistingData(data: QueueItem[T] = null): scala.collection.mutable.Seq[QueueItem[T]]

  /**
   * Callback to process the batched data right before it is being sent by the
   * handle API of the event handler
   * @param data the batched data received by the event handler
   * @return the processed batched data that gets sent by the handle() API of the event handler
   */
  def beforeSendingData(data: Seq[QueueItem[T]] = null): scala.collection.mutable.Seq[QueueItem[T]]

  /**
   * Callback to process the last batch of data right before the producer send thread is shutdown
   * @return the last batch of data that is sent to the EventHandler
  */
  def lastBatchBeforeClose: scala.collection.mutable.Seq[QueueItem[T]]

  /**
   * Cleans up and shuts down the callback handler
   */
  def close
}
