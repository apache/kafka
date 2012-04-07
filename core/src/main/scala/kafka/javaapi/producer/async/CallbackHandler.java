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
package kafka.javaapi.producer.async;


import java.util.Properties;
import kafka.producer.async.QueueItem;

/**
 * Callback handler APIs for use in the async producer. The purpose is to
 * give the user some callback handles to insert custom functionality at
 * various stages as the data flows through the pipeline of the async producer
 */
public interface CallbackHandler<T> {
    /**
     * Initializes the callback handler using a Properties object
     * @param props the properties used to initialize the callback handler
    */
    public void init(Properties props);

    /**
     * Callback to process the data before it enters the batching queue
     * of the asynchronous producer
     * @param data the data sent to the producer
     * @return the processed data that enters the queue
    */
    public QueueItem<T> beforeEnqueue(QueueItem<T> data);

    /**
     * Callback to process the data just after it enters the batching queue
     * of the asynchronous producer
     * @param data the data sent to the producer
     * @param added flag that indicates if the data was successfully added to the queue
    */
    public void afterEnqueue(QueueItem<T> data, boolean added);

    /**
     * Callback to process the data item right after it has been dequeued by the
     * background sender thread of the asynchronous producer
     * @param data the data item dequeued from the async producer queue
     * @return the processed list of data items that gets added to the data handled by the event handler
     */
    public java.util.List<QueueItem<T>> afterDequeuingExistingData(QueueItem<T> data);

    /**
     * Callback to process the batched data right before it is being processed by the
     * handle API of the event handler
     * @param data the batched data received by the event handler
     * @return the processed batched data that gets processed by the handle() API of the event handler
    */
    public java.util.List<QueueItem<T>> beforeSendingData(java.util.List<QueueItem<T>> data);

    /**
     * Callback to process the last batch of data right before the producer send thread is shutdown
     * @return the last batch of data that is sent to the EventHandler
    */
    public java.util.List<QueueItem<T>> lastBatchBeforeClose();

    /**
     * Cleans up and shuts down the callback handler
    */
    public void close();
}
