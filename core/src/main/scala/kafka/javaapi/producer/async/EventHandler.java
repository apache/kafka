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


import java.util.List;
import java.util.Properties;
import kafka.javaapi.producer.SyncProducer;
import kafka.producer.async.QueueItem;
import kafka.serializer.Encoder;

/**
 * Handler that dispatches the batched data from the queue of the
 * asynchronous producer.
 */
public interface EventHandler<T> {
    /**
     * Initializes the event handler using a Properties object
     * @param props the properties used to initialize the event handler
    */
    public void init(Properties props);

    /**
     * Callback to dispatch the batched data and send it to a Kafka server
     * @param events the data sent to the producer
     * @param producer the low-level producer used to send the data
    */
    public void handle(List<QueueItem<T>> events, SyncProducer producer, Encoder<T> encoder);

    /**
     * Cleans up and shuts down the event handler
    */
    public void close();
}
