/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams;

import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;

public interface KafkaClientSupplier {
    /**
     * Creates an instance of Producer which is used to produce records to brokers by this StreamThread.
     * @param config Producer config which supplied by StreamsConfig given to KafkaStreams
     * @return An instance of kafka Producer.
     */
    Producer<byte[], byte[]> getProducer(Map<String, Object> config);

    /**
     * Creates an instance of Consumer which is used to consume records from brokers by this StreamThread.
     * @param config Consumer config which supplied by StreamsConfig given to KafkaStreams
     * @return An instance of kafka Consumer.
     */
    Consumer<byte[], byte[]> getConsumer(Map<String, Object> config);

    /**
     * Creates an instance of Consumer which is used to consume records from brokers by this StreamThread.
     * @param config Restore consumer config which supplied by StreamsConfig given to KafkaStreams
     * @return An instance of kafka Consumer.
     */
    Consumer<byte[], byte[]> getRestoreConsumer(Map<String, Object> config);
}
