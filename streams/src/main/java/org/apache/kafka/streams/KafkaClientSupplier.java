/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.StreamThread;

import java.util.Map;

/**
 * {@code KafkaClientSupplier} can be used to provide custom Kafka clients to a {@link KafkaStreams} instance.
 *
 * @see KafkaStreams#KafkaStreams(org.apache.kafka.streams.Topology, StreamsConfig, KafkaClientSupplier)
 */
public interface KafkaClientSupplier {
    /**
     * Create a {@link Producer} which is used to write records to sink topics.
     *
     * @param config {@link StreamsConfig#getProducerConfigs(String) producer config} which is supplied by the
     *               {@link StreamsConfig} given to the {@link KafkaStreams} instance
     * @return an instance of Kafka producer
     */
    Producer<byte[], byte[]> getProducer(final Map<String, Object> config);

    /**
     * Create a {@link Consumer} which is used to read records of source topics.
     *
     * @param config {@link StreamsConfig#getConsumerConfigs(StreamThread, String, String) consumer config} which is
     *               supplied by the {@link StreamsConfig} given to the {@link KafkaStreams} instance
     * @return an instance of Kafka consumer
     */
    Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config);

    /**
     * Create a {@link Consumer} which is used to read records to restore {@link StateStore}s.
     *
     * @param config {@link StreamsConfig#getRestoreConsumerConfigs(String) restore consumer config} which is supplied
     *               by the {@link StreamsConfig} given to the {@link KafkaStreams}
     * @return an instance of Kafka consumer
     */
    Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config);
}
