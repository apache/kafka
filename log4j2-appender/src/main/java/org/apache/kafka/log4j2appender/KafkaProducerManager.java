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
package org.apache.kafka.log4j2appender;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.logging.log4j.status.StatusLogger;

import java.lang.invoke.MethodHandle;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerManager implements AutoCloseable {
    private final MethodHandle producerConstructor;
    private final Properties properties;
    private final String topic;
    private final boolean syncSend;
    private final boolean ignoreExceptions;

    private Producer<byte[], byte[]> producer;

    KafkaProducerManager(final MethodHandle producerConstructor,
                                   final Properties properties,
                                   final String topic,
                                   final boolean syncSend,
                                   final boolean ignoreExceptions) {
        this.producerConstructor = Objects.requireNonNull(producerConstructor);
        this.properties = Objects.requireNonNull(properties);
        if (Objects.requireNonNull(properties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isEmpty()) {
            throw new IllegalArgumentException("bootstrapServers can't be empty");
        }
        this.topic = Objects.requireNonNull(topic);
        if (this.topic.isEmpty()) {
            throw new IllegalArgumentException("topic can't be empty");
        }
        this.syncSend = syncSend;
        this.ignoreExceptions = ignoreExceptions;
    }

    String getTopic() {
        return topic;
    }

    Producer<byte[], byte[]> getProducer() {
        if (producer == null) {
            try {
                producer = (Producer<byte[], byte[]>) producerConstructor.invoke(properties);
            } catch (Throwable e) {
                throw new IllegalStateException(e);
            }
        }

        return producer;
    }

    Properties getProperties() {
        return properties;
    }

    public void send(final byte[] msg) throws RuntimeException {
        Future<RecordMetadata> response = getProducer().send(new ProducerRecord<>(topic, msg));
        if (syncSend) {
            try {
                response.get();
            } catch (InterruptedException | ExecutionException ex) {
                if (!ignoreExceptions)
                    throw new RuntimeException(ex);
                StatusLogger.getLogger().debug("Exception while getting response", ex);
            }
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close(Duration.ofMillis(500L));
            producer = null;
        }
    }
}
