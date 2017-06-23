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
package org.apache.kafka.test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.KafkaClientSupplier;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class MockClientSupplier implements KafkaClientSupplier {
    private final String applicationId;
    private static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

    public final MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    public final MockConsumer<byte[], byte[]> restoreConsumer = new MockConsumer<>(OffsetResetStrategy.LATEST);

    public final List<MockProducer> producers = new LinkedList<>();

    public MockClientSupplier() {
        this(null);
    }

    public MockClientSupplier(final String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
        if (applicationId != null) {
            assertThat((String) config.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG), startsWith(applicationId + "-"));
        } else {
            assertFalse(config.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        }
        final MockProducer<byte[], byte[]> producer = new MockProducer<>(true, BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
        producers.add(producer);
        return producer;
    }

    @Override
    public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
        return consumer;
    }

    @Override
    public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
        return restoreConsumer;
    }

}
