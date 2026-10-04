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
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.SourceConnectorConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.TopicAdmin;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class KafkaOffsetBackingStoreGetTopicConfigTest {

    @Test
    public void testWorkerLevelStoreReturnsCorrectTopicConfig() {
        JsonConverter keyConverter = new JsonConverter();
        keyConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
        KafkaOffsetBackingStore workerStore = new KafkaOffsetBackingStore(
                () -> mock(TopicAdmin.class),
                () -> "test-client-",
                keyConverter
        );
        assertEquals(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, workerStore.getTopicConfig(),
                "Worker-level offset store should reference 'offset.storage.topic'");
    }

    @Test
    public void testConnectorSpecificReadWriteStoreReturnsCorrectTopicConfig() {
        String connectorOffsetTopic = "my-connector-offsets";
        Producer<byte[], byte[]> producer = new MockProducer<>(Cluster.empty(), false, null, new ByteArraySerializer(), new ByteArraySerializer());
        Consumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        TopicAdmin topicAdmin = mock(TopicAdmin.class);
        JsonConverter keyConverter = new JsonConverter();
        keyConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);

        KafkaOffsetBackingStore connectorStore = KafkaOffsetBackingStore.readWriteStore(
                connectorOffsetTopic, producer, consumer, topicAdmin, keyConverter);

        assertEquals(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, connectorStore.getTopicConfig(),
                "Connector-specific store should return 'offsets.storage.topic', not 'offset.storage.topic'");
    }

    @Test
    public void testConnectorSpecificReadOnlyStoreReturnsCorrectTopicConfig() {
        String connectorOffsetTopic = "my-connector-offsets";
        Consumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
        TopicAdmin topicAdmin = mock(TopicAdmin.class);
        JsonConverter keyConverter = new JsonConverter();
        keyConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);

        KafkaOffsetBackingStore connectorStore = KafkaOffsetBackingStore.readOnlyStore(
                connectorOffsetTopic, consumer, topicAdmin, keyConverter);

        assertEquals(SourceConnectorConfig.OFFSETS_TOPIC_CONFIG, connectorStore.getTopicConfig(),
                "Connector-specific store should return 'offsets.storage.topic', not 'offset.storage.topic'");
    }
}
