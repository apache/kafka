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
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.LoggingContext;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class ConnectorOffsetBackingStoreTest {

    private static final String NAMESPACE = "namespace";
    // Connect format - any types should be accepted here
    private static final Map<String, Object> OFFSET_KEY = Collections.singletonMap("key", "key");
    private static final Map<String, Object> OFFSET_VALUE = Collections.singletonMap("key", 12);

    // Serialized
    private static final byte[] OFFSET_KEY_SERIALIZED = "key-serialized".getBytes();
    private static final byte[] OFFSET_VALUE_SERIALIZED = "value-serialized".getBytes();

    private static final Exception PRODUCE_EXCEPTION = new KafkaException();

    @Mock
    private Converter keyConverter;
    @Mock
    private Converter valueConverter;


    @Test
    public void testFlushFailureWhenWriteToSecondaryStoreFailsForTombstoneOffsets() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));


        try {
            offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
                assertEquals(PRODUCE_EXCEPTION, error);
                assertNull(result);
            }).get(1000L, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            assertEquals(PRODUCE_EXCEPTION, e.getCause());
        }
    }

    @Test
    public void testFlushSuccessWhenWritesSucceedToBothPrimaryAndSecondaryStoresForTombstoneOffsets() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushSuccessWhenWriteToSecondaryStoreFailsForNonTombstoneOffsets() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryAndSecondaryStoreSucceeds() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceeds() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", true);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
            () -> LoggingContext.forConnector("source-connector"),
            workerStore,
            connectorStore,
            "offsets-topic",
            mock(TopicAdmin.class));

        try {
            offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
                assertEquals(PRODUCE_EXCEPTION, error);
                assertNull(result);
            }).get(1000L, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            assertEquals(PRODUCE_EXCEPTION, e.getCause());
        }
    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsAndSecondarySucceedsForTombstoneRecords() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", true);
        KafkaOffsetBackingStore workerStore = setupOffsetBackingStoreWithProducer("topic2", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withConnectorAndWorkerStores(
                () -> LoggingContext.forConnector("source-connector"),
                workerStore,
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        try {
            offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, null, null), (error, result) -> {
                assertEquals(PRODUCE_EXCEPTION, error);
                assertNull(result);
            }).get(1000L, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            assertEquals(PRODUCE_EXCEPTION, e.getCause());
        }
    }

    @Test
    public void testFlushSuccessWhenWritesToPrimaryStoreSucceedsWithNoSecondaryStore() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", false);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
            assertNull(error);
            assertNull(result);
        }).get(1000L, TimeUnit.MILLISECONDS);

    }

    @Test
    public void testFlushFailureWhenWritesToPrimaryStoreFailsWithNoSecondaryStore() throws Exception {

        KafkaOffsetBackingStore connectorStore = setupOffsetBackingStoreWithProducer("topic1", true);

        ConnectorOffsetBackingStore offsetBackingStore = ConnectorOffsetBackingStore.withOnlyConnectorStore(
                () -> LoggingContext.forConnector("source-connector"),
                connectorStore,
                "offsets-topic",
                mock(TopicAdmin.class));

        try {
            offsetBackingStore.set(getSerialisedOffsets(OFFSET_KEY, OFFSET_KEY_SERIALIZED, OFFSET_VALUE, OFFSET_VALUE_SERIALIZED), (error, result) -> {
                assertEquals(PRODUCE_EXCEPTION, error);
                assertNull(result);
            }).get(1000L, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            assertEquals(PRODUCE_EXCEPTION, e.getCause());
        }
    }

    @SuppressWarnings("unchecked")
    private KafkaOffsetBackingStore setupOffsetBackingStoreWithProducer(String topic, boolean throwingProducer) {
        KafkaOffsetBackingStore offsetBackingStore = new KafkaOffsetBackingStore(() -> mock(TopicAdmin.class), () -> "connect",  mock(Converter.class));
        KafkaBasedLog<byte[], byte[]> kafkaBasedLog = new KafkaBasedLog<byte[], byte[]>(
                topic, new HashMap<>(), new HashMap<>(),
                () -> mock(TopicAdmin.class), mock(Callback.class), new MockTime(), null) {
            @Override
            protected Producer<byte[], byte[]> createProducer() {
                return createMockProducer(throwingProducer);
            }

            @Override
            protected Consumer<byte[], byte[]> createConsumer() {
                return createMockConsumer(topic);
            }
        };
        kafkaBasedLog.start();
        offsetBackingStore.offsetLog = kafkaBasedLog;
        return offsetBackingStore;
    }

    private MockConsumer<byte[], byte[]> createMockConsumer(String topic) {
        MockConsumer<byte[], byte[]> consumer = new MockConsumer<>(OffsetResetStrategy.LATEST);
        Node noNode = Node.noNode();
        Node[] nodes = new Node[]{noNode};
        consumer.updatePartitions(topic, Collections.singletonList(new PartitionInfo(topic, 0, noNode, nodes, nodes)));
        consumer.updateBeginningOffsets(mkMap(mkEntry(new TopicPartition(topic, 0), 100L)));
        return consumer;
    }

    private Producer<byte[], byte[]> createMockProducer(boolean throwingProducer) {
        return new MockProducer<byte[], byte[]>() {
            @Override
            public synchronized Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record, final org.apache.kafka.clients.producer.Callback callback) {
                if (throwingProducer) {
                    callback.onCompletion(null, PRODUCE_EXCEPTION);
                } else {
                    callback.onCompletion(null, null);
                }
                return null;
            }
        };
    }

    private Map<ByteBuffer, ByteBuffer> getSerialisedOffsets(Map<String, Object> key, byte[] keySerialized,
                                                             Map<String, Object> value, byte[] valueSerialized) {
        List<Object> keyWrapped = Arrays.asList(NAMESPACE, key);
        when(keyConverter.fromConnectData(NAMESPACE, null, keyWrapped)).thenReturn(keySerialized);
        when(valueConverter.fromConnectData(NAMESPACE, null, value)).thenReturn(valueSerialized);

        return Collections.singletonMap(
            keySerialized == null ? null : ByteBuffer.wrap(keySerialized),
            valueSerialized == null ? null : ByteBuffer.wrap(valueSerialized));
    }
}
