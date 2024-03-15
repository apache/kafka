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

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.connect.runtime.distributed.DistributedConfig.EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KafkaOffsetBackingStoreTest {
    private static final String CLIENT_ID_BASE = "test-client-id-";
    private static final String TOPIC = "connect-offsets";
    private static final short TOPIC_PARTITIONS = 2;
    private static final short TOPIC_REPLICATION_FACTOR = 5;
    private static final Map<String, String> DEFAULT_PROPS = new HashMap<>();
    static {
        DEFAULT_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        DEFAULT_PROPS.put(DistributedConfig.OFFSET_STORAGE_TOPIC_CONFIG, TOPIC);
        DEFAULT_PROPS.put(DistributedConfig.OFFSET_STORAGE_REPLICATION_FACTOR_CONFIG, Short.toString(TOPIC_REPLICATION_FACTOR));
        DEFAULT_PROPS.put(DistributedConfig.OFFSET_STORAGE_PARTITIONS_CONFIG, Integer.toString(TOPIC_PARTITIONS));
        DEFAULT_PROPS.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "connect-configs");
        DEFAULT_PROPS.put(DistributedConfig.CONFIG_STORAGE_REPLICATION_FACTOR_CONFIG, Short.toString(TOPIC_REPLICATION_FACTOR));
        DEFAULT_PROPS.put(DistributedConfig.GROUP_ID_CONFIG, "connect");
        DEFAULT_PROPS.put(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG, "status-topic");
        DEFAULT_PROPS.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_PROPS.put(DistributedConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
    }
    private static final Map<ByteBuffer, ByteBuffer> FIRST_SET = new HashMap<>();
    static {
        FIRST_SET.put(buffer("key"), buffer("value"));
        FIRST_SET.put(null, null);
    }

    private static final ByteBuffer TP0_KEY = buffer("TP0KEY");
    private static final ByteBuffer TP1_KEY = buffer("TP1KEY");
    private static final ByteBuffer TP2_KEY = buffer("TP2KEY");
    private static final ByteBuffer TP0_VALUE = buffer("VAL0");
    private static final ByteBuffer TP1_VALUE = buffer("VAL1");
    private static final ByteBuffer TP2_VALUE = buffer("VAL2");
    private static final ByteBuffer TP0_VALUE_NEW = buffer("VAL0_NEW");
    private static final ByteBuffer TP1_VALUE_NEW = buffer("VAL1_NEW");

    private Map<String, String> props = new HashMap<>(DEFAULT_PROPS);
    @Mock
    KafkaBasedLog<byte[], byte[]> storeLog;
    @Mock
    Converter keyConverter;
    private KafkaOffsetBackingStore store;

    @Captor
    private ArgumentCaptor<String> capturedTopic;
    @Captor
    private ArgumentCaptor<Map<String, Object>> capturedProducerProps;
    @Captor
    private ArgumentCaptor<Map<String, Object>> capturedConsumerProps;
    @Captor
    private ArgumentCaptor<Supplier<TopicAdmin>> capturedAdminSupplier;
    @Captor
    private ArgumentCaptor<NewTopic> capturedNewTopic;
    @Captor
    private ArgumentCaptor<Callback<ConsumerRecord<byte[], byte[]>>> capturedConsumedCallback;
    @Captor
    private ArgumentCaptor<Callback<Void>> storeLogCallbackArgumentCaptor;

    @Before
    public void setup() throws Exception {
        Supplier<TopicAdmin> adminSupplier = () -> {
            fail("Should not attempt to instantiate admin in these tests");
            // Should never be reached; only add this thrown exception to satisfy the compiler
            throw new AssertionError();
        };
        Supplier<String> clientIdBase = () -> CLIENT_ID_BASE;

        when(keyConverter.toConnectData(any(), any())).thenReturn(new SchemaAndValue(null,
                Arrays.asList("connector", Collections.singletonMap("partitionKey", "dummy"))));
        store = spy(new KafkaOffsetBackingStore(adminSupplier, clientIdBase, keyConverter));

        doReturn(storeLog).when(store).createKafkaBasedLog(capturedTopic.capture(), capturedProducerProps.capture(),
                capturedConsumerProps.capture(), capturedConsumedCallback.capture(),
                capturedNewTopic.capture(), capturedAdminSupplier.capture(), any(), any());
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(storeLog);
    }

    private DistributedConfig mockConfig(Map<String, String> props) {
        DistributedConfig config = spy(new DistributedConfig(props));
        doReturn("test-cluster").when(config).kafkaClusterId();
        return config;
    }

    @Test
    public void testStartStop() {
        props.put("offset.storage.min.insync.replicas", "3");
        props.put("offset.storage.max.message.bytes", "1001");

        store.configure(mockConfig(props));
        store.start();

        verify(storeLog).start();

        assertEquals(TOPIC, capturedTopic.getValue());
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", capturedProducerProps.getValue().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", capturedProducerProps.getValue().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        assertEquals(TOPIC, capturedNewTopic.getValue().name());
        assertEquals(TOPIC_PARTITIONS, capturedNewTopic.getValue().numPartitions());
        assertEquals(TOPIC_REPLICATION_FACTOR, capturedNewTopic.getValue().replicationFactor());

        store.stop();

        verify(storeLog).stop();
    }

    @Test
    public void testReloadOnStart() {
        doAnswer(invocation -> {
            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, TP0_KEY.array(), TP0_VALUE.array(),
                    new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0, 0, TP1_KEY.array(), TP1_VALUE.array(),
                    new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TP0_KEY.array(), TP0_VALUE_NEW.array(),
                    new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TP1_KEY.array(), TP1_VALUE_NEW.array(),
                    new RecordHeaders(), Optional.empty()));
            return null;
        }).when(storeLog).start();

        store.configure(mockConfig(props));
        store.start();

        HashMap<ByteBuffer, ByteBuffer> data = store.data;
        assertEquals(TP0_VALUE_NEW, data.get(TP0_KEY));
        assertEquals(TP1_VALUE_NEW, data.get(TP1_KEY));

        store.stop();

        verify(storeLog).stop();
    }

    @Test
    public void testGetSet() throws Exception {
        store.configure(mockConfig(props));
        store.start();

        verify(storeLog).start();

        doAnswer(invocation -> {
            // First get() against an empty store
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        // Getting from empty store should return nulls
        Map<ByteBuffer, ByteBuffer> offsets = store.get(Arrays.asList(TP0_KEY, TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        // Since we didn't read them yet, these will be null
        assertNull(offsets.get(TP0_KEY));
        assertNull(offsets.get(TP1_KEY));
        // Set some offsets
        Map<ByteBuffer, ByteBuffer> toSet = new HashMap<>();
        toSet.put(TP0_KEY, TP0_VALUE);
        toSet.put(TP1_KEY, TP1_VALUE);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        Future<Void> setFuture = store.set(toSet, (error, result) -> invoked.set(true));
        assertFalse(setFuture.isDone());
        // Set offsets
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback0 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP0_KEY.array()), aryEq(TP0_VALUE.array()), callback0.capture());
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback1 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP1_KEY.array()), aryEq(TP1_VALUE.array()), callback1.capture());
        // Out of order callbacks shouldn't matter, should still require all to be invoked before invoking the callback
        // for the store's set callback
        callback1.getValue().onCompletion(null, null);
        assertFalse(invoked.get());
        callback0.getValue().onCompletion(null, null);
        setFuture.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(invoked.get());

        doAnswer(invocation -> {
            // Second get() should get the produced data and return the new values
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, TP0_KEY.array(), TP0_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0, 0, TP1_KEY.array(), TP1_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        // Getting data should read to end of our published data and return it
        offsets = store.get(Arrays.asList(TP0_KEY, TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        assertEquals(TP0_VALUE, offsets.get(TP0_KEY));
        assertEquals(TP1_VALUE, offsets.get(TP1_KEY));

        doAnswer(invocation -> {
            // Third get() should pick up data produced by someone else and return those values
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TP0_KEY.array(), TP0_VALUE_NEW.array(),
                            new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 1, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TP1_KEY.array(), TP1_VALUE_NEW.array(),
                            new RecordHeaders(), Optional.empty()));
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        // Getting data should read to end of our published data and return it
        offsets = store.get(Arrays.asList(TP0_KEY, TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        assertEquals(TP0_VALUE_NEW, offsets.get(TP0_KEY));
        assertEquals(TP1_VALUE_NEW, offsets.get(TP1_KEY));

        store.stop();

        verify(storeLog).stop();
    }

    @Test
    public void testGetSetNull() throws Exception {
        // Second get() should get the produced data and return the new values
        doAnswer(invocation -> {
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0, null, TP0_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0, 0, TP1_KEY.array(), TP1_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        store.configure(mockConfig(props));
        store.start();

        verify(storeLog).start();

        // Set some offsets
        Map<ByteBuffer, ByteBuffer> toSet = new HashMap<>();
        toSet.put(null, TP0_VALUE);
        toSet.put(TP1_KEY, TP1_VALUE);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        Future<Void> setFuture = store.set(toSet, (error, result) -> invoked.set(true));
        assertFalse(setFuture.isDone());

        // Set offsets
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback0 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(isNull(), aryEq(TP0_VALUE.array()), callback0.capture());
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback1 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP1_KEY.array()), aryEq(TP1_VALUE.array()), callback1.capture());
        // Out of order callbacks shouldn't matter, should still require all to be invoked before invoking the callback
        // for the store's set callback
        callback1.getValue().onCompletion(null, null);
        assertFalse(invoked.get());
        callback0.getValue().onCompletion(null, null);
        setFuture.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(invoked.get());
        // Getting data should read to end of our published data and return it
        Map<ByteBuffer, ByteBuffer> offsets = store.get(Arrays.asList(null, TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        assertEquals(TP0_VALUE, offsets.get(null));
        assertEquals(TP1_VALUE, offsets.get(TP1_KEY));

        // set null offset for TP1_KEY
        toSet.clear();
        toSet.put(TP1_KEY, null);
        invoked.set(false);
        setFuture = store.set(toSet, (error, result) -> invoked.set(true));
        assertFalse(setFuture.isDone());

        callback0 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP1_KEY.array()), isNull(), callback0.capture());
        callback0.getValue().onCompletion(null, null);
        assertTrue(invoked.get());

        doAnswer(invocation -> {
            // Read null offset for TP1_KEY (tombstone message)
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0, TP1_KEY.array(), null,
                            new RecordHeaders(), Optional.empty()));
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        // Getting data should read to end of our published data and return it
        offsets = store.get(Collections.singletonList(TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        assertNull(offsets.get(TP1_KEY));

        // Just verifying that KafkaOffsetBackingStore::get returns null isn't enough, we also need to verify that the mapping for the source partition key is removed.
        // This is because KafkaOffsetBackingStore::get returns null if either there is no existing offset for the source partition or if there is an offset with null value.
        // We need to make sure that tombstoned offsets are removed completely (i.e. that the mapping for the corresponding source partition is removed).
        assertFalse(store.data.containsKey(TP1_KEY));

        store.stop();

        verify(storeLog).stop();
    }

    @Test
    public void testSetFailure() {
        store.configure(mockConfig(props));
        store.start();

        verify(storeLog).start();

        // Set some offsets
        Map<ByteBuffer, ByteBuffer> toSet = new HashMap<>();
        toSet.put(TP0_KEY, TP0_VALUE);
        toSet.put(TP1_KEY, TP1_VALUE);
        toSet.put(TP2_KEY, TP2_VALUE);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final AtomicBoolean invokedFailure = new AtomicBoolean(false);
        Future<Void> setFuture = store.set(toSet, (error, result) -> {
            invoked.set(true);
            if (error != null)
                invokedFailure.set(true);
        });
        assertFalse(setFuture.isDone());
        // Set offsets
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback0 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP0_KEY.array()), aryEq(TP0_VALUE.array()), callback0.capture());
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback1 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP1_KEY.array()), aryEq(TP1_VALUE.array()), callback1.capture());
        ArgumentCaptor<org.apache.kafka.clients.producer.Callback> callback2 = ArgumentCaptor.forClass(org.apache.kafka.clients.producer.Callback.class);
        verify(storeLog).send(aryEq(TP2_KEY.array()), aryEq(TP2_VALUE.array()), callback2.capture());
        // Out of order callbacks shouldn't matter, should still require all to be invoked before invoking the callback
        // for the store's set callback
        callback1.getValue().onCompletion(null, null);
        assertFalse(invoked.get());
        callback2.getValue().onCompletion(null, new KafkaException("bogus error"));
        assertTrue(invoked.get());
        assertTrue(invokedFailure.get());
        callback0.getValue().onCompletion(null, null);
        ExecutionException e = assertThrows(ExecutionException.class, () -> setFuture.get(10000, TimeUnit.MILLISECONDS));
        assertNotNull(e.getCause());
        assertTrue(e.getCause() instanceof KafkaException);

        store.stop();

        verify(storeLog).stop();
    }

    @Test
    public void testConsumerPropertiesInsertedByDefaultWithExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        props.remove(ISOLATION_LEVEL_CONFIG);

        store.configure(mockConfig(props));

        assertEquals(
                IsolationLevel.READ_COMMITTED.toString(),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );
    }

    @Test
    public void testConsumerPropertiesOverrideUserSuppliedValuesWithExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "enabled");
        props.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString());

        store.configure(mockConfig(props));

        assertEquals(
                IsolationLevel.READ_COMMITTED.toString(),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );
    }

    @Test
    public void testConsumerPropertiesNotInsertedByDefaultWithoutExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "disabled");
        props.remove(ISOLATION_LEVEL_CONFIG);

        store.configure(mockConfig(props));

        assertNull(capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG));
    }

    @Test
    public void testConsumerPropertiesDoNotOverrideUserSuppliedValuesWithoutExactlyOnceSourceEnabled() {
        props.put(EXACTLY_ONCE_SOURCE_SUPPORT_CONFIG, "disabled");
        props.put(ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_UNCOMMITTED.toString());

        store.configure(mockConfig(props));

        assertEquals(
                IsolationLevel.READ_UNCOMMITTED.toString(),
                capturedConsumerProps.getValue().get(ISOLATION_LEVEL_CONFIG)
        );

    }

    @Test
    public void testClientIds() {
        store.configure(mockConfig(props));

        final String expectedClientId = CLIENT_ID_BASE + "offsets";
        assertEquals(expectedClientId, capturedProducerProps.getValue().get(CLIENT_ID_CONFIG));
        assertEquals(expectedClientId, capturedConsumerProps.getValue().get(CLIENT_ID_CONFIG));
    }

    @Test
    public void testConnectorPartitions() throws Exception {
        JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"), true);
        store = spy(new KafkaOffsetBackingStore(() -> {
            fail("Should not attempt to instantiate admin in these tests");
            return null;
        }, () -> CLIENT_ID_BASE, jsonConverter));

        doReturn(storeLog).when(store).createKafkaBasedLog(capturedTopic.capture(), capturedProducerProps.capture(),
                capturedConsumerProps.capture(), capturedConsumedCallback.capture(),
                capturedNewTopic.capture(), capturedAdminSupplier.capture(), any(), any());

        store.configure(mockConfig(props));
        store.start();

        verify(storeLog).start();

        doAnswer(invocation -> {
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0, 0,
                            jsonConverter.fromConnectData("", null, Arrays.asList("connector1",
                                    Collections.singletonMap("partitionKey", "partitionValue1"))), TP0_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0, 0,
                            jsonConverter.fromConnectData("", null, Arrays.asList("connector1",
                                    Collections.singletonMap("partitionKey", "partitionValue1"))), TP1_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0, 0,
                            jsonConverter.fromConnectData("", null, Arrays.asList("connector1",
                                    Collections.singletonMap("partitionKey", "partitionValue2"))), TP2_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 3, 0L, TimestampType.CREATE_TIME, 0, 0,
                            jsonConverter.fromConnectData("", null, Arrays.asList("connector2",
                                    Collections.singletonMap("partitionKey", "partitionValue"))), TP1_VALUE.array(),
                            new RecordHeaders(), Optional.empty()));
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        // Trigger a read to the end of the log
        store.get(Collections.emptyList()).get(10000, TimeUnit.MILLISECONDS);

        Set<Map<String, Object>> connectorPartitions1 = store.connectorPartitions("connector1");
        Set<Map<String, Object>> expectedConnectorPartition1 = new HashSet<>();
        expectedConnectorPartition1.add(Collections.singletonMap("partitionKey", "partitionValue1"));
        expectedConnectorPartition1.add(Collections.singletonMap("partitionKey", "partitionValue2"));
        assertEquals(expectedConnectorPartition1, connectorPartitions1);

        Set<Map<String, Object>> connectorPartitions2 = store.connectorPartitions("connector2");
        Set<Map<String, Object>> expectedConnectorPartition2 = Collections.singleton(Collections.singletonMap("partitionKey", "partitionValue"));
        assertEquals(expectedConnectorPartition2, connectorPartitions2);

        doAnswer(invocation -> {
            capturedConsumedCallback.getValue().onCompletion(null,
                    new ConsumerRecord<>(TOPIC, 0, 4, 0L, TimestampType.CREATE_TIME, 0, 0,
                            jsonConverter.fromConnectData("", null, Arrays.asList("connector1",
                                    Collections.singletonMap("partitionKey", "partitionValue1"))), null,
                            new RecordHeaders(), Optional.empty()));
            storeLogCallbackArgumentCaptor.getValue().onCompletion(null, null);
            return null;
        }).when(storeLog).readToEnd(storeLogCallbackArgumentCaptor.capture());

        // Trigger a read to the end of the log
        store.get(Collections.emptyList()).get(10000, TimeUnit.MILLISECONDS);

        // Null valued offset for a partition key should remove that partition for the connector
        connectorPartitions1 = store.connectorPartitions("connector1");
        assertEquals(Collections.singleton(Collections.singletonMap("partitionKey", "partitionValue2")), connectorPartitions1);

        store.stop();
        verify(storeLog).stop();
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

}
