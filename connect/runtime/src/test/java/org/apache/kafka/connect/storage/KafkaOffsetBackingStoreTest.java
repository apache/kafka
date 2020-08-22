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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.ConnectUtils;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaOffsetBackingStore.class, ConnectUtils.class})
@PowerMockIgnore({"javax.management.*", "javax.crypto.*"})
@SuppressWarnings({"unchecked", "deprecation"})
public class KafkaOffsetBackingStoreTest {
    private static final String TOPIC = "connect-offsets";
    private static final short TOPIC_PARTITIONS = 2;
    private static final short TOPIC_REPLICATION_FACTOR = 5;
    private static final Map<String, String> DEFAULT_PROPS = new HashMap<>();
    private static final DistributedConfig DEFAULT_DISTRIBUTED_CONFIG;
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
        DEFAULT_PROPS.put(DistributedConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_PROPS.put(DistributedConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        DEFAULT_DISTRIBUTED_CONFIG = new DistributedConfig(DEFAULT_PROPS);

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

    @Mock
    KafkaBasedLog<byte[], byte[]> storeLog;
    private KafkaOffsetBackingStore store;

    private Capture<String> capturedTopic = EasyMock.newCapture();
    private Capture<Map<String, Object>> capturedProducerProps = EasyMock.newCapture();
    private Capture<Map<String, Object>> capturedConsumerProps = EasyMock.newCapture();
    private Capture<Map<String, Object>> capturedAdminProps = EasyMock.newCapture();
    private Capture<NewTopic> capturedNewTopic = EasyMock.newCapture();
    private Capture<Callback<ConsumerRecord<byte[], byte[]>>> capturedConsumedCallback = EasyMock.newCapture();

    @Before
    public void setUp() throws Exception {
        store = PowerMock.createPartialMockAndInvokeDefaultConstructor(KafkaOffsetBackingStore.class, "createKafkaBasedLog");
    }

    @Test
    public void testStartStop() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList());
        expectStop();
        expectClusterId();

        PowerMock.replayAll();

        Map<String, String> settings = new HashMap<>(DEFAULT_PROPS);
        settings.put("offset.storage.min.insync.replicas", "3");
        settings.put("offset.storage.max.message.bytes", "1001");
        store.configure(new DistributedConfig(settings));
        assertEquals(TOPIC, capturedTopic.getValue());
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", capturedProducerProps.getValue().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", capturedProducerProps.getValue().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", capturedConsumerProps.getValue().get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));

        assertEquals(TOPIC, capturedNewTopic.getValue().name());
        assertEquals(TOPIC_PARTITIONS, capturedNewTopic.getValue().numPartitions());
        assertEquals(TOPIC_REPLICATION_FACTOR, capturedNewTopic.getValue().replicationFactor());

        store.start();
        store.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testReloadOnStart() throws Exception {
        expectConfigure();
        expectStart(Arrays.asList(
                new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY.array(), TP0_VALUE.array()),
                new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY.array(), TP1_VALUE.array()),
                new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY.array(), TP0_VALUE_NEW.array()),
                new ConsumerRecord<>(TOPIC, 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY.array(), TP1_VALUE_NEW.array())
        ));
        expectStop();
        expectClusterId();

        PowerMock.replayAll();

        store.configure(DEFAULT_DISTRIBUTED_CONFIG);
        store.start();
        HashMap<ByteBuffer, ByteBuffer> data = Whitebox.getInternalState(store, "data");
        assertEquals(TP0_VALUE_NEW, data.get(TP0_KEY));
        assertEquals(TP1_VALUE_NEW, data.get(TP1_KEY));

        store.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testGetSet() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList());
        expectStop();

        // First get() against an empty store
        final Capture<Callback<Void>> firstGetReadToEndCallback = EasyMock.newCapture();
        storeLog.readToEnd(EasyMock.capture(firstGetReadToEndCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            firstGetReadToEndCallback.getValue().onCompletion(null, null);
            return null;
        });

        // Set offsets
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        storeLog.send(EasyMock.aryEq(TP0_KEY.array()), EasyMock.aryEq(TP0_VALUE.array()), EasyMock.capture(callback0));
        PowerMock.expectLastCall();
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        storeLog.send(EasyMock.aryEq(TP1_KEY.array()), EasyMock.aryEq(TP1_VALUE.array()), EasyMock.capture(callback1));
        PowerMock.expectLastCall();

        // Second get() should get the produced data and return the new values
        final Capture<Callback<Void>> secondGetReadToEndCallback = EasyMock.newCapture();
        storeLog.readToEnd(EasyMock.capture(secondGetReadToEndCallback));
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY.array(), TP0_VALUE.array()));
                capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY.array(), TP1_VALUE.array()));
                secondGetReadToEndCallback.getValue().onCompletion(null, null);
                return null;
            }
        });

        // Third get() should pick up data produced by someone else and return those values
        final Capture<Callback<Void>> thirdGetReadToEndCallback = EasyMock.newCapture();
        storeLog.readToEnd(EasyMock.capture(thirdGetReadToEndCallback));
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY.array(), TP0_VALUE_NEW.array()));
                capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY.array(), TP1_VALUE_NEW.array()));
                thirdGetReadToEndCallback.getValue().onCompletion(null, null);
                return null;
            }
        });

        expectClusterId();
        PowerMock.replayAll();

        store.configure(DEFAULT_DISTRIBUTED_CONFIG);
        store.start();

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
        Future<Void> setFuture = store.set(toSet, new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                invoked.set(true);
            }
        });
        assertFalse(setFuture.isDone());
        // Out of order callbacks shouldn't matter, should still require all to be invoked before invoking the callback
        // for the store's set callback
        callback1.getValue().onCompletion(null, null);
        assertFalse(invoked.get());
        callback0.getValue().onCompletion(null, null);
        setFuture.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(invoked.get());

        // Getting data should read to end of our published data and return it
        offsets = store.get(Arrays.asList(TP0_KEY, TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        assertEquals(TP0_VALUE, offsets.get(TP0_KEY));
        assertEquals(TP1_VALUE, offsets.get(TP1_KEY));

        // Getting data should read to end of our published data and return it
        offsets = store.get(Arrays.asList(TP0_KEY, TP1_KEY)).get(10000, TimeUnit.MILLISECONDS);
        assertEquals(TP0_VALUE_NEW, offsets.get(TP0_KEY));
        assertEquals(TP1_VALUE_NEW, offsets.get(TP1_KEY));

        store.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testGetSetNull() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList());

        // Set offsets
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        storeLog.send(EasyMock.isNull(byte[].class), EasyMock.aryEq(TP0_VALUE.array()), EasyMock.capture(callback0));
        PowerMock.expectLastCall();
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        storeLog.send(EasyMock.aryEq(TP1_KEY.array()), EasyMock.isNull(byte[].class), EasyMock.capture(callback1));
        PowerMock.expectLastCall();

        // Second get() should get the produced data and return the new values
        final Capture<Callback<Void>> secondGetReadToEndCallback = EasyMock.newCapture();
        storeLog.readToEnd(EasyMock.capture(secondGetReadToEndCallback));
        PowerMock.expectLastCall().andAnswer(() -> {
            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, (byte[]) null, TP0_VALUE.array()));
            capturedConsumedCallback.getValue().onCompletion(null, new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY.array(), (byte[]) null));
            secondGetReadToEndCallback.getValue().onCompletion(null, null);
            return null;
        });

        expectStop();
        expectClusterId();

        PowerMock.replayAll();

        store.configure(DEFAULT_DISTRIBUTED_CONFIG);
        store.start();

        // Set offsets using null keys and values
        Map<ByteBuffer, ByteBuffer> toSet = new HashMap<>();
        toSet.put(null, TP0_VALUE);
        toSet.put(TP1_KEY, null);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        Future<Void> setFuture = store.set(toSet, new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                invoked.set(true);
            }
        });
        assertFalse(setFuture.isDone());
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
        assertNull(offsets.get(TP1_KEY));

        store.stop();

        PowerMock.verifyAll();
    }

    @Test
    public void testSetFailure() throws Exception {
        expectConfigure();
        expectStart(Collections.emptyList());
        expectStop();

        // Set offsets
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        storeLog.send(EasyMock.aryEq(TP0_KEY.array()), EasyMock.aryEq(TP0_VALUE.array()), EasyMock.capture(callback0));
        PowerMock.expectLastCall();
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        storeLog.send(EasyMock.aryEq(TP1_KEY.array()), EasyMock.aryEq(TP1_VALUE.array()), EasyMock.capture(callback1));
        PowerMock.expectLastCall();
        Capture<org.apache.kafka.clients.producer.Callback> callback2 = EasyMock.newCapture();
        storeLog.send(EasyMock.aryEq(TP2_KEY.array()), EasyMock.aryEq(TP2_VALUE.array()), EasyMock.capture(callback2));
        PowerMock.expectLastCall();

        expectClusterId();

        PowerMock.replayAll();

        store.configure(DEFAULT_DISTRIBUTED_CONFIG);
        store.start();

        // Set some offsets
        Map<ByteBuffer, ByteBuffer> toSet = new HashMap<>();
        toSet.put(TP0_KEY, TP0_VALUE);
        toSet.put(TP1_KEY, TP1_VALUE);
        toSet.put(TP2_KEY, TP2_VALUE);
        final AtomicBoolean invoked = new AtomicBoolean(false);
        final AtomicBoolean invokedFailure = new AtomicBoolean(false);
        Future<Void> setFuture = store.set(toSet, new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                invoked.set(true);
                if (error != null)
                    invokedFailure.set(true);
            }
        });
        assertFalse(setFuture.isDone());
        // Out of order callbacks shouldn't matter, should still require all to be invoked before invoking the callback
        // for the store's set callback
        callback1.getValue().onCompletion(null, null);
        assertFalse(invoked.get());
        callback2.getValue().onCompletion(null, new KafkaException("bogus error"));
        assertTrue(invoked.get());
        assertTrue(invokedFailure.get());
        callback0.getValue().onCompletion(null, null);
        try {
            setFuture.get(10000, TimeUnit.MILLISECONDS);
            fail("Should have seen KafkaException thrown when waiting on KafkaOffsetBackingStore.set() future");
        } catch (ExecutionException e) {
            // expected
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof KafkaException);
        }

        store.stop();

        PowerMock.verifyAll();
    }

    private void expectConfigure() throws Exception {
        PowerMock.expectPrivate(store, "createKafkaBasedLog", EasyMock.capture(capturedTopic), EasyMock.capture(capturedProducerProps),
                EasyMock.capture(capturedConsumerProps), EasyMock.capture(capturedConsumedCallback),
                EasyMock.capture(capturedNewTopic), EasyMock.capture(capturedAdminProps))
                .andReturn(storeLog);
    }

    private void expectStart(final List<ConsumerRecord<byte[], byte[]>> preexistingRecords) throws Exception {
        storeLog.start();
        PowerMock.expectLastCall().andAnswer(new IAnswer<Object>() {
            @Override
            public Object answer() throws Throwable {
                for (ConsumerRecord<byte[], byte[]> rec : preexistingRecords)
                    capturedConsumedCallback.getValue().onCompletion(null, rec);
                return null;
            }
        });
    }

    private void expectStop() {
        storeLog.stop();
        PowerMock.expectLastCall();
    }

    private void expectClusterId() {
        PowerMock.mockStaticPartial(ConnectUtils.class, "lookupKafkaClusterId");
        EasyMock.expect(ConnectUtils.lookupKafkaClusterId(EasyMock.anyObject())).andReturn("test-cluster").anyTimes();
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

}
