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
 **/

package org.apache.kafka.copycat.storage;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.copycat.errors.CopycatException;
import org.apache.kafka.copycat.util.Callback;
import org.apache.kafka.copycat.util.TestFuture;
import org.easymock.Capture;
import org.easymock.EasyMock;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.apache.kafka.copycat.util.ByteArrayProducerRecordEquals.eqProducerRecord;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaOffsetBackingStore.class)
@PowerMockIgnore("javax.management.*")
public class KafkaOffsetBackingStoreTest {
    private static final String TOPIC = "copycat-offsets";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
    private static final Map<String, String> DEFAULT_PROPS = new HashMap<>();
    static {
        DEFAULT_PROPS.put(KafkaOffsetBackingStore.OFFSET_STORAGE_TOPIC_CONFIG, TOPIC);
        DEFAULT_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
    }
    private static final Set<TopicPartition> CONSUMER_ASSIGNMENT = new HashSet<>(Arrays.asList(TP0, TP1));
    private static final Map<ByteBuffer, ByteBuffer> FIRST_SET = new HashMap<>();
    static {
        FIRST_SET.put(buffer("key"), buffer("value"));
        FIRST_SET.put(null, null);
    }


    private static final Node LEADER = new Node(1, "broker1", 9092);
    private static final Node REPLICA = new Node(1, "broker2", 9093);

    private static final PartitionInfo TPINFO0 = new PartitionInfo(TOPIC, 0, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});
    private static final PartitionInfo TPINFO1 = new PartitionInfo(TOPIC, 1, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});

    private static final ByteBuffer TP0_KEY = buffer("TP0KEY");
    private static final ByteBuffer TP1_KEY = buffer("TP1KEY");
    private static final ByteBuffer TP0_VALUE = buffer("VAL0");
    private static final ByteBuffer TP1_VALUE = buffer("VAL1");
    private static final ByteBuffer TP0_VALUE_NEW = buffer("VAL0_NEW");
    private static final ByteBuffer TP1_VALUE_NEW = buffer("VAL1_NEW");

    private KafkaOffsetBackingStore store;

    @Mock private KafkaProducer<byte[], byte[]> producer;
    private MockConsumer<byte[], byte[]> consumer;

    @Before
    public void setUp() throws Exception {
        store = PowerMock.createPartialMockAndInvokeDefaultConstructor(KafkaOffsetBackingStore.class, new String[]{"createConsumer", "createProducer"});
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Arrays.asList(TPINFO0, TPINFO1));
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(TP0, 0L);
        beginningOffsets.put(TP1, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
    }

    @Test(expected = CopycatException.class)
    public void testMissingTopic() {
        store = new KafkaOffsetBackingStore();
        store.configure(Collections.<String, Object>emptyMap());
    }

    @Test
    public void testStartStop() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        store.configure(DEFAULT_PROPS);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        store.start();
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testReloadOnStart() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        store.configure(DEFAULT_PROPS);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 1L);
        endOffsets.put(TP1, 1L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Needs to seek to end to find end offsets
                consumer.waitForPoll(10000);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, TP0_KEY.array(), TP0_VALUE.array()));
                    }
                }, 10000);

                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, TP1_KEY.array(), TP1_VALUE.array()));
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        HashMap<ByteBuffer, ByteBuffer> data = Whitebox.getInternalState(store, "data");
        assertEquals(TP0_VALUE, data.get(TP0_KEY));
        assertEquals(TP1_VALUE, data.get(TP1_KEY));

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testGetSet() throws Exception {
        expectStart();
        TestFuture<RecordMetadata> tp0Future = new TestFuture<>();
        ProducerRecord<byte[], byte[]> tp0Record = new ProducerRecord<>(TOPIC, TP0_KEY.array(), TP0_VALUE.array());
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        EasyMock.expect(producer.send(eqProducerRecord(tp0Record), EasyMock.capture(callback1))).andReturn(tp0Future);
        TestFuture<RecordMetadata> tp1Future = new TestFuture<>();
        ProducerRecord<byte[], byte[]> tp1Record = new ProducerRecord<>(TOPIC, TP1_KEY.array(), TP1_VALUE.array());
        Capture<org.apache.kafka.clients.producer.Callback> callback2 = EasyMock.newCapture();
        EasyMock.expect(producer.send(eqProducerRecord(tp1Record), EasyMock.capture(callback2))).andReturn(tp1Future);

        expectStop();

        PowerMock.replayAll();

        store.configure(DEFAULT_PROPS);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Should keep polling until it has partition info
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.seek(TP0, 0);
                        consumer.seek(TP1, 0);
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

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
        tp1Future.resolve((RecordMetadata) null); // Output not used, so safe to not return a real value for testing
        assertFalse(setFuture.isDone());
        tp0Future.resolve((RecordMetadata) null);
        // Out of order callbacks shouldn't matter, should still require all to be invoked before invoking the callback
        // for the store's set callback
        callback2.getValue().onCompletion(null, null);
        assertFalse(invoked.get());
        callback1.getValue().onCompletion(null, null);
        setFuture.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(invoked.get());

        // Getting data should continue to return old data...
        final AtomicBoolean getInvokedAndPassed = new AtomicBoolean(false);
        store.get(Arrays.asList(TP0_KEY, TP1_KEY), new Callback<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public void onCompletion(Throwable error, Map<ByteBuffer, ByteBuffer> result) {
                // Since we didn't read them yet, these will be null
                assertEquals(null, result.get(TP0_KEY));
                assertEquals(null, result.get(TP1_KEY));
                getInvokedAndPassed.set(true);
            }
        }).get(10000, TimeUnit.MILLISECONDS);
        assertTrue(getInvokedAndPassed.get());

        // Until the consumer gets the new data
        Thread readNewDataThread = new Thread("read-new-data-thread") {
            @Override
            public void run() {
                // Should keep polling until it reaches current log end offset for all partitions
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, TP0_KEY.array(), TP0_VALUE_NEW.array()));
                    }
                }, 10000);

                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, TP1_KEY.array(), TP1_VALUE_NEW.array()));
                    }
                }, 10000);
            }
        };
        readNewDataThread.start();
        readNewDataThread.join(10000);
        assertFalse(readNewDataThread.isAlive());

        // And now the new data should be returned
        final AtomicBoolean finalGetInvokedAndPassed = new AtomicBoolean(false);
        store.get(Arrays.asList(TP0_KEY, TP1_KEY), new Callback<Map<ByteBuffer, ByteBuffer>>() {
            @Override
            public void onCompletion(Throwable error, Map<ByteBuffer, ByteBuffer> result) {
                assertEquals(TP0_VALUE_NEW, result.get(TP0_KEY));
                assertEquals(TP1_VALUE_NEW, result.get(TP1_KEY));
                finalGetInvokedAndPassed.set(true);
            }
        }).get(10000, TimeUnit.MILLISECONDS);
        assertTrue(finalGetInvokedAndPassed.get());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testConsumerError() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        store.configure(DEFAULT_PROPS);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 1L);
        endOffsets.put(TP1, 1L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Trigger exception
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.setException(Errors.CONSUMER_COORDINATOR_NOT_AVAILABLE.exception());
                    }
                }, 10000);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, TP0_KEY.array(), TP0_VALUE_NEW.array()));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, TP0_KEY.array(), TP0_VALUE_NEW.array()));
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testProducerError() throws Exception {
        expectStart();
        TestFuture<RecordMetadata> tp0Future = new TestFuture<>();
        ProducerRecord<byte[], byte[]> tp0Record = new ProducerRecord<>(TOPIC, TP0_KEY.array(), TP0_VALUE.array());
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        EasyMock.expect(producer.send(eqProducerRecord(tp0Record), EasyMock.capture(callback1))).andReturn(tp0Future);
        TestFuture<RecordMetadata> tp1Future = new TestFuture<>();
        ProducerRecord<byte[], byte[]> tp1Record = new ProducerRecord<>(TOPIC, TP1_KEY.array(), TP1_VALUE.array());
        Capture<org.apache.kafka.clients.producer.Callback> callback2 = EasyMock.newCapture();
        EasyMock.expect(producer.send(eqProducerRecord(tp1Record), EasyMock.capture(callback2))).andReturn(tp1Future);

        expectStop();

        PowerMock.replayAll();

        store.configure(DEFAULT_PROPS);

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        Thread startConsumerOpsThread = new Thread("start-consumer-ops-thread") {
            @Override
            public void run() {
                // Should keep polling until it has partition info
                consumer.waitForPollThen(new Runnable() {
                    @Override
                    public void run() {
                        consumer.seek(TP0, 0);
                        consumer.seek(TP1, 0);
                    }
                }, 10000);
            }
        };
        startConsumerOpsThread.start();
        store.start();
        startConsumerOpsThread.join(10000);
        assertFalse(startConsumerOpsThread.isAlive());
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

        Map<ByteBuffer, ByteBuffer> toSet = new HashMap<>();
        toSet.put(TP0_KEY, TP0_VALUE);
        toSet.put(TP1_KEY, TP1_VALUE);
        final AtomicReference<Throwable> setException = new AtomicReference<>();
        Future<Void> setFuture = store.set(toSet, new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                assertNull(setException.get()); // Should only be invoked once
                setException.set(error);
            }
        });
        assertFalse(setFuture.isDone());
        KafkaException exc = new LeaderNotAvailableException("Error");
        tp1Future.resolve(exc);
        callback2.getValue().onCompletion(null, exc);
        // One failure should resolve the future immediately
        try {
            setFuture.get(10000, TimeUnit.MILLISECONDS);
            fail("Should have see ExecutionException");
        } catch (ExecutionException e) {
            // expected
        }
        assertNotNull(setException.get());

        // Callbacks can continue to arrive
        tp0Future.resolve((RecordMetadata) null);
        callback1.getValue().onCompletion(null, null);

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }


    private void expectStart() throws Exception {
        PowerMock.expectPrivate(store, "createProducer")
                .andReturn(producer);
        PowerMock.expectPrivate(store, "createConsumer")
                .andReturn(consumer);
    }

    private void expectStop() {
        producer.close();
        PowerMock.expectLastCall();
        // MockConsumer close is checked after test.
    }

    private static ByteBuffer buffer(String v) {
        return ByteBuffer.wrap(v.getBytes());
    }

}
