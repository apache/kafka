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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.MockTime;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest(KafkaBasedLog.class)
@PowerMockIgnore("javax.management.*")
public class KafkaBasedLogTest {

    private static final String TOPIC = "connect-log";
    private static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    private static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);
    private static final Map<String, Object> PRODUCER_PROPS = new HashMap<>();
    static {
        PRODUCER_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        PRODUCER_PROPS.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        PRODUCER_PROPS.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    }
    private static final Map<String, Object> CONSUMER_PROPS = new HashMap<>();
    static {
        CONSUMER_PROPS.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9093");
        CONSUMER_PROPS.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        CONSUMER_PROPS.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    }

    private static final Set<TopicPartition> CONSUMER_ASSIGNMENT = new HashSet<>(Arrays.asList(TP0, TP1));
    private static final Map<String, String> FIRST_SET = new HashMap<>();
    static {
        FIRST_SET.put("key", "value");
        FIRST_SET.put(null, null);
    }

    private static final Node LEADER = new Node(1, "broker1", 9092);
    private static final Node REPLICA = new Node(1, "broker2", 9093);

    private static final PartitionInfo TPINFO0 = new PartitionInfo(TOPIC, 0, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});
    private static final PartitionInfo TPINFO1 = new PartitionInfo(TOPIC, 1, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});

    private static final String TP0_KEY = "TP0KEY";
    private static final String TP1_KEY = "TP1KEY";
    private static final String TP0_VALUE = "VAL0";
    private static final String TP1_VALUE = "VAL1";
    private static final String TP0_VALUE_NEW = "VAL0_NEW";
    private static final String TP1_VALUE_NEW = "VAL1_NEW";

    private Time time = new MockTime();
    private KafkaBasedLog<String, String> store;

    @Mock
    private Runnable initializer;
    @Mock
    private KafkaProducer<String, String> producer;
    private MockConsumer<String, String> consumer;

    private Map<TopicPartition, List<ConsumerRecord<String, String>>> consumedRecords = new HashMap<>();
    private Callback<ConsumerRecord<String, String>> consumedCallback = new Callback<ConsumerRecord<String, String>>() {
        @Override
        public void onCompletion(Throwable error, ConsumerRecord<String, String> record) {
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            List<ConsumerRecord<String, String>> records = consumedRecords.get(partition);
            if (records == null) {
                records = new ArrayList<>();
                consumedRecords.put(partition, records);
            }
            records.add(record);
        }
    };

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        store = PowerMock.createPartialMock(KafkaBasedLog.class, new String[]{"createConsumer", "createProducer"},
                TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, consumedCallback, time, initializer);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        consumer.updatePartitions(TOPIC, Arrays.asList(TPINFO0, TPINFO1));
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(TP0, 0L);
        beginningOffsets.put(TP1, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);
    }

    @Test
    public void testStartStop() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

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

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 1L);
        endOffsets.put(TP1, 1L);
        consumer.updateEndOffsets(endOffsets);
        final CountDownLatch finishedLatch = new CountDownLatch(1);
        consumer.schedulePollTask(new Runnable() { // Use first poll task to setup sequence of remaining responses to polls
            @Override
            public void run() {
                // Should keep polling until it reaches current log end offset for all partitions. Should handle
                // as many empty polls as needed
                consumer.scheduleNopPollTask();
                consumer.scheduleNopPollTask();
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE));
                    }
                });
                consumer.scheduleNopPollTask();
                consumer.scheduleNopPollTask();
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY, TP1_VALUE));
                    }
                });
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        finishedLatch.countDown();
                    }
                });
            }
        });
        store.start();
        assertTrue(finishedLatch.await(10000, TimeUnit.MILLISECONDS));

        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(2, consumedRecords.size());

        assertEquals(TP0_VALUE, consumedRecords.get(TP0).get(0).value());
        assertEquals(TP1_VALUE, consumedRecords.get(TP1).get(0).value());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testReloadOnStartWithNoNewRecordsPresent() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 7L);
        endOffsets.put(TP1, 7L);
        consumer.updateEndOffsets(endOffsets);
        // Better test with an advanced offset other than just 0L
        consumer.updateBeginningOffsets(endOffsets);

        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                // Throw an exception that will not be ignored or handled by Connect framework. In
                // reality a misplaced call to poll blocks indefinitely and connect aborts due to
                // time outs (for instance via ConnectRestException)
                throw new WakeupException();
            }
        });

        store.start();

        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(7L, consumer.position(TP0));
        assertEquals(7L, consumer.position(TP1));

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testSendAndReadToEnd() throws Exception {
        expectStart();
        TestFuture<RecordMetadata> tp0Future = new TestFuture<>();
        ProducerRecord<String, String> tp0Record = new ProducerRecord<>(TOPIC, TP0_KEY, TP0_VALUE);
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        EasyMock.expect(producer.send(EasyMock.eq(tp0Record), EasyMock.capture(callback0))).andReturn(tp0Future);
        TestFuture<RecordMetadata> tp1Future = new TestFuture<>();
        ProducerRecord<String, String> tp1Record = new ProducerRecord<>(TOPIC, TP1_KEY, TP1_VALUE);
        Capture<org.apache.kafka.clients.producer.Callback> callback1 = EasyMock.newCapture();
        EasyMock.expect(producer.send(EasyMock.eq(tp1Record), EasyMock.capture(callback1))).andReturn(tp1Future);

        // Producer flushes when read to log end is called
        producer.flush();
        PowerMock.expectLastCall();

        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        store.start();
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(0L, consumer.position(TP0));
        assertEquals(0L, consumer.position(TP1));

        // Set some keys
        final AtomicInteger invoked = new AtomicInteger(0);
        org.apache.kafka.clients.producer.Callback producerCallback = new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                invoked.incrementAndGet();
            }
        };
        store.send(TP0_KEY, TP0_VALUE, producerCallback);
        store.send(TP1_KEY, TP1_VALUE, producerCallback);
        assertEquals(0, invoked.get());
        tp1Future.resolve((RecordMetadata) null); // Output not used, so safe to not return a real value for testing
        callback1.getValue().onCompletion(null, null);
        assertEquals(1, invoked.get());
        tp0Future.resolve((RecordMetadata) null);
        callback0.getValue().onCompletion(null, null);
        assertEquals(2, invoked.get());

        // Now we should have to wait for the records to be read back when we call readToEnd()
        final AtomicBoolean getInvoked = new AtomicBoolean(false);
        final FutureCallback<Void> readEndFutureCallback = new FutureCallback<>(new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                getInvoked.set(true);
            }
        });
        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                // Once we're synchronized in a poll, start the read to end and schedule the exact set of poll events
                // that should follow. This readToEnd call will immediately wakeup this consumer.poll() call without
                // returning any data.
                Map<TopicPartition, Long> newEndOffsets = new HashMap<>();
                newEndOffsets.put(TP0, 2L);
                newEndOffsets.put(TP1, 2L);
                consumer.updateEndOffsets(newEndOffsets);
                store.readToEnd(readEndFutureCallback);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.scheduleNopPollTask();
                consumer.scheduleNopPollTask();
                consumer.scheduleNopPollTask();
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE_NEW));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY, TP1_VALUE));
                    }
                });

                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP1_KEY, TP1_VALUE_NEW));
                    }
                });

                // Already have FutureCallback that should be invoked/awaited, so no need for follow up finishedLatch
            }
        });
        readEndFutureCallback.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(getInvoked.get());
        assertEquals(2, consumedRecords.size());

        assertEquals(2, consumedRecords.get(TP0).size());
        assertEquals(TP0_VALUE, consumedRecords.get(TP0).get(0).value());
        assertEquals(TP0_VALUE_NEW, consumedRecords.get(TP0).get(1).value());

        assertEquals(2, consumedRecords.get(TP1).size());
        assertEquals(TP1_VALUE, consumedRecords.get(TP1).get(0).value());
        assertEquals(TP1_VALUE_NEW, consumedRecords.get(TP1).get(1).value());

        // Cleanup
        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testPollConsumerError() throws Exception {
        expectStart();
        expectStop();

        PowerMock.replayAll();

        final CountDownLatch finishedLatch = new CountDownLatch(1);
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 1L);
        endOffsets.put(TP1, 1L);
        consumer.updateEndOffsets(endOffsets);
        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                // Trigger exception
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.setPollException(Errors.COORDINATOR_NOT_AVAILABLE.exception());
                    }
                });

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.scheduleNopPollTask();
                consumer.scheduleNopPollTask();
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE_NEW));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE_NEW));
                    }
                });

                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        finishedLatch.countDown();
                    }
                });
            }
        });
        store.start();
        assertTrue(finishedLatch.await(10000, TimeUnit.MILLISECONDS));
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(1L, consumer.position(TP0));

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testGetOffsetsConsumerErrorOnReadToEnd() throws Exception {
        expectStart();

        // Producer flushes when read to log end is called
        producer.flush();
        PowerMock.expectLastCall();

        expectStop();

        PowerMock.replayAll();
        final CountDownLatch finishedLatch = new CountDownLatch(1);
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        store.start();
        final AtomicBoolean getInvoked = new AtomicBoolean(false);
        final FutureCallback<Void> readEndFutureCallback = new FutureCallback<>(new Callback<Void>() {
            @Override
            public void onCompletion(Throwable error, Void result) {
                getInvoked.set(true);
            }
        });
        consumer.schedulePollTask(new Runnable() {
            @Override
            public void run() {
                // Once we're synchronized in a poll, start the read to end and schedule the exact set of poll events
                // that should follow. This readToEnd call will immediately wakeup this consumer.poll() call without
                // returning any data.
                Map<TopicPartition, Long> newEndOffsets = new HashMap<>();
                newEndOffsets.put(TP0, 1L);
                newEndOffsets.put(TP1, 1L);
                consumer.updateEndOffsets(newEndOffsets);
                // Set exception to occur when getting offsets to read log to end.  It'll be caught in the work thread,
                // which will retry and eventually get the correct offsets and read log to end.
                consumer.setOffsetsException(new TimeoutException("Failed to get offsets by times"));
                store.readToEnd(readEndFutureCallback);

                // Should keep polling until it reaches current log end offset for all partitions
                consumer.scheduleNopPollTask();
                consumer.scheduleNopPollTask();
                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE));
                        consumer.addRecord(new ConsumerRecord<>(TOPIC, 1, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, TP0_KEY, TP0_VALUE_NEW));
                    }
                });

                consumer.schedulePollTask(new Runnable() {
                    @Override
                    public void run() {
                        finishedLatch.countDown();
                    }
                });
            }
        });
        readEndFutureCallback.get(10000, TimeUnit.MILLISECONDS);
        assertTrue(getInvoked.get());
        assertTrue(finishedLatch.await(10000, TimeUnit.MILLISECONDS));
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(1L, consumer.position(TP0));

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }

    @Test
    public void testProducerError() throws Exception {
        expectStart();
        TestFuture<RecordMetadata> tp0Future = new TestFuture<>();
        ProducerRecord<String, String> tp0Record = new ProducerRecord<>(TOPIC, TP0_KEY, TP0_VALUE);
        Capture<org.apache.kafka.clients.producer.Callback> callback0 = EasyMock.newCapture();
        EasyMock.expect(producer.send(EasyMock.eq(tp0Record), EasyMock.capture(callback0))).andReturn(tp0Future);

        expectStop();

        PowerMock.replayAll();

        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        endOffsets.put(TP0, 0L);
        endOffsets.put(TP1, 0L);
        consumer.updateEndOffsets(endOffsets);
        store.start();
        assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
        assertEquals(0L, consumer.position(TP0));
        assertEquals(0L, consumer.position(TP1));

        final AtomicReference<Throwable> setException = new AtomicReference<>();
        store.send(TP0_KEY, TP0_VALUE, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                assertNull(setException.get()); // Should only be invoked once
                setException.set(exception);
            }
        });
        KafkaException exc = new LeaderNotAvailableException("Error");
        tp0Future.resolve(exc);
        callback0.getValue().onCompletion(null, exc);
        assertNotNull(setException.get());

        store.stop();

        assertFalse(Whitebox.<Thread>getInternalState(store, "thread").isAlive());
        assertTrue(consumer.closed());
        PowerMock.verifyAll();
    }


    private void expectStart() throws Exception {
        initializer.run();
        EasyMock.expectLastCall().times(1);

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
