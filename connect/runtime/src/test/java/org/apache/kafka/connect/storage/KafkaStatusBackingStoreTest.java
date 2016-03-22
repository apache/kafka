/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.storage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.easymock.IAnswer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("unchecked")
public class KafkaStatusBackingStoreTest extends EasyMockSupport {

    private static final String STATUS_TOPIC = "status-topic";
    private static final String WORKER_ID = "localhost:8083";
    private static final String CONNECTOR = "conn";
    private static final ConnectorTaskId TASK = new ConnectorTaskId(CONNECTOR, 0);

    @Test
    public void putConnectorState() {
        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        byte[] value = new byte[0];
        expect(converter.fromConnectData(eq(STATUS_TOPIC), anyObject(Schema.class), anyObject(Struct.class)))
                .andStubReturn(value);

        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-connector-conn"), eq(value), capture(callbackCapture));
        expectLastCall()
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, null);
                        return null;
                    }
                });
        replayAll();

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertEquals(null, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void putConnectorStateRetriableFailure() {
        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        byte[] value = new byte[0];
        expect(converter.fromConnectData(eq(STATUS_TOPIC), anyObject(Schema.class), anyObject(Struct.class)))
                .andStubReturn(value);

        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-connector-conn"), eq(value), capture(callbackCapture));
        expectLastCall()
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, new TimeoutException());
                        return null;
                    }
                })
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, null);
                        return null;
                    }
                });
        replayAll();

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertEquals(null, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void putConnectorStateNonRetriableFailure() {
        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        byte[] value = new byte[0];
        expect(converter.fromConnectData(eq(STATUS_TOPIC), anyObject(Schema.class), anyObject(Struct.class)))
                .andStubReturn(value);

        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-connector-conn"), eq(value), capture(callbackCapture));
        expectLastCall()
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, new UnknownServerException());
                        return null;
                    }
                });
        replayAll();

        // the error is logged and ignored
        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertEquals(null, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void putSafeConnectorIgnoresStaleStatus() {
        byte[] value = new byte[0];
        String otherWorkerId = "anotherhost:8083";

        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        // the persisted came from a different host and has a newer generation
        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", otherWorkerId);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 1L);

        expect(converter.toConnectData(STATUS_TOPIC, value))
                .andReturn(new SchemaAndValue(null, statusMap));

        // we're verifying that there is no call to KafkaBasedLog.send

        replayAll();

        store.read(consumerRecord(0, "status-connector-conn", value));
        store.putSafe(new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, otherWorkerId, 1);
        assertEquals(status, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void putSafeOverridesValueSetBySameWorker() {
        final byte[] value = new byte[0];

        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        final KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        // the persisted came from the same host, but has a newer generation
        Map<String, Object> firstStatusRead = new HashMap<>();
        firstStatusRead.put("worker_id", WORKER_ID);
        firstStatusRead.put("state", "RUNNING");
        firstStatusRead.put("generation", 1L);

        Map<String, Object> secondStatusRead = new HashMap<>();
        secondStatusRead.put("worker_id", WORKER_ID);
        secondStatusRead.put("state", "UNASSIGNED");
        secondStatusRead.put("generation", 0L);

        expect(converter.toConnectData(STATUS_TOPIC, value))
                .andReturn(new SchemaAndValue(null, firstStatusRead))
                .andReturn(new SchemaAndValue(null, secondStatusRead));

        expect(converter.fromConnectData(eq(STATUS_TOPIC), anyObject(Schema.class), anyObject(Struct.class)))
                .andStubReturn(value);

        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-connector-conn"), eq(value), capture(callbackCapture));
        expectLastCall()
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, null);
                        store.read(consumerRecord(1, "status-connector-conn", value));
                        return null;
                    }
                });

        replayAll();

        store.read(consumerRecord(0, "status-connector-conn", value));
        store.putSafe(new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0);
        assertEquals(status, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void putConnectorStateShouldOverride() {
        final byte[] value = new byte[0];
        String otherWorkerId = "anotherhost:8083";

        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        final KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        // the persisted came from a different host and has a newer generation
        Map<String, Object> firstStatusRead = new HashMap<>();
        firstStatusRead.put("worker_id", otherWorkerId);
        firstStatusRead.put("state", "RUNNING");
        firstStatusRead.put("generation", 1L);

        Map<String, Object> secondStatusRead = new HashMap<>();
        secondStatusRead.put("worker_id", WORKER_ID);
        secondStatusRead.put("state", "UNASSIGNED");
        secondStatusRead.put("generation", 0L);

        expect(converter.toConnectData(STATUS_TOPIC, value))
                .andReturn(new SchemaAndValue(null, firstStatusRead))
                .andReturn(new SchemaAndValue(null, secondStatusRead));

        expect(converter.fromConnectData(eq(STATUS_TOPIC), anyObject(Schema.class), anyObject(Struct.class)))
                .andStubReturn(value);

        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-connector-conn"), eq(value), capture(callbackCapture));
        expectLastCall()
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, null);
                        store.read(consumerRecord(1, "status-connector-conn", value));
                        return null;
                    }
                });
        replayAll();

        store.read(consumerRecord(0, "status-connector-conn", value));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0);
        store.put(status);
        assertEquals(status, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void readConnectorState() {
        byte[] value = new byte[0];

        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", WORKER_ID);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 0L);

        expect(converter.toConnectData(STATUS_TOPIC, value))
                .andReturn(new SchemaAndValue(null, statusMap));

        replayAll();

        store.read(consumerRecord(0, "status-connector-conn", value));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        assertEquals(status, store.get(CONNECTOR));

        verifyAll();
    }

    @Test
    public void putTaskState() {
        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        byte[] value = new byte[0];
        expect(converter.fromConnectData(eq(STATUS_TOPIC), anyObject(Schema.class), anyObject(Struct.class)))
                .andStubReturn(value);

        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-task-conn-0"), eq(value), capture(callbackCapture));
        expectLastCall()
                .andAnswer(new IAnswer<Void>() {
                    @Override
                    public Void answer() throws Throwable {
                        callbackCapture.getValue().onCompletion(null, null);
                        return null;
                    }
                });
        replayAll();

        TaskStatus status = new TaskStatus(TASK, TaskStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertEquals(null, store.get(TASK));

        verifyAll();
    }

    @Test
    public void readTaskState() {
        byte[] value = new byte[0];

        KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);
        Converter converter = mock(Converter.class);
        KafkaStatusBackingStore store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", WORKER_ID);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 0L);

        expect(converter.toConnectData(STATUS_TOPIC, value))
                .andReturn(new SchemaAndValue(null, statusMap));

        replayAll();

        store.read(consumerRecord(0, "status-task-conn-0", value));

        TaskStatus status = new TaskStatus(TASK, TaskStatus.State.RUNNING, WORKER_ID, 0);
        assertEquals(status, store.get(TASK));

        verifyAll();
    }

    private static ConsumerRecord<String, byte[]> consumerRecord(long offset, String key, byte[] value) {
        return new ConsumerRecord<>(STATUS_TOPIC, 0, offset, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, 0L, 0, 0, key, value);
    }

}
