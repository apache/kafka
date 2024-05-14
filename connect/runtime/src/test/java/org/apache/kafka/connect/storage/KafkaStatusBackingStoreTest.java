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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.ConnectorStatus;
import org.apache.kafka.connect.runtime.TaskStatus;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.kafka.clients.CommonClientConfigs.CLIENT_ID_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KafkaStatusBackingStoreTest {

    private static final String STATUS_TOPIC = "status-topic";
    private static final String WORKER_ID = "localhost:8083";
    private static final String CONNECTOR = "conn";
    private static final ConnectorTaskId TASK = new ConnectorTaskId(CONNECTOR, 0);

    private KafkaStatusBackingStore store;
    private final KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);

    Converter converter = mock(Converter.class);
    WorkerConfig workerConfig = mock(WorkerConfig.class);

    @Before
    public void setup() {
        store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);
    }

    @Test
    public void misconfigurationOfStatusBackingStore() {
        when(workerConfig.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG)).thenReturn(null);
        when(workerConfig.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG)).thenReturn("   ");

        Exception e = assertThrows(ConfigException.class, () -> store.configure(workerConfig));
        assertEquals("Must specify topic for connector status.", e.getMessage());
        e = assertThrows(ConfigException.class, () -> store.configure(workerConfig));
        assertEquals("Must specify topic for connector status.", e.getMessage());
    }

    @Test
    public void putConnectorState() {
        byte[] value = new byte[0];
        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class))).thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            return null;
        }).when(kafkaBasedLog).send(eq("status-connector-conn"), eq(value), any(Callback.class));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertNull(store.get(CONNECTOR));
    }

    @Test
    public void putConnectorStateRetriableFailure() {
        byte[] value = new byte[0];
        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class))).thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, new TimeoutException());
            return null;
        }).doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            return null;
        })
        .when(kafkaBasedLog).send(eq("status-connector-conn"), eq(value), any(Callback.class));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertNull(store.get(CONNECTOR));
    }

    @Test
    public void putConnectorStateNonRetriableFailure() {
        byte[] value = new byte[0];
        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class))).thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, new UnknownServerException());
            return null;
        }).when(kafkaBasedLog).send(eq("status-connector-conn"), eq(value), any(Callback.class));

        // the error is logged and ignored
        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertNull(store.get(CONNECTOR));
    }

    @Test
    public void putSafeConnectorIgnoresStaleStatus() {
        byte[] value = new byte[0];
        String otherWorkerId = "anotherhost:8083";

        // the persisted came from a different host and has a newer generation
        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", otherWorkerId);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 1L);

        when(converter.toConnectData(STATUS_TOPIC, value)).thenReturn(new SchemaAndValue(null, statusMap));

        store.read(consumerRecord(0, "status-connector-conn", value));
        store.putSafe(new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0));

        verify(kafkaBasedLog, never()).send(anyString(), any(), any(Callback.class));
        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, otherWorkerId, 1);
        assertEquals(status, store.get(CONNECTOR));
    }

    @Test
    public void putSafeWithNoPreviousValueIsPropagated() {
        final byte[] value = new byte[0];
        ArgumentCaptor<Struct> captor = ArgumentCaptor.forClass(Struct.class);

        kafkaBasedLog.send(eq("status-connector-" + CONNECTOR), eq(value), any(Callback.class));
        final ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.FAILED, WORKER_ID, 0);
        store.putSafe(status);

        verify(converter).fromConnectData(eq(STATUS_TOPIC), any(Schema.class), captor.capture());
        assertEquals(status.state().toString(), captor.getValue().get(KafkaStatusBackingStore.STATE_KEY_NAME));
        assertEquals(status.workerId(), captor.getValue().get(KafkaStatusBackingStore.WORKER_ID_KEY_NAME));
        assertEquals(status.generation(), captor.getValue().get(KafkaStatusBackingStore.GENERATION_KEY_NAME));
    }

    @Test
    public void putSafeOverridesValueSetBySameWorker() {
        final byte[] value = new byte[0];

        // the persisted came from the same host, but has a newer generation
        Map<String, Object> firstStatusRead = new HashMap<>();
        firstStatusRead.put("worker_id", WORKER_ID);
        firstStatusRead.put("state", "RUNNING");
        firstStatusRead.put("generation", 1L);

        Map<String, Object> secondStatusRead = new HashMap<>();
        secondStatusRead.put("worker_id", WORKER_ID);
        secondStatusRead.put("state", "UNASSIGNED");
        secondStatusRead.put("generation", 0L);

        when(converter.toConnectData(STATUS_TOPIC, value))
                .thenReturn(new SchemaAndValue(null, firstStatusRead))
                .thenReturn(new SchemaAndValue(null, secondStatusRead));

        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class)))
                .thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            store.read(consumerRecord(1, "status-connector-conn", value));
            return null;
        }).when(kafkaBasedLog).send(eq("status-connector-conn"), eq(value), any(Callback.class));

        store.read(consumerRecord(0, "status-connector-conn", value));
        store.putSafe(new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0);
        assertEquals(status, store.get(CONNECTOR));
    }

    @Test
    public void putConnectorStateShouldOverride() {
        final byte[] value = new byte[0];
        String otherWorkerId = "anotherhost:8083";

        // the persisted came from a different host and has a newer generation
        Map<String, Object> firstStatusRead = new HashMap<>();
        firstStatusRead.put("worker_id", otherWorkerId);
        firstStatusRead.put("state", "RUNNING");
        firstStatusRead.put("generation", 1L);

        Map<String, Object> secondStatusRead = new HashMap<>();
        secondStatusRead.put("worker_id", WORKER_ID);
        secondStatusRead.put("state", "UNASSIGNED");
        secondStatusRead.put("generation", 0L);

        when(converter.toConnectData(STATUS_TOPIC, value))
                .thenReturn(new SchemaAndValue(null, firstStatusRead))
                .thenReturn(new SchemaAndValue(null, secondStatusRead));

        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class)))
                .thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            store.read(consumerRecord(1, "status-connector-conn", value));
            return null;
        }).when(kafkaBasedLog).send(eq("status-connector-conn"), eq(value), any(Callback.class));

        store.read(consumerRecord(0, "status-connector-conn", value));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.UNASSIGNED, WORKER_ID, 0);
        store.put(status);
        assertEquals(status, store.get(CONNECTOR));
    }

    @Test
    public void readConnectorState() {
        byte[] value = new byte[0];

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", WORKER_ID);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 0L);

        when(converter.toConnectData(STATUS_TOPIC, value))
                .thenReturn(new SchemaAndValue(null, statusMap));

        store.read(consumerRecord(0, "status-connector-conn", value));

        ConnectorStatus status = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        assertEquals(status, store.get(CONNECTOR));
    }

    @Test
    public void putTaskState() {
        byte[] value = new byte[0];
        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class)))
                .thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            store.read(consumerRecord(1, "status-connector-conn", value));
            return null;
        }).when(kafkaBasedLog).send(eq("status-task-conn-0"), eq(value), any(Callback.class));

        TaskStatus status = new TaskStatus(TASK, TaskStatus.State.RUNNING, WORKER_ID, 0);
        store.put(status);

        // state is not visible until read back from the log
        assertNull(store.get(TASK));
    }

    @Test
    public void readTaskState() {
        byte[] value = new byte[0];

        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", WORKER_ID);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 0L);

        when(converter.toConnectData(STATUS_TOPIC, value))
                .thenReturn(new SchemaAndValue(null, statusMap));

        store.read(consumerRecord(0, "status-task-conn-0", value));

        TaskStatus status = new TaskStatus(TASK, TaskStatus.State.RUNNING, WORKER_ID, 0);
        assertEquals(status, store.get(TASK));
    }

    @Test
    public void readTaskStateShouldIgnoreStaleStatusesFromOtherWorkers() {
        byte[] value = new byte[0];
        String otherWorkerId = "anotherhost:8083";
        String yetAnotherWorkerId = "yetanotherhost:8083";

        // This worker sends a RUNNING status in the most recent generation
        Map<String, Object> firstStatusRead = new HashMap<>();
        firstStatusRead.put("worker_id", otherWorkerId);
        firstStatusRead.put("state", "RUNNING");
        firstStatusRead.put("generation", 10L);

        // Another worker still ends up producing an UNASSIGNED status before it could
        // read the newer RUNNING status from above belonging to an older generation.
        Map<String, Object> secondStatusRead = new HashMap<>();
        secondStatusRead.put("worker_id", WORKER_ID);
        secondStatusRead.put("state", "UNASSIGNED");
        secondStatusRead.put("generation", 9L);

        Map<String, Object> thirdStatusRead = new HashMap<>();
        thirdStatusRead.put("worker_id", yetAnotherWorkerId);
        thirdStatusRead.put("state", "RUNNING");
        thirdStatusRead.put("generation", 1L);

        when(converter.toConnectData(STATUS_TOPIC, value))
                .thenReturn(new SchemaAndValue(null, firstStatusRead))
                .thenReturn(new SchemaAndValue(null, secondStatusRead))
                .thenReturn(new SchemaAndValue(null, thirdStatusRead));

        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class)))
                .thenReturn(value);

        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            store.read(consumerRecord(2, "status-task-conn-0", value));
            return null;
        }).when(kafkaBasedLog).send(eq("status-task-conn-0"), eq(value), any(Callback.class));

        store.read(consumerRecord(0, "status-task-conn-0", value));
        store.read(consumerRecord(1, "status-task-conn-0", value));

        // The latest task status should reflect RUNNING status from the newer generation
        TaskStatus status = new TaskStatus(TASK, TaskStatus.State.RUNNING, otherWorkerId, 10);
        assertEquals(status, store.get(TASK));

        // This  status is from the another worker not necessarily belonging to the above group.
        // In this case, the status should get updated irrespective of whatever status info was present before.
        TaskStatus latestStatus = new TaskStatus(TASK, TaskStatus.State.RUNNING, yetAnotherWorkerId, 1);
        store.put(latestStatus);
        assertEquals(latestStatus, store.get(TASK));
    }

    @Test
    public void deleteConnectorState() {
        final byte[] value = new byte[0];
        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", WORKER_ID);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 0L);

        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class))).thenReturn(value);
        when(converter.toConnectData(STATUS_TOPIC, value)).thenReturn(new SchemaAndValue(null, statusMap));

        ConnectorStatus connectorStatus = new ConnectorStatus(CONNECTOR, ConnectorStatus.State.RUNNING, WORKER_ID, 0);
        store.put(connectorStatus);
        TaskStatus taskStatus = new TaskStatus(TASK, TaskStatus.State.RUNNING, WORKER_ID, 0);
        store.put(taskStatus);
        store.read(consumerRecord(0, "status-task-conn-0", value));

        verify(kafkaBasedLog).send(eq("status-connector-" + CONNECTOR), eq(value), any(Callback.class));
        verify(kafkaBasedLog).send(eq("status-task-conn-0"), eq(value), any(Callback.class));

        assertEquals(new HashSet<>(Collections.singletonList(CONNECTOR)), store.connectors());
        assertEquals(new HashSet<>(Collections.singletonList(taskStatus)), new HashSet<>(store.getAll(CONNECTOR)));
        store.read(consumerRecord(0, "status-connector-conn", null));
        assertTrue(store.connectors().isEmpty());
        assertTrue(store.getAll(CONNECTOR).isEmpty());
    }

    @Test
    public void deleteTaskState() {
        final byte[] value = new byte[0];
        Map<String, Object> statusMap = new HashMap<>();
        statusMap.put("worker_id", WORKER_ID);
        statusMap.put("state", "RUNNING");
        statusMap.put("generation", 0L);

        when(converter.fromConnectData(eq(STATUS_TOPIC), any(Schema.class), any(Struct.class))).thenReturn(value);
        when(converter.toConnectData(STATUS_TOPIC, value)).thenReturn(new SchemaAndValue(null, statusMap));

        TaskStatus taskStatus = new TaskStatus(TASK, TaskStatus.State.RUNNING, WORKER_ID, 0);
        store.put(taskStatus);
        store.read(consumerRecord(0, "status-task-conn-0", value));

        verify(kafkaBasedLog).send(eq("status-task-conn-0"), eq(value), any(Callback.class));

        assertEquals(new HashSet<>(Collections.singletonList(taskStatus)), new HashSet<>(store.getAll(CONNECTOR)));
        store.read(consumerRecord(0, "status-task-conn-0", null));
        assertTrue(store.getAll(CONNECTOR).isEmpty());
    }

    @Test
    public void testClientIds() {
        String clientIdBase = "test-client-id-";
        Supplier<TopicAdmin> topicAdminSupplier = () -> mock(TopicAdmin.class);

        ArgumentCaptor<Map<String, Object>> capturedProducerProps = ArgumentCaptor.forClass(Map.class);
        ArgumentCaptor<Map<String, Object>> capturedConsumerProps = ArgumentCaptor.forClass(Map.class);

        store = spy(new KafkaStatusBackingStore(new MockTime(), converter, topicAdminSupplier, clientIdBase));
        KafkaBasedLog<String, byte[]> kafkaLog = mock(KafkaBasedLog.class);
        doReturn(kafkaLog).when(store).createKafkaBasedLog(any(), capturedProducerProps.capture(),
                capturedConsumerProps.capture(), any(),
                any(), any(), any(), any());


        when(workerConfig.getString(DistributedConfig.STATUS_STORAGE_TOPIC_CONFIG)).thenReturn("connect-statuses");
        store.configure(workerConfig);

        final String expectedClientId = clientIdBase + "statuses";
        assertEquals(expectedClientId, capturedProducerProps.getValue().get(CLIENT_ID_CONFIG));
        assertEquals(expectedClientId, capturedConsumerProps.getValue().get(CLIENT_ID_CONFIG));
    }

    private static ConsumerRecord<String, byte[]> consumerRecord(long offset, String key, byte[] value) {
        return new ConsumerRecord<>(STATUS_TOPIC, 0, offset, System.currentTimeMillis(),
                TimestampType.CREATE_TIME, 0, 0, key, value, new RecordHeaders(), Optional.empty());
    }

}
