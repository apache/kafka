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
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.TopicStatus;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.storage.KafkaStatusBackingStore.CONNECTOR_STATUS_PREFIX;
import static org.apache.kafka.connect.storage.KafkaStatusBackingStore.TASK_STATUS_PREFIX;
import static org.apache.kafka.connect.storage.KafkaStatusBackingStore.TOPIC_STATUS_PREFIX;
import static org.apache.kafka.connect.storage.KafkaStatusBackingStore.TOPIC_STATUS_SEPARATOR;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class KafkaStatusBackingStoreFormatTest {

    private static final String STATUS_TOPIC = "status-topic";
    private static final String FOO_TOPIC = "foo-topic";
    private static final String FOO_CONNECTOR = "foo-source";
    private static final String BAR_TOPIC = "bar-topic";

    private Time time;
    private KafkaStatusBackingStore store;
    private JsonConverter converter;

    private KafkaBasedLog<String, byte[]> kafkaBasedLog = mock(KafkaBasedLog.class);

    @Before
    public void setup() {
        time = new MockTime();
        converter = new JsonConverter();
        converter.configure(Collections.singletonMap(SCHEMAS_ENABLE_CONFIG, false), false);
        store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);
    }

    @Test
    public void readInvalidStatus() {
        String key = "status-unknown";
        byte[] value = new byte[0];
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.connectors().isEmpty());
        assertTrue(store.tasks.isEmpty());
        assertTrue(store.topics.isEmpty());
        store.read(statusRecord);
        assertTrue(store.connectors().isEmpty());
        assertTrue(store.tasks.isEmpty());
        assertTrue(store.topics.isEmpty());

        key = CONNECTOR_STATUS_PREFIX;
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.connectors().isEmpty());
        store.read(statusRecord);
        assertTrue(store.connectors().isEmpty());

        key = TASK_STATUS_PREFIX;
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.tasks.isEmpty());
        store.read(statusRecord);
        assertTrue(store.tasks.isEmpty());

        key = TASK_STATUS_PREFIX + FOO_CONNECTOR + "-#";
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.tasks.isEmpty());
        store.read(statusRecord);
        assertTrue(store.tasks.isEmpty());

        key = TOPIC_STATUS_PREFIX;
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.topics.isEmpty());
        store.read(statusRecord);
        assertTrue(store.topics.isEmpty());

        key = TOPIC_STATUS_PREFIX + TOPIC_STATUS_SEPARATOR;
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.topics.isEmpty());
        store.read(statusRecord);
        assertTrue(store.topics.isEmpty());

        key = TOPIC_STATUS_PREFIX + FOO_TOPIC + ":";
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.topics.isEmpty());
        store.read(statusRecord);
        assertTrue(store.topics.isEmpty());

        key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR;
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.topics.isEmpty());
        store.read(statusRecord);
        assertTrue(store.topics.isEmpty());
    }

    @Test
    public void readInvalidStatusValue() {
        String key = CONNECTOR_STATUS_PREFIX + FOO_CONNECTOR;
        byte[] value = "invalid".getBytes();
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.connectors().isEmpty());
        store.read(statusRecord);
        assertTrue(store.connectors().isEmpty());

        key = TASK_STATUS_PREFIX + FOO_CONNECTOR + "-0";
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.tasks.isEmpty());
        store.read(statusRecord);
        assertTrue(store.tasks.isEmpty());

        key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;
        statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        assertTrue(store.topics.isEmpty());
        store.read(statusRecord);
        assertTrue(store.topics.isEmpty());
    }

    @Test
    public void readTopicStatus() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), Time.SYSTEM.milliseconds());
        String key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;
        byte[] value = store.serializeTopicStatus(topicStatus);
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, value);
        store.read(statusRecord);
        assertTrue(store.topics.containsKey(FOO_CONNECTOR));
        assertTrue(store.topics.get(FOO_CONNECTOR).containsKey(FOO_TOPIC));
        assertEquals(topicStatus, store.topics.get(FOO_CONNECTOR).get(FOO_TOPIC));
    }

    @Test
    public void deleteTopicStatus() {
        TopicStatus topicStatus = new TopicStatus("foo", new ConnectorTaskId("bar", 0), Time.SYSTEM.milliseconds());
        store.topics.computeIfAbsent("bar", k -> new ConcurrentHashMap<>()).put("foo", topicStatus);
        assertTrue(store.topics.containsKey("bar"));
        assertTrue(store.topics.get("bar").containsKey("foo"));
        assertEquals(topicStatus, store.topics.get("bar").get("foo"));
        // should return null
        byte[] value = store.serializeTopicStatus(null);
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, "status-topic-foo:connector-bar", value);
        store.read(statusRecord);
        assertTrue(store.topics.containsKey("bar"));
        assertFalse(store.topics.get("bar").containsKey("foo"));
        assertEquals(Collections.emptyMap(), store.topics.get("bar"));
    }

    @Test
    public void putTopicState() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), time.milliseconds());
        String key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;
        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            return null;
        }).when(kafkaBasedLog).send(eq(key), valueCaptor.capture(), any(Callback.class));

        store.put(topicStatus);
        // check capture state
        assertEquals(topicStatus, store.parseTopicStatus(valueCaptor.getValue()));
        // state is not visible until read back from the log
        assertNull(store.getTopic(FOO_CONNECTOR, FOO_TOPIC));

        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, key, valueCaptor.getValue());
        store.read(statusRecord);
        assertEquals(topicStatus, store.getTopic(FOO_CONNECTOR, FOO_TOPIC));
        assertEquals(Collections.singleton(topicStatus), new HashSet<>(store.getAllTopics(FOO_CONNECTOR)));
    }

    @Test
    public void putTopicStateRetriableFailure() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), time.milliseconds());
        String key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;

        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, new TimeoutException());
            return null;
        }).doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            return null;
        }).when(kafkaBasedLog).send(eq(key), valueCaptor.capture(), any(Callback.class));

        store.put(topicStatus);
        verify(kafkaBasedLog, timeout(1000).times(2)).send(any(), any(), any());

        // check capture state
        assertEquals(topicStatus, store.parseTopicStatus(valueCaptor.getValue()));
        // state is not visible until read back from the log
        assertNull(store.getTopic(FOO_CONNECTOR, FOO_TOPIC));
    }

    @Test
    public void putTopicStateNonRetriableFailure() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), time.milliseconds());
        String key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;

        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, new UnknownServerException());
            return null;
        }).when(kafkaBasedLog).send(eq(key), valueCaptor.capture(), any(Callback.class));

        // the error is logged and ignored
        store.put(topicStatus);

        // check capture state
        assertEquals(topicStatus, store.parseTopicStatus(valueCaptor.getValue()));
        // state is not visible until read back from the log
        assertNull(store.getTopic(FOO_CONNECTOR, FOO_TOPIC));
    }

    @Test
    public void putTopicStateShouldOverridePreviousState() {
        TopicStatus firstTopicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR,
                0), time.milliseconds());
        time.sleep(1000);
        TopicStatus secondTopicStatus = new TopicStatus(BAR_TOPIC, new ConnectorTaskId(FOO_CONNECTOR,
                0), time.milliseconds());
        String firstKey = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;
        String secondKey = TOPIC_STATUS_PREFIX + BAR_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;

        ArgumentCaptor<byte[]> valueCaptor = ArgumentCaptor.forClass(byte[].class);
        doAnswer(invocation -> {
            ((Callback) invocation.getArgument(2)).onCompletion(null, null);
            ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, secondKey, valueCaptor.getValue());
            store.read(statusRecord);
            return null;
        }).when(kafkaBasedLog).send(eq(secondKey), valueCaptor.capture(), any(Callback.class));

        byte[] value = store.serializeTopicStatus(firstTopicStatus);
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, firstKey, value);
        store.read(statusRecord);
        store.put(secondTopicStatus);

        // check capture state
        assertEquals(secondTopicStatus, store.parseTopicStatus(valueCaptor.getValue()));
        assertEquals(firstTopicStatus, store.getTopic(FOO_CONNECTOR, FOO_TOPIC));
        assertEquals(secondTopicStatus, store.getTopic(FOO_CONNECTOR, BAR_TOPIC));
        assertEquals(new HashSet<>(Arrays.asList(firstTopicStatus, secondTopicStatus)), new HashSet<>(store.getAllTopics(FOO_CONNECTOR)));
    }

}
