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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.TopicStatus;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.apache.kafka.connect.storage.KafkaStatusBackingStore.TOPIC_STATUS_PREFIX;
import static org.apache.kafka.connect.storage.KafkaStatusBackingStore.TOPIC_STATUS_SEPARATOR;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
public class KafkaStatusBackingStoreFormatTest extends EasyMockSupport {

    private static final String STATUS_TOPIC = "status-topic";
    private static final String FOO_TOPIC = "foo-topic";
    private static final String FOO_CONNECTOR = "foo-source";
    private static final String BAR_TOPIC = "bar-topic";
    private static final String BAR_CONNECTOR = "bar-source";
    private static final Time time = new MockTime();

    private KafkaStatusBackingStore store;
    private JsonConverter converter;
    @Mock
    private KafkaBasedLog<String, byte[]> kafkaBasedLog;

    @Before
    public void setup() {
        converter = new JsonConverter();
        converter.configure(Collections.singletonMap(SCHEMAS_ENABLE_CONFIG, false), false);
        store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, kafkaBasedLog);
    }

    @Test
    public void readTopicStatus() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), Time.SYSTEM.milliseconds());
        byte[] value = store.serializeTopicStatus(topicStatus);
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0,
                "status-topic-" + FOO_TOPIC + ":connector-" + FOO_CONNECTOR, value);
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
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, "status-topic-foo:connector-bar", null);
        store.read(statusRecord);
        assertTrue(store.topics.containsKey("bar"));
        assertFalse(store.topics.get("bar").containsKey("foo"));
        assertEquals(Collections.emptyMap(), store.topics.get("bar"));
    }

    @Test
    public void putTopicState() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), time.milliseconds());
        final Capture<byte[]> valueCapture = newCapture();
        final Capture<Callback> callbackCapture = newCapture();
        kafkaBasedLog.send(eq("status-topic-foo-topic:connector-foo-source"), capture(valueCapture), capture(callbackCapture));
        expectLastCall()
                .andAnswer(() -> {
                    callbackCapture.getValue().onCompletion(null, null);
                    return null;
                });
        replayAll();

        store.put(topicStatus);

        // state is not visible until read back from the log
        assertEquals(null, store.getTopic(FOO_CONNECTOR, FOO_TOPIC));
        assertEquals(topicStatus, store.parseTopicStatus(valueCapture.getValue()));

        verifyAll();
    }

    @Test
    public void putTopicStateRetriableFailure() {
        TopicStatus topicStatus = new TopicStatus(FOO_TOPIC, new ConnectorTaskId(FOO_CONNECTOR, 0), time.milliseconds());
        final Capture<byte[]> valueCapture = newCapture();
        final Capture<Callback> callbackCapture = newCapture();
        String key = TOPIC_STATUS_PREFIX + FOO_TOPIC + TOPIC_STATUS_SEPARATOR + FOO_CONNECTOR;
        kafkaBasedLog.send(eq(key), capture(valueCapture), capture(callbackCapture));
        expectLastCall()
                .andAnswer(() -> {
                    callbackCapture.getValue().onCompletion(null, new TimeoutException());
                    return null;
                })
                .andAnswer(() -> {
                    callbackCapture.getValue().onCompletion(null, null);
                    return null;
                });

        replayAll();
        store.put(topicStatus);

        // state is not visible until read back from the log
        assertEquals(null, store.getTopic(FOO_CONNECTOR, FOO_TOPIC));
        assertEquals(topicStatus, store.parseTopicStatus(valueCapture.getValue()));

        verifyAll();
    }

}
