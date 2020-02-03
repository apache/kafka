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
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.TopicStatus;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KafkaStatusBackingStoreFormatTest extends EasyMockSupport {

    private static final String STATUS_TOPIC = "status-topic";

    private JsonConverter converter;
    private KafkaStatusBackingStore store;

    @Before
    public void setup() {
        converter = new JsonConverter();
        converter.configure(Collections.singletonMap(SCHEMAS_ENABLE_CONFIG, false), false);
        store = new KafkaStatusBackingStore(new MockTime(), converter, STATUS_TOPIC, null);
    }

    @Test
    public void readTopicStatus() {
        TopicStatus topicStatus = new TopicStatus("foo", new ConnectorTaskId("bar", 0), Time.SYSTEM.milliseconds());
        byte[] value = store.serializeTopicStatus(topicStatus);
        ConsumerRecord<String, byte[]> statusRecord = new ConsumerRecord<>(STATUS_TOPIC, 0, 0, "status-topic-foo:connector-bar", value);
        store.read(statusRecord);
        assertTrue(store.topics.containsKey("bar"));
        assertTrue(store.topics.get("bar").containsKey("foo"));
        assertEquals(topicStatus, store.topics.get("bar").get("foo"));
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
}
