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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.PollResult;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent.Type;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class MetadataErrorManagerTest {
    private Metadata metadata;
    private BackgroundEventHandler backgroundEventHandler;
    private Queue<BackgroundEvent> backgroundEventQueue;
    private MetadataErrorManager metadataErrorManager;

    @BeforeEach
    void setUp() {
        this.metadata = new Metadata(50,
                50,
                5000,
                new LogContext(),
                new ClusterResourceListeners());
        this.backgroundEventQueue = new LinkedList<>();
        this.backgroundEventHandler = new BackgroundEventHandler(backgroundEventQueue);
        this.metadataErrorManager = new MetadataErrorManager(metadata, backgroundEventHandler);
    }

    @Test
    void testNoMetadataError() {
        PollResult empty = metadataErrorManager.poll(0);
        BackgroundEvent event = backgroundEventQueue.poll();

        assertEquals(PollResult.EMPTY, empty);
        assertNull(event);
    }

    @Test
    void testMetadataError() {
        Time time = new MockTime();
        String invalidTopic = "invalid topic";
        MetadataResponse invalidTopicResponse = RequestTestUtils.metadataUpdateWith("clusterId", 1,
                Collections.singletonMap(invalidTopic, Errors.INVALID_TOPIC_EXCEPTION), Collections.emptyMap());
        metadata.updateWithCurrentRequestVersion(invalidTopicResponse, false, time.milliseconds());

        PollResult empty = metadataErrorManager.poll(0);
        BackgroundEvent event = backgroundEventQueue.poll();

        assertEquals(PollResult.EMPTY, empty);
        assertNotNull(event);
        assertEquals(Type.ERROR, event.type());
        assertEquals(InvalidTopicException.class, ((ErrorEvent) event).error().getClass());
        assertEquals(String.format("Invalid topics: [%s]", invalidTopic),
                ((ErrorEvent) event).error().getMessage());
    }
}