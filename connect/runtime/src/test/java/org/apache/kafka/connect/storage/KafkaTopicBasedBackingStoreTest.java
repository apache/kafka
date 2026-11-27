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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.util.TopicAdmin;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaTopicBasedBackingStoreTest {

    private static final String TOPIC = "connect-offsets";
    private static final String TOPIC_CONFIG = "offset.storage.topic";
    private static final long TIMEOUT_MS = 1000;
    private static final long BACKOFF_MS = 100;
    private static final NewTopic TOPIC_DESCRIPTION = TopicAdmin.defineTopic(TOPIC).compacted().build();
    private static final TopicAdmin.TopicCreationResponse CREATED = new TopicAdmin.TopicCreationResponse(Set.of(TOPIC), Set.of());
    private static final TopicAdmin.TopicCreationResponse EXISTING = new TopicAdmin.TopicCreationResponse(Set.of(), Set.of(TOPIC));
    private static final TopicAdmin.TopicCreationResponse NOT_CREATED = new TopicAdmin.TopicCreationResponse(Set.of(), Set.of());

    private final MockTime time = new MockTime();
    private final TopicAdmin admin = mock(TopicAdmin.class);
    private Consumer<TopicAdmin> initializer;

    @BeforeEach
    public void setup() {
        WorkerConfig config = mock(WorkerConfig.class);
        when(config.internalTopicsCreationEnabled()).thenReturn(true);
        when(config.originals()).thenReturn(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, String.valueOf(TIMEOUT_MS),
                AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, String.valueOf(BACKOFF_MS)));

        KafkaTopicBasedBackingStore store = new KafkaTopicBasedBackingStore() {
            @Override
            protected String getTopicConfig() {
                return TOPIC_CONFIG;
            }

            @Override
            protected String getTopicPurpose() {
                return "source connector offsets";
            }
        };
        initializer = store.topicInitializer(TOPIC, TOPIC_DESCRIPTION, config, time);
    }

    @Test
    public void shouldNotVerifyConfigWhenTopicIsCreated() {
        when(admin.createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time))).thenReturn(CREATED);

        initializer.accept(admin);

        verify(admin, never()).describeTopics(TOPIC);
        verify(admin, never()).verifyTopicCleanupPolicyOnlyCompact(eq(TOPIC), anyString(), anyString());
    }

    @Test
    public void shouldVerifyConfigWithoutDescribeWhenTopicAlreadyExists() {
        when(admin.createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time))).thenReturn(EXISTING);

        initializer.accept(admin);

        verify(admin, never()).describeTopics(TOPIC);
        verify(admin).verifyTopicCleanupPolicyOnlyCompact(eq(TOPIC), eq(TOPIC_CONFIG), anyString());
    }

    @Test
    public void shouldVerifyConfigWhenTopicCouldNotBeCreatedButExists() {
        when(admin.createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time))).thenReturn(NOT_CREATED);
        when(admin.describeTopics(TOPIC)).thenReturn(Map.of(TOPIC, new TopicDescription(TOPIC, false, List.of())));

        initializer.accept(admin);

        verify(admin, times(1)).createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time));
        verify(admin).verifyTopicCleanupPolicyOnlyCompact(eq(TOPIC), eq(TOPIC_CONFIG), anyString());
    }

    @Test
    public void shouldRetryWhenTopicIsMissingAndCouldNotBeCreated() {
        when(admin.createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time)))
                .thenReturn(NOT_CREATED)
                .thenReturn(NOT_CREATED)
                .thenReturn(CREATED);
        when(admin.describeTopics(TOPIC)).thenReturn(Map.of());
        long startMs = time.milliseconds();

        initializer.accept(admin);

        verify(admin, times(3)).createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time));
        verify(admin, never()).verifyTopicCleanupPolicyOnlyCompact(eq(TOPIC), anyString(), anyString());
        assertEquals(2 * BACKOFF_MS, time.milliseconds() - startMs);
    }

    @Test
    public void shouldFailWhenTopicIsMissingAndCouldNotBeCreatedUntilTimeout() {
        when(admin.createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time))).thenReturn(NOT_CREATED);
        when(admin.describeTopics(TOPIC)).thenReturn(Map.of());

        ConnectException e = assertThrows(ConnectException.class, () -> initializer.accept(admin));

        assertTrue(e.getMessage().contains(TOPIC_CONFIG));
        verify(admin, times((int) (TIMEOUT_MS / BACKOFF_MS) + 1)).createTopicsWithRetry(eq(TOPIC_DESCRIPTION), anyLong(), anyLong(), eq(time));
        verify(admin, never()).verifyTopicCleanupPolicyOnlyCompact(eq(TOPIC), anyString(), anyString());
    }
}
