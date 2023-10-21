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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MirrorUtilsTest {

    private static final String TOPIC = "topic";

    private final Admin admin = mock(Admin.class);
    private final CreateTopicsResult ctr = mock(CreateTopicsResult.class);
    @SuppressWarnings("unchecked")
    private final KafkaFuture<Void> future = mock(KafkaFuture.class);

    @Test
    public void testCreateCompactedTopic() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenReturn(null);
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin);

        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }

    @Test
    public void testCreateCompactedTopicAlreadyExists() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenThrow(new ExecutionException(new TopicExistsException("topic exists")));
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin);

        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }

    @Test
    public void testCreateCompactedTopicAssumeTopicAlreadyExistsWithUnsupportedVersionException() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenThrow(new ExecutionException(new UnsupportedVersionException("unsupported")));
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin);

        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }

    @Test
    public void testCreateCompactedTopicAssumeTopicAlreadyExistsWithClusterAuthorizationException() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenThrow(new ExecutionException(new ClusterAuthorizationException("not authorised")));
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin);

        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }

    @Test
    public void testCreateCompactedTopicAssumeTopicAlreadyExistsWithTopicAuthorizationException() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenThrow(new ExecutionException(new TopicAuthorizationException("not authorised")));
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin);

        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }

    @Test
    public void testCreateCompactedTopicFailsWithInvalidConfigurationException() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenThrow(new ExecutionException(new InvalidConfigurationException("wrong config")));
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        Throwable ce = assertThrows(ConnectException.class, () -> MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin), "Should have exception thrown");

        assertTrue(ce.getCause() instanceof InvalidConfigurationException);
        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }

    @Test
    public void testCreateCompactedTopicFailsWithTimeoutException() throws Exception {
        Map<String, KafkaFuture<Void>> values = Collections.singletonMap(TOPIC, future);
        when(future.get()).thenThrow(new ExecutionException(new TimeoutException("Timeout")));
        when(ctr.values()).thenReturn(values);
        when(admin.createTopics(any(), any())).thenReturn(ctr);
        Throwable ce = assertThrows(ConnectException.class, () -> MirrorUtils.createCompactedTopic(TOPIC, (short) 1, (short) 1, admin), "Should have exception thrown");

        assertTrue(ce.getCause() instanceof TimeoutException);
        verify(future).get();
        verify(ctr).values();
        verify(admin).createTopics(any(), any());
    }
}
