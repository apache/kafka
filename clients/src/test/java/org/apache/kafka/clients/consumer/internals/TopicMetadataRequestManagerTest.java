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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.spy;

public class TopicMetadataRequestManagerTest {
    private MockTime time;
    private TopicMetadataRequestManager topicMetadataRequestManager;
    private int defaultApiTimeoutMs;

    @BeforeEach
    public void setup() {
        this.time = new MockTime();
        Properties props = new Properties();
        props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
        props.put(ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        ConsumerConfig config = new ConsumerConfig(props);
        this.topicMetadataRequestManager = spy(new TopicMetadataRequestManager(
            new LogContext(),
            config));
        this.defaultApiTimeoutMs = config.getInt(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG);
    }

    @Test
    public void testPoll_SuccessfulRequestTopicMetadata() {
        String topic = "hello";
        Timer timer = time.timer(defaultApiTimeoutMs);
        this.topicMetadataRequestManager.requestTopicMetadata(topic, timer);
        timer.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
    }

    @Test
    public void testPoll_SuccessfulRequestAllTopicsMetadata() {
        Timer timer = time.timer(defaultApiTimeoutMs);
        this.topicMetadataRequestManager.requestAllTopicsMetadata(timer);
        timer.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    public void testTopicExceptionAndInflightRequests(final Errors error, final boolean shouldRetry) {
        String topic = "hello";
        Timer timer = time.timer(defaultApiTimeoutMs);
        this.topicMetadataRequestManager.requestTopicMetadata(topic, timer);
        timer.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        res.unsentRequests.get(0).future().complete(buildTopicMetadataClientResponse(
            res.unsentRequests.get(0),
            topic,
            error,
            timer));
        List<TopicMetadataRequestManager.TopicMetadataRequestState> inflights = this.topicMetadataRequestManager.inflightRequests();

        if (shouldRetry) {
            assertEquals(1, inflights.size());
            assertEquals(topic, inflights.get(0).topic());
        } else {
            assertEquals(0, inflights.size());
        }
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    public void testAllTopicsExceptionAndInflightRequests(final Errors error, final boolean shouldRetry) {
        Timer timer = time.timer(defaultApiTimeoutMs);
        this.topicMetadataRequestManager.requestAllTopicsMetadata(timer);
        timer.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        res.unsentRequests.get(0).future().complete(buildAllTopicsMetadataClientResponse(
                res.unsentRequests.get(0),
                error,
                timer));
        List<TopicMetadataRequestManager.TopicMetadataRequestState> inflights = this.topicMetadataRequestManager.inflightRequests();

        if (shouldRetry) {
            assertEquals(1, inflights.size());
        } else {
            assertEquals(0, inflights.size());
        }
    }

    @Test
    public void testExpiringRequest() {
        String topic = "hello";

        // Request topic metadata with 1000ms expiration
        Timer timer = time.timer(1000);
        CompletableFuture<Map<String, List<PartitionInfo>>> future =
            this.topicMetadataRequestManager.requestTopicMetadata(topic, timer);
        assertEquals(1, this.topicMetadataRequestManager.inflightRequests().size());

        // Poll the request manager to get the list of requests to send
        // - fail the request with a RetriableException
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildTopicMetadataClientResponse(
            res.unsentRequests.get(0),
            topic,
            Errors.REQUEST_TIMED_OUT,
            timer));

        // Sleep for long enough to exceed the backoff delay but still within the expiration
        // - fail the request again with a RetriableException
        timer.sleep(500);
        res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        res.unsentRequests.get(0).future().complete(buildTopicMetadataClientResponse(
            res.unsentRequests.get(0),
            topic,
            Errors.REQUEST_TIMED_OUT,
            timer));

        // Sleep for long enough to expire the request which should fail
        timer.sleep(1000);
        res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res.unsentRequests.size());
        assertEquals(0, this.topicMetadataRequestManager.inflightRequests().size());
        assertTrue(future.isCompletedExceptionally());
    }

    @ParameterizedTest
    @MethodSource("hardFailureExceptionProvider")
    public void testHardFailures(Exception exception) {
        String topic = "hello";
        Timer timer = time.timer(defaultApiTimeoutMs);
        this.topicMetadataRequestManager.requestTopicMetadata(topic, timer);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());

        res.unsentRequests.get(0).future().completeExceptionally(exception);

        if (exception instanceof RetriableException) {
            assertFalse(topicMetadataRequestManager.inflightRequests().isEmpty());
        } else {
            assertTrue(topicMetadataRequestManager.inflightRequests().isEmpty());
        }
    }

    @Test
    public void testNetworkTimeout() {
        String topic = "hello";
        Timer timer = time.timer(defaultApiTimeoutMs);
        topicMetadataRequestManager.requestTopicMetadata(topic, timer);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.PollResult res2 = this.topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res2.unsentRequests.size());

        // Mimic a network timeout
        res.unsentRequests.get(0).handler().onFailure(timer.currentTimeMs(), new TimeoutException());

        long backoffMs = topicMetadataRequestManager.inflightRequests().get(0).remainingBackoffMs(timer.currentTimeMs());
        // Sleep for exponential backoff - 1ms
        timer.sleep(backoffMs - 1);
        res2 = topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(0, res2.unsentRequests.size());

        timer.sleep(1);
        res2 = topicMetadataRequestManager.poll(timer.currentTimeMs());
        assertEquals(1, res2.unsentRequests.size());

        res2.unsentRequests.get(0).future().complete(buildTopicMetadataClientResponse(
            res2.unsentRequests.get(0),
            topic,
            Errors.NONE,
            timer));
        assertTrue(topicMetadataRequestManager.inflightRequests().isEmpty());
    }

    private ClientResponse buildTopicMetadataClientResponse(
            final NetworkClientDelegate.UnsentRequest request,
            final String topic,
            final Errors error,
            final Timer timer) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof MetadataRequest);
        MetadataRequest metadataRequest = (MetadataRequest) abstractRequest;
        Cluster cluster = mockCluster(3, 0);
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        topics.add(new MetadataResponse.TopicMetadata(error, topic, false,
                Collections.emptyList()));
        final MetadataResponse metadataResponse = RequestTestUtils.metadataResponse(cluster.nodes(),
                cluster.clusterResource().clusterId(),
                cluster.controller().id(),
                topics);
        return new ClientResponse(
                new RequestHeader(ApiKeys.METADATA, metadataRequest.version(), "mockClientId", 1),
                request.handler(),
                "-1",
                timer.currentTimeMs(),
                timer.currentTimeMs(),
                false,
                null,
                null,
                metadataResponse);
    }

    private ClientResponse buildAllTopicsMetadataClientResponse(
        final NetworkClientDelegate.UnsentRequest request,
        final Errors error,
        final Timer timer) {
        AbstractRequest abstractRequest = request.requestBuilder().build();
        assertTrue(abstractRequest instanceof MetadataRequest);
        MetadataRequest metadataRequest = (MetadataRequest) abstractRequest;
        Cluster cluster = mockCluster(3, 0);
        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        topics.add(new MetadataResponse.TopicMetadata(error, "topic1", false,
            Collections.emptyList()));
        topics.add(new MetadataResponse.TopicMetadata(error, "topic2", false,
            Collections.emptyList()));
        final MetadataResponse metadataResponse = RequestTestUtils.metadataResponse(cluster.nodes(),
            cluster.clusterResource().clusterId(),
            cluster.controller().id(),
            topics);
        return new ClientResponse(
            new RequestHeader(ApiKeys.METADATA, metadataRequest.version(), "mockClientId", 1),
            request.handler(),
            "-1",
            timer.currentTimeMs(),
            timer.currentTimeMs(),
            false,
            null,
            null,
            metadataResponse);
    }

    private static Cluster mockCluster(final int numNodes, final int controllerIndex) {
        HashMap<Integer, Node> nodes = new HashMap<>();
        for (int i = 0; i < numNodes; i++)
            nodes.put(i, new Node(i, "localhost", 8121 + i));
        return new Cluster("mockClusterId", nodes.values(),
            Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), nodes.get(controllerIndex));
    }


    private static Collection<Arguments> exceptionProvider() {
        return Arrays.asList(
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false),
            Arguments.of(Errors.INVALID_TOPIC_EXCEPTION, false),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false),
            Arguments.of(Errors.NETWORK_EXCEPTION, true),
            Arguments.of(Errors.NONE, false));
    }

    private static Collection<Arguments> hardFailureExceptionProvider() {
        return Arrays.asList(
                Arguments.of(new TimeoutException("timeout")),
                Arguments.of(new KafkaException("non-retriable exception")),
                Arguments.of(new NetworkException("retriable-exception")));
    }

}
