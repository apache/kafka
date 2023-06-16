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
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.AfterEach;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicMetadataRequestManagerTest {

    private ConsumerTestBuilder.DefaultEventHandlerTestBuilder testBuilder;
    private Time time;
    private TopicMetadataRequestManager topicMetadataRequestManager;

    @BeforeEach
    public void setup() {
        testBuilder = new ConsumerTestBuilder.DefaultEventHandlerTestBuilder();
        time = testBuilder.time;
        topicMetadataRequestManager = testBuilder.topicMetadataRequestManager;
    }

    @AfterEach
    public void tearDown() {
        if (testBuilder != null)
            testBuilder.close();
    }

    @ParameterizedTest
    @MethodSource("topicsProvider")
    public void testPoll_SuccessfulRequestTopicMetadata(Optional<String> topic) {
        this.topicMetadataRequestManager.requestTopicMetadata(topic);
        this.time.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(this.time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
    }

    @ParameterizedTest
    @MethodSource("exceptionProvider")
    public void testExceptionAndInflightRequests(final Errors error, final boolean shouldRetry) {
        String topic = "hello";
        this.topicMetadataRequestManager.requestTopicMetadata(Optional.of("hello"));
        this.time.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(this.time.milliseconds());
        assertEquals(1, res.unsentRequests.size());
        NetworkClientDelegate.UnsentRequest request = res.unsentRequests.get(0);
        request.future().complete(buildTopicMetadataClientResponse(request, topic, error));
        List<TopicMetadataRequestManager.CompletableTopicMetadataRequest> inflights = this.topicMetadataRequestManager.inflightRequests();

        if (shouldRetry) {
            assertEquals(1, inflights.size());
            assertEquals(topic, inflights.get(0).topic());
        } else {
            assertEquals(0, inflights.size());
        }
    }

    @Test
    public void testEnsureRequestRemovedFromInflightsOnErrorResponse() {
        this.topicMetadataRequestManager.requestTopicMetadata(Optional.of("hello"));
        this.time.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(this.time.milliseconds());
        res.unsentRequests.get(0).future().completeExceptionally(new KafkaException("some error"));

        List<TopicMetadataRequestManager.CompletableTopicMetadataRequest> inflights = this.topicMetadataRequestManager.inflightRequests();
        assertTrue(inflights.isEmpty());
    }

    @Test
    public void testSendingTheSameRequest() {
        final String topic = "hello";
        CompletableFuture<Map<String, List<PartitionInfo>>> future = this.topicMetadataRequestManager.requestTopicMetadata(Optional.of(topic));
        CompletableFuture<Map<String, List<PartitionInfo>>> future2 =
            this.topicMetadataRequestManager.requestTopicMetadata(Optional.of(topic));
        this.time.sleep(100);
        NetworkClientDelegate.PollResult res = this.topicMetadataRequestManager.poll(this.time.milliseconds());
        assertEquals(1, res.unsentRequests.size());

        NetworkClientDelegate.UnsentRequest request = res.unsentRequests.get(0);
        request.future().complete(buildTopicMetadataClientResponse(request, topic, Errors.NONE));

        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        assertTrue(future2.isDone());
        assertFalse(future2.isCompletedExceptionally());
    }

    private ClientResponse buildTopicMetadataClientResponse(
        final NetworkClientDelegate.UnsentRequest request,
        final String topic,
        final Errors error) {
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
            request.callback(),
            "-1",
            time.milliseconds(),
            time.milliseconds(),
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


    private static Collection<Arguments> topicsProvider() {
        return Arrays.asList(
            Arguments.of(Optional.of("topic1")),
            Arguments.of(Optional.empty()));
    }

    private static Collection<Arguments> exceptionProvider() {
        return Arrays.asList(
            Arguments.of(Errors.UNKNOWN_TOPIC_OR_PARTITION, false),
            Arguments.of(Errors.INVALID_TOPIC_EXCEPTION, false),
            Arguments.of(Errors.UNKNOWN_SERVER_ERROR, false),
            Arguments.of(Errors.NETWORK_EXCEPTION, true),
            Arguments.of(Errors.NONE, false));
    }
}
