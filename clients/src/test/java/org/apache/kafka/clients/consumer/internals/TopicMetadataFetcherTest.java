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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

public class TopicMetadataFetcherTest {

    private final String topicName = "test";
    private final Uuid topicId = Uuid.randomUuid();
    private final Map<String, Uuid> topicIds = new HashMap<String, Uuid>() {
        {
            put(topicName, topicId);
        }
    };
    private final TopicPartition tp0 = new TopicPartition(topicName, 0);
    private final int validLeaderEpoch = 0;
    private final MetadataResponse initialUpdateResponse =
        RequestTestUtils.metadataUpdateWithIds(1, singletonMap(topicName, 4), topicIds);

    private MockTime time = new MockTime(1);
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private MockClient client;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private TopicMetadataFetcher topicMetadataFetcher;

    @BeforeEach
    public void setup() {
    }

    private void assignFromUser(Set<TopicPartition> partitions) {
        subscriptions.assignFromUser(partitions);
        client.updateMetadata(initialUpdateResponse);

        // A dummy metadata update to ensure valid leader epoch.
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds("dummy", 1,
            Collections.emptyMap(), singletonMap(topicName, 4),
            tp -> validLeaderEpoch, topicIds), false, 0L);
    }

    @AfterEach
    public void teardown() throws Exception {
        if (metrics != null)
            this.metrics.close();
    }

    @Test
    public void testGetAllTopics() {
        // sending response before request, as getTopicMetadata is a blocking call
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(Errors.NONE));

        Map<String, List<PartitionInfo>> allTopics = topicMetadataFetcher.getAllTopicMetadata(time.timer(5000L));

        assertEquals(initialUpdateResponse.topicMetadata().size(), allTopics.size());
    }

    @Test
    public void testGetAllTopicsDisconnect() {
        // first try gets a disconnect, next succeeds
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(null, true);
        client.prepareResponse(newMetadataResponse(Errors.NONE));
        Map<String, List<PartitionInfo>> allTopics = topicMetadataFetcher.getAllTopicMetadata(time.timer(5000L));
        assertEquals(initialUpdateResponse.topicMetadata().size(), allTopics.size());
    }

    @Test
    public void testGetAllTopicsTimeout() {
        // since no response is prepared, the request should time out
        buildFetcher();
        assignFromUser(singleton(tp0));
        assertThrows(TimeoutException.class, () -> topicMetadataFetcher.getAllTopicMetadata(time.timer(50L)));
    }

    @Test
    public void testGetAllTopicsUnauthorized() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(Errors.TOPIC_AUTHORIZATION_FAILED));
        try {
            topicMetadataFetcher.getAllTopicMetadata(time.timer(10L));
            fail();
        } catch (TopicAuthorizationException e) {
            assertEquals(singleton(topicName), e.unauthorizedTopics());
        }
    }

    @Test
    public void testGetTopicMetadataInvalidTopic() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(Errors.INVALID_TOPIC_EXCEPTION));
        assertThrows(InvalidTopicException.class, () -> topicMetadataFetcher.getTopicMetadata(topicName, true, time.timer(5000L)));
    }

    @Test
    public void testGetTopicMetadataUnknownTopic() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION));

        List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topicName, true, time.timer(5000L));
        assertNull(topicMetadata);
    }

    @Test
    public void testGetTopicMetadataLeaderNotAvailable() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        client.prepareResponse(newMetadataResponse(Errors.LEADER_NOT_AVAILABLE));
        client.prepareResponse(newMetadataResponse(Errors.NONE));

        List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topicName, true, time.timer(5000L));
        assertNotNull(topicMetadata);
    }

    @Test
    public void testGetTopicMetadataOfflinePartitions() {
        buildFetcher();
        assignFromUser(singleton(tp0));
        MetadataResponse originalResponse = newMetadataResponse(Errors.NONE); //baseline ok response

        //create a response based on the above one with all partitions being leaderless
        List<MetadataResponse.TopicMetadata> altTopics = new ArrayList<>();
        for (MetadataResponse.TopicMetadata item : originalResponse.topicMetadata()) {
            List<MetadataResponse.PartitionMetadata> partitions = item.partitionMetadata();
            List<MetadataResponse.PartitionMetadata> altPartitions = new ArrayList<>();
            for (MetadataResponse.PartitionMetadata p : partitions) {
                altPartitions.add(new MetadataResponse.PartitionMetadata(
                    p.error,
                    p.topicPartition,
                    Optional.empty(), //no leader
                    Optional.empty(),
                    p.replicaIds,
                    p.inSyncReplicaIds,
                    p.offlineReplicaIds
                ));
            }
            MetadataResponse.TopicMetadata alteredTopic = new MetadataResponse.TopicMetadata(
                item.error(),
                item.topic(),
                item.isInternal(),
                altPartitions
            );
            altTopics.add(alteredTopic);
        }
        Node controller = originalResponse.controller();
        MetadataResponse altered = RequestTestUtils.metadataResponse(
            originalResponse.brokers(),
            originalResponse.clusterId(),
            controller != null ? controller.id() : MetadataResponse.NO_CONTROLLER_ID,
            altTopics);

        client.prepareResponse(altered);

        List<PartitionInfo> topicMetadata = topicMetadataFetcher.getTopicMetadata(topicName, false, time.timer(5000L));

        assertNotNull(topicMetadata);
        assertFalse(topicMetadata.isEmpty());
        //noinspection ConstantConditions
        assertEquals(metadata.fetch().partitionCountForTopic(topicName).longValue(), topicMetadata.size());
    }

    private MetadataResponse newMetadataResponse(Errors error) {
        List<MetadataResponse.PartitionMetadata> partitionsMetadata = new ArrayList<>();
        if (error == Errors.NONE) {
            Optional<MetadataResponse.TopicMetadata> foundMetadata = initialUpdateResponse.topicMetadata()
                    .stream()
                    .filter(topicMetadata -> topicMetadata.topic().equals(topicName))
                    .findFirst();
            foundMetadata.ifPresent(topicMetadata -> partitionsMetadata.addAll(topicMetadata.partitionMetadata()));
        }

        MetadataResponse.TopicMetadata topicMetadata = new MetadataResponse.TopicMetadata(error, topicName, false,
                partitionsMetadata);
        List<Node> brokers = new ArrayList<>(initialUpdateResponse.brokers());
        return RequestTestUtils.metadataResponse(brokers, initialUpdateResponse.clusterId(),
                initialUpdateResponse.controller().id(), Collections.singletonList(topicMetadata));
    }

    private void buildFetcher() {
        MetricConfig metricConfig = new MetricConfig();
        long metadataExpireMs = Long.MAX_VALUE;
        long retryBackoffMs = 100;
        long retryBackoffMaxMs = 1000;
        LogContext logContext = new LogContext();
        SubscriptionState subscriptionState = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        buildDependencies(metricConfig, metadataExpireMs, subscriptionState, logContext);
        topicMetadataFetcher = new TopicMetadataFetcher(logContext, consumerClient, retryBackoffMs, retryBackoffMaxMs);
    }

    private void buildDependencies(MetricConfig metricConfig,
                                   long metadataExpireMs,
                                   SubscriptionState subscriptionState,
                                   LogContext logContext) {
        time = new MockTime(1);
        subscriptions = subscriptionState;
        metadata = new ConsumerMetadata(0, 0, metadataExpireMs, false, false,
                subscriptions, logContext, new ClusterResourceListeners());
        client = new MockClient(time, metadata);
        metrics = new Metrics(metricConfig, time);
        consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time,
                100, 1000, Integer.MAX_VALUE);
    }
}
