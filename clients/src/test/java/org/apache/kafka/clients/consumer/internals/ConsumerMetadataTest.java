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

import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsumerMetadataTest {

    private final Node node = new Node(1, "localhost", 9092);
    private final SubscriptionState subscription = new SubscriptionState(new LogContext(), OffsetResetStrategy.EARLIEST);
    private final Time time = new MockTime();

    @Test
    public void testPatternSubscriptionNoInternalTopics() {
        testPatternSubscription(false);
    }

    @Test
    public void testPatternSubscriptionIncludeInternalTopics() {
        testPatternSubscription(true);
    }

    private void testPatternSubscription(boolean includeInternalTopics) {
        subscription.subscribe(Pattern.compile("__.*"), Optional.empty());
        ConsumerMetadata metadata = newConsumerMetadata(includeInternalTopics);

        MetadataRequest.Builder builder = metadata.newMetadataRequestBuilder();
        assertTrue(builder.isAllTopics());

        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        topics.add(topicMetadata("__consumer_offsets", true));
        topics.add(topicMetadata("__matching_topic", false));
        topics.add(topicMetadata("non_matching_topic", false));

        MetadataResponse response = RequestTestUtils.metadataResponse(singletonList(node),
            "clusterId", node.id(), topics);
        metadata.updateWithCurrentRequestVersion(response, false, time.milliseconds());

        if (includeInternalTopics)
            assertEquals(Utils.mkSet("__matching_topic", "__consumer_offsets"), metadata.fetch().topics());
        else
            assertEquals(Collections.singleton("__matching_topic"), metadata.fetch().topics());
    }

    @Test
    public void testUserAssignment() {
        subscription.assignFromUser(Utils.mkSet(
                new TopicPartition("foo", 0),
                new TopicPartition("bar", 0),
                new TopicPartition("__consumer_offsets", 0)));
        testBasicSubscription(Utils.mkSet("foo", "bar"), Utils.mkSet("__consumer_offsets"));

        subscription.assignFromUser(Utils.mkSet(
                new TopicPartition("baz", 0),
                new TopicPartition("__consumer_offsets", 0)));
        testBasicSubscription(Utils.mkSet("baz"), Utils.mkSet("__consumer_offsets"));
    }

    @Test
    public void testNormalSubscription() {
        subscription.subscribe(Utils.mkSet("foo", "bar", "__consumer_offsets"), Optional.empty());
        subscription.groupSubscribe(Utils.mkSet("baz", "foo", "bar", "__consumer_offsets"));
        testBasicSubscription(Utils.mkSet("foo", "bar", "baz"), Utils.mkSet("__consumer_offsets"));

        subscription.resetGroupSubscription();
        testBasicSubscription(Utils.mkSet("foo", "bar"), Utils.mkSet("__consumer_offsets"));
    }

    @Test
    public void testTransientTopics() {
        Map<String, Uuid> topicIds = new HashMap<>();
        topicIds.put("foo", Uuid.randomUuid());
        subscription.subscribe(singleton("foo"), Optional.empty());
        ConsumerMetadata metadata = newConsumerMetadata(false);
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds(1, singletonMap("foo", 1), topicIds), false, time.milliseconds());
        assertEquals(topicIds.get("foo"), metadata.topicIds().get("foo"));
        assertFalse(metadata.updateRequested());

        metadata.addTransientTopics(singleton("foo"));
        assertFalse(metadata.updateRequested());

        metadata.addTransientTopics(singleton("bar"));
        assertTrue(metadata.updateRequested());

        Map<String, Integer> topicPartitionCounts = new HashMap<>();
        topicPartitionCounts.put("foo", 1);
        topicPartitionCounts.put("bar", 1);
        topicIds.put("bar", Uuid.randomUuid());
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds(1, topicPartitionCounts, topicIds), false, time.milliseconds());
        Map<String, Uuid> metadataTopicIds = metadata.topicIds();
        topicIds.forEach((topicName, topicId) -> assertEquals(topicId, metadataTopicIds.get(topicName)));
        assertFalse(metadata.updateRequested());

        assertEquals(Utils.mkSet("foo", "bar"), new HashSet<>(metadata.fetch().topics()));

        metadata.clearTransientTopics();
        topicIds.remove("bar");
        metadata.updateWithCurrentRequestVersion(RequestTestUtils.metadataUpdateWithIds(1, topicPartitionCounts, topicIds), false, time.milliseconds());
        assertEquals(singleton("foo"), new HashSet<>(metadata.fetch().topics()));
        assertEquals(topicIds.get("foo"), metadata.topicIds().get("foo"));
        assertNull(topicIds.get("bar"));
    }

    private void testBasicSubscription(Set<String> expectedTopics, Set<String> expectedInternalTopics) {
        Set<String> allTopics = new HashSet<>();
        allTopics.addAll(expectedTopics);
        allTopics.addAll(expectedInternalTopics);

        ConsumerMetadata metadata = newConsumerMetadata(false);

        MetadataRequest.Builder builder = metadata.newMetadataRequestBuilder();
        assertEquals(allTopics, new HashSet<>(builder.topics()));

        List<MetadataResponse.TopicMetadata> topics = new ArrayList<>();
        for (String expectedTopic : expectedTopics)
            topics.add(topicMetadata(expectedTopic, false));
        for (String expectedInternalTopic : expectedInternalTopics)
            topics.add(topicMetadata(expectedInternalTopic, true));

        MetadataResponse response = RequestTestUtils.metadataResponse(singletonList(node),
            "clusterId", node.id(), topics);
        metadata.updateWithCurrentRequestVersion(response, false, time.milliseconds());

        assertEquals(allTopics, metadata.fetch().topics());
    }

    private MetadataResponse.TopicMetadata topicMetadata(String topic, boolean isInternal) {
        MetadataResponse.PartitionMetadata partitionMetadata = new MetadataResponse.PartitionMetadata(Errors.NONE,
                new TopicPartition(topic, 0), Optional.of(node.id()), Optional.of(5),
                singletonList(node.id()), singletonList(node.id()), singletonList(node.id()));
        return new MetadataResponse.TopicMetadata(Errors.NONE, topic, isInternal, singletonList(partitionMetadata));
    }

    private ConsumerMetadata newConsumerMetadata(boolean includeInternalTopics) {
        long refreshBackoffMs = 50;
        long expireMs = 50000;
        return new ConsumerMetadata(refreshBackoffMs, refreshBackoffMs, expireMs, includeInternalTopics, false,
                subscription, new LogContext(), new ClusterResourceListeners());
    }

}
