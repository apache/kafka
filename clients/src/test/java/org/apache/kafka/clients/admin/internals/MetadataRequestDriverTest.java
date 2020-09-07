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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DescribeProducersRequest;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.CollectionUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.admin.internals.AdminRequestUtil.metadataResponse;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MetadataRequestDriverTest {
    private final MockTime time = new MockTime();
    private final long deadlineMs = time.milliseconds() + 10000;
    private final long retryBackoffMs = 100;

    @Test
    public void testLookupGrouping() {
        Set<TopicPartition> topicPartitions = mkSet(
            new TopicPartition("foo", 0),
            new TopicPartition("foo", 2),
            new TopicPartition("bar", 1));

        TestMetadataRequestDriver driver = new TestMetadataRequestDriver(topicPartitions);
        List<RequestDriver<TopicPartition, String>.RequestSpec> requests = driver.poll();
        assertEquals(1, requests.size());

        // While a Metadata request is inflight, we will not send another
        assertEquals(0, driver.poll().size());

        RequestDriver<TopicPartition, String>.RequestSpec spec = requests.get(0);
        assertEquals(topicPartitions, spec.keys);
        assertEquals(0, spec.tries);
        assertEquals(deadlineMs, spec.deadlineMs);
        assertEquals(0, spec.nextAllowedTryMs);
        assertTrue(spec.request instanceof MetadataRequest.Builder);

        MetadataRequest.Builder metadataRequest = (MetadataRequest.Builder) spec.request;
        assertEquals(mkSet("foo", "bar"), new HashSet<>(metadataRequest.topics()));
    }

    @Test
    public void testSuccessfulLeaderDiscovery() {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        TopicPartition tp2 = new TopicPartition("foo", 2);

        // Request includes 2 of 3 partitions for the topic
        Set<TopicPartition> topicPartitions = mkSet(tp0, tp2);

        TestMetadataRequestDriver driver = new TestMetadataRequestDriver(topicPartitions);
        List<RequestDriver<TopicPartition, String>.RequestSpec> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestDriver<TopicPartition, String>.RequestSpec metadataSpec = requests1.iterator().next();
        driver.onResponse(time.milliseconds(), metadataSpec, new MetadataResponse(metadataResponse(Utils.mkMap(
            mkEntry(tp0, new MetadataResponsePartition()
               .setErrorCode(Errors.NONE.code())
               .setLeaderId(1)
               .setLeaderEpoch(15)
               .setIsrNodes(asList(1, 2, 3))
               .setReplicaNodes(asList(1, 2, 3))),
            mkEntry(tp1, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(2)
                .setLeaderEpoch(37)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3))),
            mkEntry(tp2, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(3)
                .setLeaderEpoch(99)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3)))
        ))));

        // We should have two fulfillment requests now sent to the leaders of partitions 0 and 2
        List<RequestDriver<TopicPartition, String>.RequestSpec> requests2 = driver.poll();
        assertEquals(2, requests2.size());

        RequestDriver<TopicPartition, String>.RequestSpec spec0 = requests2.stream()
            .filter(spec -> spec.keys.contains(tp0))
            .findFirst()
            .get();
        assertEquals(mkSet(tp0), spec0.keys);
        assertEquals(OptionalInt.of(1), spec0.scope.destinationBrokerId());

        RequestDriver<TopicPartition, String>.RequestSpec spec1 = requests2.stream()
            .filter(spec -> spec.keys.contains(tp2))
            .findFirst()
            .get();
        assertEquals(mkSet(tp2), spec1.keys);
        assertEquals(OptionalInt.of(3), spec1.scope.destinationBrokerId());
    }

    @Test
    public void testRetryLeaderDiscovery() {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        TopicPartition tp2 = new TopicPartition("bar", 0);

        // Request includes 2 of 3 partitions for the topic
        Set<TopicPartition> topicPartitions = mkSet(tp0, tp2);

        TestMetadataRequestDriver driver = new TestMetadataRequestDriver(topicPartitions);
        List<RequestDriver<TopicPartition, String>.RequestSpec> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestDriver<TopicPartition, String>.RequestSpec metadataSpec = requests1.iterator().next();
        driver.onResponse(time.milliseconds(), metadataSpec, new MetadataResponse(metadataResponse(Utils.mkMap(
            mkEntry(tp0, new MetadataResponsePartition()
                .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code())),
            mkEntry(tp1, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(2)
                .setLeaderEpoch(37)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3))),
            mkEntry(tp2, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(3)
                .setLeaderEpoch(99)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3)))
        ))));

        List<RequestDriver<TopicPartition, String>.RequestSpec> requests2 = driver.poll();
        assertEquals(2, requests2.size());

        // We should retry the Metadata request with only one of the two topics
        RequestDriver<TopicPartition, String>.RequestSpec spec0 = requests2.stream()
            .filter(spec -> spec.keys.contains(tp0))
            .findFirst()
            .get();
        assertEquals(mkSet(tp0), spec0.keys);
        assertEquals(OptionalInt.empty(), spec0.scope.destinationBrokerId());
        assertTrue(spec0.request instanceof MetadataRequest.Builder);
        assertEquals(1, spec0.tries);
        assertEquals(time.milliseconds() + retryBackoffMs, spec0.nextAllowedTryMs);
        MetadataRequest.Builder retryMetadataRequest = (MetadataRequest.Builder) spec0.request;
        assertEquals(mkSet("foo"), new HashSet<>(retryMetadataRequest.topics()));

        RequestDriver<TopicPartition, String>.RequestSpec spec1 = requests2.stream()
            .filter(spec -> spec.keys.contains(tp2))
            .findFirst()
            .get();
        assertEquals(mkSet(tp2), spec1.keys);
        assertEquals(OptionalInt.of(3), spec1.scope.destinationBrokerId());
    }

    @Test
    public void testFatalTopicError() {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp2 = new TopicPartition("bar", 0);
        Set<TopicPartition> topicPartitions = mkSet(tp0, tp2);

        TestMetadataRequestDriver driver = new TestMetadataRequestDriver(topicPartitions);
        List<RequestDriver<TopicPartition, String>.RequestSpec> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestDriver<TopicPartition, String>.RequestSpec metadataSpec = requests1.iterator().next();
        MetadataResponseData metadataResponseData = metadataResponse(Utils.mkMap(
            mkEntry(tp2, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(3)
                .setLeaderEpoch(99)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3)))
        ));

        metadataResponseData.topics().add(new MetadataResponseTopic()
            .setName("foo")
            .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code())
        );

        driver.onResponse(time.milliseconds(), metadataSpec, new MetadataResponse(metadataResponseData));

        List<RequestDriver<TopicPartition, String>.RequestSpec> requests2 = driver.poll();
        assertEquals(1, requests2.size());

        // The lookup for "foo" should fail and not be retried
        TopicAuthorizationException topicAuthorizationException = TestUtils.assertFutureThrows(
            driver.futures().get(tp0), TopicAuthorizationException.class);
        assertEquals(mkSet("foo"), topicAuthorizationException.unauthorizedTopics());

        // However, the lookup for the other other topic should proceed
        RequestDriver<TopicPartition, String>.RequestSpec spec = requests2.get(0);
        assertEquals(mkSet(tp2), spec.keys);
        assertEquals(OptionalInt.of(3), spec.scope.destinationBrokerId());
    }

    @Test
    public void testFatalPartitionError() {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp1 = new TopicPartition("foo", 1);
        TopicPartition tp2 = new TopicPartition("bar", 0);

        // Request includes 2 of 3 partitions for the topic
        Set<TopicPartition> topicPartitions = mkSet(tp0, tp2);

        TestMetadataRequestDriver driver = new TestMetadataRequestDriver(topicPartitions);
        List<RequestDriver<TopicPartition, String>.RequestSpec> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestDriver<TopicPartition, String>.RequestSpec metadataSpec = requests1.iterator().next();

        // Any unexpected partition error will cause the partition to fail
        driver.onResponse(time.milliseconds(), metadataSpec, new MetadataResponse(metadataResponse(Utils.mkMap(
            mkEntry(tp0, new MetadataResponsePartition()
                .setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code())),
            mkEntry(tp1, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(2)
                .setLeaderEpoch(37)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3))),
            mkEntry(tp2, new MetadataResponsePartition()
                .setErrorCode(Errors.NONE.code())
                .setLeaderId(3)
                .setLeaderEpoch(99)
                .setIsrNodes(asList(1, 2, 3))
                .setReplicaNodes(asList(1, 2, 3)))
        ))));

        List<RequestDriver<TopicPartition, String>.RequestSpec> requests2 = driver.poll();
        assertEquals(1, requests2.size());

        // The lookup for "foo-0" should fail and not be retried
        TestUtils.assertFutureThrows(driver.futures().get(tp0), UnknownServerException.class);

        // However, the lookup for the other other partition should proceed
        RequestDriver<TopicPartition, String>.RequestSpec spec = requests2.get(0);
        assertEquals(mkSet(tp2), spec.keys);
        assertEquals(OptionalInt.of(3), spec.scope.destinationBrokerId());
    }

    private final class TestMetadataRequestDriver extends MetadataRequestDriver<String> {

        public TestMetadataRequestDriver(Collection<TopicPartition> futures) {
            super(futures, deadlineMs, retryBackoffMs);
        }

        @Override
        AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId, Set<TopicPartition> topicPartitions) {
            DescribeProducersRequestData request = new DescribeProducersRequestData();
            DescribeProducersRequest.Builder builder = new DescribeProducersRequest.Builder(request);

            CollectionUtils.groupPartitionsByTopic(
                topicPartitions,
                builder::addTopic,
                (topicRequest, partitionId) -> topicRequest.partitionIndexes().add(partitionId)
            );

            return builder;
        }

        @Override
        void handleFulfillmentResponse(Integer brokerId, Set<TopicPartition> keys, AbstractResponse response) {
            throw new UnsupportedOperationException();
        }
    }

}