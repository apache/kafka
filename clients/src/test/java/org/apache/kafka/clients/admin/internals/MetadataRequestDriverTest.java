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

import org.apache.kafka.clients.admin.internals.RequestDriver.RequestSpec;
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
import java.util.Optional;
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
        List<RequestSpec<TopicPartition>> requests = driver.poll();
        assertEquals(1, requests.size());

        // While a Metadata request is inflight, we will not send another
        assertEquals(0, driver.poll().size());

        RequestSpec<TopicPartition> spec = requests.get(0);
        assertEquals(topicPartitions, spec.keys);
        assertExpectedBackoffAndDeadline(spec, 0);
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
        List<RequestSpec<TopicPartition>> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestSpec<TopicPartition> metadataSpec = requests1.iterator().next();
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
        List<RequestSpec<TopicPartition>> requests2 = driver.poll();
        assertEquals(2, requests2.size());

        RequestSpec<TopicPartition> spec0 = lookupRequest(requests2, tp0);
        assertEquals(mkSet(tp0), spec0.keys);
        assertEquals(OptionalInt.of(1), spec0.scope.destinationBrokerId());

        RequestSpec<TopicPartition> spec1 = lookupRequest(requests2, tp2);
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
        List<RequestSpec<TopicPartition>> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestSpec<TopicPartition> metadataSpec = requests1.iterator().next();
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

        List<RequestSpec<TopicPartition>> requests2 = driver.poll();
        assertEquals(2, requests2.size());

        // We should retry the Metadata request with only one of the two topics
        RequestSpec<TopicPartition> spec0 = lookupRequest(requests2, tp0);
        assertEquals(mkSet(tp0), spec0.keys);
        assertEquals(OptionalInt.empty(), spec0.scope.destinationBrokerId());
        assertTrue(spec0.request instanceof MetadataRequest.Builder);
        assertExpectedBackoffAndDeadline(spec0, 1);
        MetadataRequest.Builder retryMetadataRequest = (MetadataRequest.Builder) spec0.request;
        assertEquals(mkSet("foo"), new HashSet<>(retryMetadataRequest.topics()));

        RequestSpec<TopicPartition> spec1 = lookupRequest(requests2, tp2);
        assertEquals(mkSet(tp2), spec1.keys);
        assertEquals(OptionalInt.of(3), spec1.scope.destinationBrokerId());
    }

    @Test
    public void testFatalTopicError() {
        TopicPartition tp0 = new TopicPartition("foo", 0);
        TopicPartition tp2 = new TopicPartition("bar", 0);
        Set<TopicPartition> topicPartitions = mkSet(tp0, tp2);

        TestMetadataRequestDriver driver = new TestMetadataRequestDriver(topicPartitions);
        List<RequestSpec<TopicPartition>> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestSpec<TopicPartition> metadataSpec = requests1.iterator().next();
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

        List<RequestSpec<TopicPartition>> requests2 = driver.poll();
        assertEquals(1, requests2.size());

        // The lookup for "foo" should fail and not be retried
        TopicAuthorizationException topicAuthorizationException = TestUtils.assertFutureThrows(
            driver.futures().get(tp0), TopicAuthorizationException.class);
        assertEquals(mkSet("foo"), topicAuthorizationException.unauthorizedTopics());

        // However, the lookup for the other other topic should proceed
        RequestSpec<TopicPartition> spec = requests2.get(0);
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
        List<RequestSpec<TopicPartition>> requests1 = driver.poll();
        assertEquals(1, requests1.size());

        RequestSpec<TopicPartition> metadataSpec = requests1.iterator().next();

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

        List<RequestSpec<TopicPartition>> requests2 = driver.poll();
        assertEquals(1, requests2.size());

        // The lookup for "foo-0" should fail and not be retried
        TestUtils.assertFutureThrows(driver.futures().get(tp0), UnknownServerException.class);

        // However, the lookup for the other other partition should proceed
        RequestSpec<TopicPartition> spec = requests2.get(0);
        assertEquals(mkSet(tp2), spec.keys);
        assertEquals(OptionalInt.of(3), spec.scope.destinationBrokerId());
    }

    private RequestSpec<TopicPartition> lookupRequest(
        List<RequestSpec<TopicPartition>> requests,
        TopicPartition key
    ) {
        Optional<RequestSpec<TopicPartition>> foundRequestOpt = requests.stream()
            .filter(spec -> spec.keys.contains(key))
            .findFirst();
        assertTrue(foundRequestOpt.isPresent());
        return foundRequestOpt.get();
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

    private void assertExpectedBackoffAndDeadline(
        RequestSpec<TopicPartition> requestSpec,
        int expectedTries
    ) {
        assertEquals(expectedTries, requestSpec.tries);
        assertEquals(deadlineMs, requestSpec.deadlineMs);
        if (expectedTries == 0) {
            assertEquals(0, requestSpec.nextAllowedTryMs);
        } else {
            assertEquals(time.milliseconds() + (expectedTries * retryBackoffMs), requestSpec.nextAllowedTryMs);
        }
    }
}
