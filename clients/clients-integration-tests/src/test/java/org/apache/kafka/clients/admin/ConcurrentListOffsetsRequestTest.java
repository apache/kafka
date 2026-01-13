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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
        types = {Type.KRAFT},
        brokers = 3
)
public class ConcurrentListOffsetsRequestTest {
    private static final String TOPIC = "topic";
    private static final short REPLICAS = 1;
    private static final int PARTITION = 2;
    private static final int TIMEOUT = 1000;
    private static final long LATCH_TIMEOUT_MS = TIMEOUT * 5L;
    private final ClusterInstance clusterInstance;
    private Admin adminClient;
    private NetworkClient networkClient;
    private final AtomicBoolean injectHostResolverError = new AtomicBoolean(false);

    ConcurrentListOffsetsRequestTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws Exception {
        clusterInstance.waitForReadyBrokers();
        clusterInstance.createTopic(TOPIC, PARTITION, REPLICAS);
        Map<String, Object> props = Map.of(
                "default.api.timeout.ms", TIMEOUT,
                "request.timeout.ms", TIMEOUT,
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        adminClient = KafkaAdminClient.createInternal(new AdminClientConfig(clusterInstance.setClientSaslConfig(props), true),
                null, new TestHostResolver());

        Field clientField = KafkaAdminClient.class.getDeclaredField("client");
        clientField.setAccessible(true);
        networkClient = (NetworkClient) clientField.get(adminClient);
    }

    @AfterEach
    public void teardown() {
        Utils.closeQuietly(adminClient, "ListOffsetsAdminClient");
    }

    @ClusterTest
    public void correctlyHandleConcurrentModificationOfPartitionLeaderCache() throws Exception {
        // making one request to prepopulate the partition leader cache so we have something to delete later
        listAllOffsets().all().get(TIMEOUT * 2L, TimeUnit.MILLISECONDS);

        final CountDownLatch invalidationLatch = new CountDownLatch(1);
        // Replacing the partition leader cache in order to be able to synchronize the calls so that they happen in the right order to reproduce the issue
        SynchronizedTestMap partitionLeaderCache = replacePartitionLeaderCache(invalidationLatch);

        // closing the connection to the first node. not using clusterInstance.shutdownBroker to reduce flakiness
        networkClient.close(partitionLeaderCache.values().iterator().next().toString());
        // as next call with try to resolve the host for the closed node, it's time to let it fail, which will lead to cache invalidation
        injectHostResolverError.set(true);

        // making another request(this request will face the host resolver error and remove the node from the cache)
        ListOffsetsResult failInducingResult = listAllOffsets();
        // waiting until we get to the invalidation
        assertTrue(invalidationLatch.await(LATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS),
                "Timed out waiting for cache invalidation");
        // making another request. at this point the fail inducing request is waiting for this one before it deletes the keys associated with the node
        // the SynchronizedTestMap class synchronizes the calls to mimic the race condition
        ListOffsetsResult failingResult = listAllOffsets();

        // verifying that we correctly declined the call
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> failInducingResult.all().get(TIMEOUT * 2, TimeUnit.MILLISECONDS));
        assertInstanceOf(TimeoutException.class, executionException.getCause());

        // verifying that we correctly declined the call(here where it's failing, as the future for the partition we deleted from the cache before will never be completed)
        executionException = assertThrows(ExecutionException.class, () -> failingResult.all().get(TIMEOUT * 2, TimeUnit.MILLISECONDS));
        assertInstanceOf(TimeoutException.class, executionException.getCause());
    }

    @SuppressWarnings("unchecked")
    private SynchronizedTestMap replacePartitionLeaderCache(CountDownLatch latch0) throws Exception {
        Field partitionLeaderCacheField = KafkaAdminClient.class.getDeclaredField("partitionLeaderCache");
        partitionLeaderCacheField.setAccessible(true);
        Map<TopicPartition, Integer> q = (Map<TopicPartition, Integer>) partitionLeaderCacheField.get(adminClient);

        SynchronizedTestMap partitionLeaderCache = new SynchronizedTestMap(q, latch0);
        partitionLeaderCacheField.set(adminClient, partitionLeaderCache);
        return partitionLeaderCache;
    }

    private ListOffsetsResult listAllOffsets() {
        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < PARTITION; i++) {
            partitions.add(new TopicPartition(TOPIC, i));
        }

        Map<TopicPartition, OffsetSpec> offsetSpecMap = partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        return adminClient.listOffsets(offsetSpecMap, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED));
    }

    private static class SynchronizedTestMap extends HashMap<TopicPartition, Integer> {

        private final AtomicInteger containsKeyCounter = new AtomicInteger(0);
        private final CountDownLatch invalidationLatch;
        private final CountDownLatch newRequestCheckLatch = new CountDownLatch(1);
        private final CountDownLatch removeCompleteLatch = new CountDownLatch(1);

        public SynchronizedTestMap(Map<TopicPartition, Integer> m, final CountDownLatch invalidationLatch) {
            super(m);
            this.invalidationLatch = invalidationLatch;
        }

        @Override
        public boolean containsKey(Object key) {
            boolean result = super.containsKey(key);
            // waiting for twice as many checks, as we have two requests
            if (containsKeyCounter.incrementAndGet() == PARTITION * 2) {
                newRequestCheckLatch.countDown();
                try {
                    // letting the remove method proceed and actually remove the data
                    if (!removeCompleteLatch.await(LATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                        throw new RuntimeException("Timed out waiting for cache removal");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            return result;
        }

        @Override
        public Integer remove(Object key) {
            try {
                // letting the caller know that we've reached the invalidation step, and it's time to send the second request
                invalidationLatch.countDown();
                // waiting for the second request to reach containsKey
                if (!newRequestCheckLatch.await(LATCH_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                    throw new RuntimeException("Timed out waiting for second request");
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Integer result = super.remove(key);
            // once the value removed, we are letting the containsKey method proceed and return the value
            removeCompleteLatch.countDown();
            return result;
        }
    }

    private class TestHostResolver extends DefaultHostResolver {

        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            if (injectHostResolverError.get()) {
                throw new UnknownHostException();
            }
            return super.resolve(host);
        }
    }
}
