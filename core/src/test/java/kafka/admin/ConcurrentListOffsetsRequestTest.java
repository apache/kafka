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
package kafka.admin;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.DefaultHostResolver;
import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.internals.PartitionLeaderCache;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
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

@ClusterTestDefaults(
        types = {Type.KRAFT},
        brokers = 3
)
public class ConcurrentListOffsetsRequestTest {
    private static final String TOPIC = "topic";
    private static final short REPLICAS = 1;
    private static final int PARTITION = 2;
    private static final int TIMEOUT = 1000;
    private Admin adminClient;
    private NetworkClient networkClient;
    private final AtomicBoolean injectHostResolverError = new AtomicBoolean(false);

    private void setup(ClusterInstance clusterInstance) throws Exception {
        clusterInstance.waitForReadyBrokers();
        clusterInstance.createTopic(TOPIC, PARTITION, REPLICAS);
        Map<String, Object> props = Map.of(
                "default.api.timeout.ms", TIMEOUT,
                "request.timeout.ms", TIMEOUT,
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());

        var createInternalMethod = KafkaAdminClient.class.getDeclaredMethod(
                "createInternal",
                AdminClientConfig.class,
                Class.forName("org.apache.kafka.clients.admin.KafkaAdminClient$TimeoutProcessorFactory"),
                HostResolver.class
        );

        createInternalMethod.setAccessible(true);

        adminClient = (Admin) createInternalMethod.invoke(null, new AdminClientConfig(clusterInstance.setClientSaslConfig(props)), null, new TestHostResolver());

        networkClient = TestUtils.fieldValue(adminClient, KafkaAdminClient.class, "client");
    }

    @AfterEach
    public void teardown() {
        Utils.closeQuietly(adminClient, "ListOffsetsAdminClient");
    }

    @ClusterTest
    public void correctlyHandleConcurrentModificationOfPartitionLeaderCache(ClusterInstance clusterInstance) throws Exception {
        setup(clusterInstance);
        // making one request to prepopulate the partition leader cache so we have something to delete later
        listAllOffsets().all().get(TIMEOUT * 2, TimeUnit.SECONDS);

        final CountDownLatch invalidationLatch = new CountDownLatch(1);
        // Replacing the partition leader cache in order to be able to synchronize the calls so that they happen in the right order to reproduce the issue
        TestPartitionLeaderCache testPartitionLeaderCache = replacePartitionLeaderCache(invalidationLatch);

        // closing the connection to the first node. not using clusterInstance.shutdownBroker to reduce flakiness
        networkClient.close(testPartitionLeaderCache.get(getTopicPartitions()).values().iterator().next().toString());
        // as next call with try to resolve the host for the closed node, it's time to let it fail, which will lead to cache invalidation
        injectHostResolverError.set(true);

        // making another request(this request will face the host resolver error and remove the node from the cache)
        ListOffsetsResult failInducingResult = listAllOffsets();
        // waiting until we get to the invalidation
        invalidationLatch.await();
        // making another request. at this point the fail inducing request is waiting for this one before it deletes the keys associated with the node
        // the TestPartitionLeaderCache class synchronizes the calls to mimic the race condition
        ListOffsetsResult failingResult = listAllOffsets();

        // verifying that we correctly declined the call
        ExecutionException executionException = assertThrows(ExecutionException.class, () -> failInducingResult.all().get(TIMEOUT * 2, TimeUnit.MILLISECONDS));
        assertInstanceOf(TimeoutException.class, executionException.getCause());

        // verifying that we correctly declined the call
        executionException = assertThrows(ExecutionException.class, () -> failingResult.all().get(TIMEOUT * 2, TimeUnit.MILLISECONDS));
        assertInstanceOf(TimeoutException.class, executionException.getCause());
    }

    private TestPartitionLeaderCache replacePartitionLeaderCache(CountDownLatch invalidationLatch) throws Exception {
        PartitionLeaderCache oldPartitionLeaderCache = TestUtils.fieldValue(adminClient, KafkaAdminClient.class, "partitionLeaderCache");

        TestPartitionLeaderCache partitionLeaderCache = new TestPartitionLeaderCache(oldPartitionLeaderCache.get(getTopicPartitions()), invalidationLatch);
        TestUtils.setFieldValue(adminClient, "partitionLeaderCache", partitionLeaderCache);
        return partitionLeaderCache;
    }

    private ListOffsetsResult listAllOffsets() {
        List<TopicPartition> partitions = getTopicPartitions();

        Map<TopicPartition, OffsetSpec> offsetSpecMap = partitions.stream().collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
        return adminClient.listOffsets(offsetSpecMap, new ListOffsetsOptions(IsolationLevel.READ_UNCOMMITTED));
    }

    private List<TopicPartition> getTopicPartitions() {
        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < PARTITION; i++) {
            partitions.add(new TopicPartition(TOPIC, i));
        }
        return partitions;
    }

    private static class TestPartitionLeaderCache extends PartitionLeaderCache {

        private final AtomicInteger getCounter = new AtomicInteger(0);
        private final CountDownLatch invalidationLatch;
        private final CountDownLatch newRequestCheckLatch = new CountDownLatch(1);
        private final CountDownLatch removeCompleteLatch = new CountDownLatch(1);

        public TestPartitionLeaderCache(Map<TopicPartition, Integer> oldPartitionLeaderCache, final CountDownLatch invalidationLatch) {
            put(oldPartitionLeaderCache);
            this.invalidationLatch = invalidationLatch;
        }

        @Override
        public Map<TopicPartition, Integer> get(Collection<TopicPartition> keys) {
            Map<TopicPartition, Integer> result = super.get(keys);
            // waiting for the third call: first one was to close the network connection, second one was from the request that invalidates the cache
            if (getCounter.incrementAndGet() == 3) {
                newRequestCheckLatch.countDown();
                try {
                    // letting the remove method proceed and actually remove the data
                    removeCompleteLatch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            return result;
        }

        @Override
        public void remove(Collection<TopicPartition> keys) {
            try {
                // letting the caller know that we've reached the invalidation step, and it's time to send the second request
                invalidationLatch.countDown();
                // waiting for the second request to reach get
                newRequestCheckLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            super.remove(keys);
            // once the value removed, we are letting the get method proceed and return the value
            removeCompleteLatch.countDown();
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
