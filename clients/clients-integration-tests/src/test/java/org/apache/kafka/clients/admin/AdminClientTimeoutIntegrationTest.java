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

import org.apache.kafka.clients.HostResolver;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.apache.kafka.test.TestUtils.assertFutureThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(types = { Type.KRAFT })
public class AdminClientTimeoutIntegrationTest {

    private static final int INCORRECT_BROKER_PORT = 225;

    private static final Logger log = LoggerFactory.getLogger(AdminClientTimeoutIntegrationTest.class);

    /**
     * Test injecting timeouts for calls that are in flight.
     */
    @ClusterTest
    public void testCallInFlightTimeouts(ClusterInstance clusterInstance) throws Exception {
        var config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
                AdminClientConfig.RETRIES_CONFIG, "0",
                // Set an extremely large overall API timeout to ensure the
                // FailureInjectingTimeoutProcessor triggers before the API-level timeout does.
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "100000000"
        );

        var factory = new FailureInjectingTimeoutProcessorFactory();
        try (var client = KafkaAdminClient.createInternal(new AdminClientConfig(config), factory)) {

            var future = client.createTopics(Stream.of("mytopic1", "mytopic2")
                    .map(t -> new NewTopic(t, 1, (short) 1)).toList(),
                    new CreateTopicsOptions().validateOnly(true)).all();

            var e = assertThrows(ExecutionException.class, future::get);
            assertInstanceOf(TimeoutException.class, e.getCause());

            var future2 = client.createTopics(Stream.of("mytopic3", "mytopic4")
                    .map(t -> new NewTopic(t, 1, (short) 1)).toList(),
                    new CreateTopicsOptions().validateOnly(true)).all();
            future2.get();
            assertEquals(1, factory.failuresInjected());
        }
    }

    @ClusterTest
    public void testAdminClientHandlingBadIPWithoutTimeout(ClusterInstance clusterInstance) throws Exception {
        Map<String, Object> config = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000",
            AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "40000",
            AdminClientConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, "1000");
        HostResolver returnBadAddressFirst = host -> new InetAddress[] {
            InetAddress.getByName("10.200.20.100"),
            InetAddress.getByName(host)
        };
        try (Admin admin = AdminClientTestUtils.create(config, returnBadAddressFirst)) {
            admin.describeCluster().nodes().get();
        }
    }

    @Test
    public void testDescribeConfigWithOptionTimeoutMs() {
        try (Admin admin = createInvalidAdminClient()) {
            List<ConfigResource> configResources = List.of(
                new ConfigResource(ConfigResource.Type.BROKER, "1"),
                new ConfigResource(ConfigResource.Type.BROKER, "2"));

            ExecutionException exception = assertTimeout(Duration.ofSeconds(30),
                () -> assertThrows(ExecutionException.class,
                    () -> admin.describeConfigs(configResources, new DescribeConfigsOptions().timeoutMs(0)).all().get()));
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    @Test
    public void testDescribeUserScramCredentialsTimeout() {
        try (Admin admin = createInvalidAdminClient()) {
            ExecutionException exception = assertTimeout(Duration.ofSeconds(10),
                () -> assertThrows(ExecutionException.class,
                    () -> admin.describeUserScramCredentials(List.of("tom4"),
                        new DescribeUserScramCredentialsOptions().timeoutMs(0)).all().get()));
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    @Test
    public void testDescribeTransactionsTimeout() {
        try (Admin admin = createInvalidAdminClient()) {
            String transactionId = "foo";
            ExecutionException exception = assertTimeout(Duration.ofSeconds(10),
                () -> assertThrows(ExecutionException.class,
                    () -> admin.describeTransactions(Set.of(transactionId),
                        new DescribeTransactionsOptions().timeoutMs(0)).description(transactionId).get()));
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    @Test
    public void testAbortTransactionTimeout() {
        try (Admin admin = createInvalidAdminClient()) {
            ExecutionException exception = assertTimeout(Duration.ofSeconds(10),
                () -> assertThrows(ExecutionException.class,
                    () -> admin.abortTransaction(
                        new AbortTransactionSpec(new TopicPartition("topic", 0), 1, (short) 1, 1),
                        new AbortTransactionOptions().timeoutMs(0)).all().get()));
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    @ClusterTest
    public void testClose(ClusterInstance clusterInstance) {
        Admin admin = clusterInstance.admin();
        admin.close();
        admin.close(); // double close has no effect
    }

    /**
     * Test closing the AdminClient with a generous timeout. Calls in progress should be completed,
     * since they can be done within the timeout. New calls should receive exceptions.
     */
    @ClusterTest
    public void testDelayedClose(ClusterInstance clusterInstance) throws Exception {
        List<NewTopic> newTopics = Stream.of("mytopic", "mytopic2")
            .map(topic -> new NewTopic(topic, 1, (short) 1)).toList();
        try (Admin admin = clusterInstance.admin()) {
            var future = admin.createTopics(newTopics, new CreateTopicsOptions().validateOnly(true)).all();
            admin.close(Duration.ofHours(2));
            var future2 = admin.createTopics(newTopics, new CreateTopicsOptions().validateOnly(true)).all();
            assertFutureThrows(IllegalStateException.class, future2);
            future.get();
            admin.close(Duration.ofMinutes(30)); // multiple close-with-timeout should have no effect
        }
    }

    /**
     * Test closing the AdminClient with a timeout of 0, when there are calls with extremely long
     * timeouts in progress. The calls should be aborted after the hard shutdown timeout elapses.
     */
    @Test
    public void testForceClose() {
        try (Admin admin = createInvalidAdminClient()) {
            var future = admin.createTopics(Stream.of("mytopic", "mytopic2")
                    .map(topic -> new NewTopic(topic, 1, (short) 1)).toList(),
                new CreateTopicsOptions().timeoutMs(900_000)).all();
            admin.close(Duration.ZERO);
            assertFutureThrows(TimeoutException.class, future);
        }
    }

    /**
     * Check that a call with a timeout does not complete before the minimum timeout has elapsed,
     * even when the default request timeout is shorter.
     */
    @Test
    public void testMinimumRequestTimeouts() {
        Map<String, Object> config = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + INCORRECT_BROKER_PORT,
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "0");
        try (Admin admin = Admin.create(config)) {
            long startTimeMs = Time.SYSTEM.milliseconds();
            var future = admin.createTopics(Stream.of("mytopic", "mytopic2")
                    .map(topic -> new NewTopic(topic, 1, (short) 1)).toList(),
                new CreateTopicsOptions().timeoutMs(2)).all();
            assertFutureThrows(TimeoutException.class, future);
            long endTimeMs = Time.SYSTEM.milliseconds();
            assertTrue(endTimeMs > startTimeMs,
                "Expected the timeout to take at least one millisecond.");
        }
    }

    @Test
    public void testListClientMetricsResourcesTimeoutMs() {
        try (Admin admin = createInvalidAdminClient()) {
            ListClientMetricsResourcesOptions timeoutOption = new ListClientMetricsResourcesOptions().timeoutMs(0);
            ExecutionException exception = assertTimeout(Duration.ofSeconds(30),
                () -> assertThrows(ExecutionException.class,
                    () -> admin.listClientMetricsResources(timeoutOption).all().get()));
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    @Test
    public void testListConfigResourcesTimeoutMs() {
        try (Admin admin = createInvalidAdminClient()) {
            ListConfigResourcesOptions timeoutOption = new ListConfigResourcesOptions().timeoutMs(0);
            ExecutionException exception = assertTimeout(Duration.ofSeconds(30),
                () -> assertThrows(ExecutionException.class,
                    () -> admin.listConfigResources(Set.of(), timeoutOption).all().get()));
            assertInstanceOf(TimeoutException.class, exception.getCause());
        }
    }

    private Admin createInvalidAdminClient() {
        return Admin.create(Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + INCORRECT_BROKER_PORT));
    }


    static class FailureInjectingTimeoutProcessorFactory extends KafkaAdminClient.TimeoutProcessorFactory {

        private int numTries = 0;

        private int failuresInjected = 0;

        @Override
        KafkaAdminClient.TimeoutProcessor create(long now) {
            return new FailureInjectingTimeoutProcessor(now);
        }

        synchronized boolean shouldInjectFailure() {
            numTries++;
            if (numTries == 1) {
                failuresInjected++;
                return true;
            }
            return false;
        }

        synchronized int failuresInjected() {
            return failuresInjected;
        }

        final class FailureInjectingTimeoutProcessor extends KafkaAdminClient.TimeoutProcessor {
            FailureInjectingTimeoutProcessor(long now) {
                super(now);
            }

            @Override
            boolean callHasExpired(KafkaAdminClient.Call call) {
                if ((!call.isInternal()) && shouldInjectFailure()) {
                    log.debug("Injecting timeout for {}.", call);
                    return true;
                } else {
                    boolean ret = super.callHasExpired(call);
                    log.debug("callHasExpired({}) = {}", call, ret);
                    return ret;
                }
            }
        }
    }
}
