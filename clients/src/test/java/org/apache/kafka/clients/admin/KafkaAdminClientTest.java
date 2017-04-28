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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.CreateTopicsResponse.Error;
import org.apache.kafka.common.requests.CreateTopicsResponse;
import org.apache.kafka.common.utils.Time;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * A unit test for KafkaAdminClient.
 *
 * See for an integration test of the KafkaAdminClient.
 * Also see KafkaAdminClientIntegrationTest for a unit test of the admin client.
 */
public class KafkaAdminClientTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testGetOrCreateListValue() {
        Map<String, List<String>> map = new HashMap<>();
        List<String> fooList = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertNotNull(fooList);
        fooList.add("a");
        fooList.add("b");
        List<String> fooList2 = KafkaAdminClient.getOrCreateListValue(map, "foo");
        assertEquals(fooList, fooList2);
        assertTrue(fooList2.contains("a"));
        assertTrue(fooList2.contains("b"));
        List<String> barList = KafkaAdminClient.getOrCreateListValue(map, "bar");
        assertNotNull(barList);
        assertTrue(barList.isEmpty());
    }

    @Test
    public void testCalcTimeoutMsRemainingAsInt() {
        assertEquals(0, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1000));
        assertEquals(100, KafkaAdminClient.calcTimeoutMsRemainingAsInt(1000, 1100));
        assertEquals(Integer.MAX_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(0, Long.MAX_VALUE));
        assertEquals(Integer.MIN_VALUE, KafkaAdminClient.calcTimeoutMsRemainingAsInt(Long.MAX_VALUE, 0));
    }

    @Test
    public void testPrettyPrintException() {
        assertEquals("Null exception.", KafkaAdminClient.prettyPrintException(null));
        assertEquals("TimeoutException", KafkaAdminClient.prettyPrintException(new TimeoutException()));
        assertEquals("TimeoutException: The foobar timed out.",
            KafkaAdminClient.prettyPrintException(new TimeoutException("The foobar timed out.")));
    }

    private static Map<String, Object> newStrMap(String... vals) {
        Map<String, Object> map = new HashMap<>();
        map.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:8121");
        map.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000");
        if (vals.length % 2 != 0) {
            throw new IllegalStateException();
        }
        for (int i = 0; i < vals.length; i += 2) {
            map.put(vals[i], vals[i + 1]);
        }
        return map;
    }

    private static AdminClientConfig newConfMap(String... vals) {
        return new AdminClientConfig(newStrMap(vals));
    }

    @Test
    public void testGenerateClientId() {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            String id = KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, ""));
            assertTrue("Got duplicate id " + id, !ids.contains(id));
            ids.add(id);
        }
        assertEquals("myCustomId",
            KafkaAdminClient.generateClientId(newConfMap(AdminClientConfig.CLIENT_ID_CONFIG, "myCustomId")));
    }

    private static class MockKafkaAdminClientContext implements AutoCloseable {
        final static String CLUSTER_ID = "mockClusterId";
        final AdminClientConfig adminClientConfig;
        final Metadata metadata;
        final HashMap<Integer, Node> nodes;
        final MockClient mockClient;
        final AdminClient client;
        Cluster cluster;

        MockKafkaAdminClientContext(Map<String, Object> config) {
            this.adminClientConfig = new AdminClientConfig(config);
            this.metadata = new Metadata(adminClientConfig.getLong(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG),
                adminClientConfig.getLong(AdminClientConfig.METADATA_MAX_AGE_CONFIG));
            this.nodes = new HashMap<Integer, Node>();
            this.nodes.put(0, new Node(0, "localhost", 8121));
            this.nodes.put(1, new Node(1, "localhost", 8122));
            this.nodes.put(2, new Node(2, "localhost", 8123));
            this.mockClient = new MockClient(Time.SYSTEM, this.metadata);
            this.client = KafkaAdminClient.create(adminClientConfig, mockClient, metadata);
            this.cluster = new Cluster(CLUSTER_ID,  nodes.values(),
                Collections.<PartitionInfo>emptySet(), Collections.<String>emptySet(),
                Collections.<String>emptySet(), nodes.get(0));
        }

        @Override
        public void close() {
            this.client.close();
        }
    }

    @Test
    public void testCloseAdminClient() throws Exception {
        try (MockKafkaAdminClientContext ctx = new MockKafkaAdminClientContext(newStrMap())) {
        }
    }

    private static void assertFutureError(Future<?> future, Class<? extends Throwable> exceptionClass)
        throws InterruptedException {
        try {
            future.get();
            fail("Expected a " + exceptionClass.getSimpleName() + " exception, but got success.");
        } catch (ExecutionException ee) {
            Throwable cause = ee.getCause();
            assertEquals("Expected a " + exceptionClass.getSimpleName() + " exception, but got " +
                cause.getClass().getSimpleName(),
                exceptionClass, cause.getClass());
        }
    }

    /**
     * Test that the client properly times out when we don't receive any metadata.
     */
    @Test
    public void testTimeoutWithoutMetadata() throws Exception {
        try (MockKafkaAdminClientContext ctx = new MockKafkaAdminClientContext(newStrMap(
            AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10"))) {
            ctx.mockClient.setNodeApiVersions(NodeApiVersions.create());
            ctx.mockClient.setNode(new Node(0, "localhost", 8121));
            ctx.mockClient.prepareResponse(new CreateTopicsResponse(new HashMap<String, Error>() {{
                    put("myTopic", new Error(Errors.NONE, ""));
                }}));
            KafkaFuture<Void> future = ctx.client.
                createTopics(Collections.singleton(new NewTopic("myTopic", new HashMap<Integer, List<Integer>>() {{
                        put(Integer.valueOf(0), Arrays.asList(new Integer[]{0, 1, 2}));
                    }})), new CreateTopicsOptions().timeoutMs(1000)).all();
            assertFutureError(future, TimeoutException.class);
        }
    }

    @Test
    public void testCreateTopics() throws Exception {
        try (MockKafkaAdminClientContext ctx = new MockKafkaAdminClientContext(newStrMap())) {
            ctx.mockClient.setNodeApiVersions(NodeApiVersions.create());
            ctx.mockClient.prepareMetadataUpdate(ctx.cluster, Collections.<String>emptySet());
            ctx.mockClient.setNode(ctx.nodes.get(0));
            ctx.mockClient.prepareResponse(new CreateTopicsResponse(new HashMap<String, Error>() {{
                    put("myTopic", new Error(Errors.NONE, ""));
                }}));
            KafkaFuture<Void> future = ctx.client.
                createTopics(Collections.singleton(new NewTopic("myTopic", new HashMap<Integer, List<Integer>>() {{
                        put(Integer.valueOf(0), Arrays.asList(new Integer[]{0, 1, 2}));
                    }})), new CreateTopicsOptions().timeoutMs(10000)).all();
            future.get();
        }
    }
}
