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

import org.apache.kafka.clients.MockClient;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple utility for setting up a mock {@link KafkaAdminClient} that uses a {@link MockClient} for a supplied
 * {@link Cluster}. Create a {@link Cluster} manually or use {@link org.apache.kafka.test.TestUtils} methods to
 * easily create a simple cluster.
 * <p>
 * To use in a test, create an instance and prepare its {@link #kafkaClient() MockClient} with the expected responses
 * for the {@link AdminClient}. Then, use the {@link #adminClient() AdminClient} in the test, which will then use the MockClient
 * and receive the responses you provided.
 * <p>
 * When finished, be sure to {@link #close() close} the environment object.
 */
public class AdminClientUnitTestEnv implements AutoCloseable {
    private final Time time;
    private final Cluster cluster;
    private final MockClient mockClient;
    private final KafkaAdminClient adminClient;

    public AdminClientUnitTestEnv(Cluster cluster, String...vals) {
        this(Time.SYSTEM, cluster, vals);
    }

    public AdminClientUnitTestEnv(Time time, Cluster cluster, String...vals) {
        this(time, cluster, newStrMap(vals));
    }

    public AdminClientUnitTestEnv(Time time, Cluster cluster, Map<String, Object> config) {
        this(newMockClient(time, cluster), time, cluster, config);
    }

    private static MockClient newMockClient(Time time, Cluster cluster) {
        MockClient mockClient = new MockClient(time);
        mockClient.prepareResponse(new MetadataResponse(cluster.nodes(),
            cluster.clusterResource().clusterId(),
            cluster.controller().id(),
            Collections.<MetadataResponse.TopicMetadata>emptyList()));
        return mockClient;
    }

    public AdminClientUnitTestEnv(MockClient mockClient, Time time, Cluster cluster,
                                  Map<String, Object> config) {
        this.time = time;
        this.cluster = cluster;
        AdminClientConfig adminClientConfig = new AdminClientConfig(config);
        this.mockClient = mockClient;
        this.adminClient = KafkaAdminClient.createInternal(adminClientConfig, mockClient, time);
    }

    public Time time() {
        return time;
    }

    public Cluster cluster() {
        return cluster;
    }

    public AdminClient adminClient() {
        return adminClient;
    }

    public MockClient kafkaClient() {
        return mockClient;
    }

    @Override
    public void close() {
        this.adminClient.close();
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
}
