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
package org.apache.kafka.connect.util;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ConnectUtilsTest {

    @Test
    public void testLookupKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1);

        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupNullKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1, null);

        assertNull(ConnectUtils.lookupKafkaClusterId(adminClient));
    }

    @Test(expected = ConnectException.class)
    public void testLookupKafkaClusterIdTimeout() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient(cluster, broker1);
        adminClient.timeoutNextRequest(1);

        ConnectUtils.lookupKafkaClusterId(adminClient);
    }

    @Test
    public void removeNonAdminClientConfigurations() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "bootstrap1");
        configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "clientId");
        configs.put(AdminClientConfig.RETRIES_CONFIG, "1");
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "100");
        configs.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, MockMetricsReporter.class.getName());
        configs.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG + ".custom", "customValue");
        configs.put("some.other.property", "value");
        configs.put("other.property", "value2");
        Map<String, Object> filtered = ConnectUtils.retainConfigs(new HashMap<>(configs), AdminClientConfig::isKnownConfig);
        assertEquals(configs.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG),
                     filtered.get(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(configs.get(AdminClientConfig.CLIENT_ID_CONFIG),
                     filtered.get(AdminClientConfig.CLIENT_ID_CONFIG));
        assertEquals(configs.get(AdminClientConfig.RETRIES_CONFIG),
                     filtered.get(AdminClientConfig.RETRIES_CONFIG));
        assertEquals(configs.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG),
                     filtered.get(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG));
        assertEquals(configs.get(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG),
                     filtered.get(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG));
        assertEquals(configs.get(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG + ".custom"),
                     filtered.get(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG + ".custom"));
        assertFalse(filtered.containsKey("some.other.property"));
        assertFalse(filtered.containsKey("other.property"));
        assertEquals(configs.size() - 2, filtered.size());
        assertTrue(configs.keySet().containsAll(filtered.keySet()));
    }
}
