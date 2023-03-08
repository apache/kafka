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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

public class WorkerConfigTest {

    private static final String CLUSTER_ID = "cluster-id";
    private MockedStatic<WorkerConfig> workerConfigMockedStatic;

    @Before
    public void setup() {
        workerConfigMockedStatic = mockStatic(WorkerConfig.class, new CallsRealMethods());
        workerConfigMockedStatic.when(() -> WorkerConfig.lookupKafkaClusterId(any(WorkerConfig.class))).thenReturn(CLUSTER_ID);
    }

    @After
    public void teardown() {
        workerConfigMockedStatic.close();
    }

    @Test
    public void testLookupKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient.Builder().
                brokers(cluster).build();
        assertEquals(MockAdminClient.DEFAULT_CLUSTER_ID, WorkerConfig.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupNullKafkaClusterId() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient.Builder().
                brokers(cluster).clusterId(null).build();
        assertNull(WorkerConfig.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testLookupKafkaClusterIdTimeout() {
        final Node broker1 = new Node(0, "dummyHost-1", 1234);
        final Node broker2 = new Node(1, "dummyHost-2", 1234);
        List<Node> cluster = Arrays.asList(broker1, broker2);
        MockAdminClient adminClient = new MockAdminClient.Builder().
                brokers(cluster).build();
        adminClient.timeoutNextRequest(1);

        assertThrows(ConnectException.class, () -> WorkerConfig.lookupKafkaClusterId(adminClient));
    }

    @Test
    public void testKafkaClusterId() {
        Map<String, String> props = baseProps();
        WorkerConfig config = new WorkerConfig(WorkerConfig.baseConfigDef(), props);
        assertEquals(CLUSTER_ID, config.kafkaClusterId());
        workerConfigMockedStatic.verify(() -> WorkerConfig.lookupKafkaClusterId(any(WorkerConfig.class)), times(1));

        // next calls hit the cache
        assertEquals(CLUSTER_ID, config.kafkaClusterId());
        workerConfigMockedStatic.verify(() -> WorkerConfig.lookupKafkaClusterId(any(WorkerConfig.class)), times(1));
    }

    private Map<String, String> baseProps() {
        Map<String, String> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonConverter");
        return props;
    }

}
