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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.MockConnectMetrics.MockMetricsReporter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.storage.StringConverter;
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
    public void shouldIncludeKnownWorkerConfigs() {
        for (String configName : DistributedConfig.configNames()) {
            assertTrue("Should allow " + configName, ConnectUtils.isKnownWorkerConfig(configName));
        }
        for (String configName : StandaloneConfig.configNames()) {
            assertTrue("Should allow " + configName, ConnectUtils.isKnownWorkerConfig(configName));
        }
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG + ".something"));
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.CONFIG_PROVIDERS_CONFIG + ".something"));
        assertTrue(ConnectUtils.isKnownWorkerConfig(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG + ".something"));

        for (String configName : ProducerConfig.configNames()) {
            assertTrue("Should allow " + configName, ConnectUtils.isKnownWorkerConfig("producer." + configName));
        }
        for (String configName : ConsumerConfig.configNames()) {
            assertTrue("Should allow " + configName, ConnectUtils.isKnownWorkerConfig("consumer." + configName));
        }

        // We don't consider extra configs to be known, even though the REST extensions use them
        assertFalse(ConnectUtils.isKnownWorkerConfig("some.arbitrary.config"));
    }

    @Test
    public void shouldIncludeProducerConfigsAndUnknownConfigs() {
        for (String configName : ProducerConfig.configNames()) {
            assertTrue("Should allow " + configName, ConnectUtils.isProducerConfig(configName));
        }
        assertTrue(ConnectUtils.isProducerConfig("custom.config"));

        // Exclude Connect worker configs
        for (String configName : DistributedConfig.configNames()) {
            if (ProducerConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isProducerConfig(configName));
        }
        for (String configName : StandaloneConfig.configNames()) {
            if (ProducerConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isProducerConfig(configName));
        }
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.CONFIG_PROVIDERS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isProducerConfig(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG + ".something"));

        // Exclude Consumer-specific configs
        assertFalse(ConnectUtils.isProducerConfig(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    }

    @Test
    public void shouldIncludeConsumerConfigsAndUnknownConfigs() {
        for (String configName : ConsumerConfig.configNames()) {
            assertTrue("Should allow " + configName, ConnectUtils.isConsumerConfig(configName));
        }
        assertTrue(ConnectUtils.isConsumerConfig("custom.config"));

        // Exclude Connect worker configs
        for (String configName : DistributedConfig.configNames()) {
            if (ConsumerConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isConsumerConfig(configName));
        }
        for (String configName : StandaloneConfig.configNames()) {
            if (ConsumerConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isConsumerConfig(configName));
        }
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.CONFIG_PROVIDERS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isConsumerConfig(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG + ".something"));

        // Exclude Producer-specific configs
        assertFalse(ConnectUtils.isConsumerConfig(ProducerConfig.ACKS_CONFIG));
    }

    @Test
    public void shouldIncludeAdminClientConfigs() {
        for (String configName : AdminClientConfig.configNames()) {
            assertTrue(ConnectUtils.isAdminClientConfig(configName));
        }

        // Exclude Connect worker configs
        for (String configName : DistributedConfig.configNames()) {
            if (AdminClientConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isAdminClientConfig(configName));
        }
        for (String configName : StandaloneConfig.configNames()) {
            if (AdminClientConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isAdminClientConfig(configName));
        }
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.INTERNAL_KEY_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.INTERNAL_VALUE_CONVERTER_CLASS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.CONFIG_PROVIDERS_CONFIG + ".something"));
        assertFalse(ConnectUtils.isAdminClientConfig(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG + ".something"));

        // Exclude Producer-specific configs
        for (String configName : ProducerConfig.configNames()) {
            if (AdminClientConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isAdminClientConfig(configName));
        }

        // Exclude Consumer-specific configs
        for (String configName : ConsumerConfig.configNames()) {
            if (AdminClientConfig.configNames().contains(configName)) {
                continue;
            }
            assertFalse("Should not allow " + configName, ConnectUtils.isAdminClientConfig(configName));
        }
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
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(DistributedConfig.CONFIG_TOPIC_CONFIG, "my-config-topic");
        configs.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
        configs.put(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG + ".custom", "foo");
        configs.put("producer." + ProducerConfig.ACKS_CONFIG, "all");
        configs.put("consumer." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configs.put("some.other.property", "value");
        configs.put("other.property", "value2");
        assertEquals(15, configs.size());
        Map<String, Object> filtered = ConnectUtils.retainAdminClientConfigs(new HashMap<>(configs));
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
        assertEquals(configs.get("some.other.property"),
                     filtered.get("some.other.property"));
        assertEquals(configs.get("other.property"),
                     filtered.get("other.property"));
        assertFalse(filtered.containsKey(ProducerConfig.ACKS_CONFIG));
        assertFalse(filtered.containsKey(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertFalse(filtered.containsKey(DistributedConfig.CONFIG_TOPIC_CONFIG));
        assertFalse(filtered.containsKey(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG));
        assertFalse(filtered.containsKey(DistributedConfig.KEY_CONVERTER_CLASS_CONFIG + ".custom"));
        assertEquals(configs.size() - 7, filtered.size());
        assertTrue(configs.keySet().containsAll(filtered.keySet()));
    }

}
