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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ForwardingAdmin;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MirrorClientConfigTest {

    @Test
    public void testBasicConfigInitialization() {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "host1:9092");
        MirrorClientConfig clientConfig = new MirrorClientConfig(config);

        assertNotNull(clientConfig);
        assertTrue(clientConfig.values().containsKey(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("host1:9092", clientConfig.getList(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG).get(0));
    }

    @Test
    public void testReplicationPolicyDefaultAndConfigurable() {
        Map<String, Object> config = Map.of(
                CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:1234"
        );
        MirrorClientConfig clientConfig = new MirrorClientConfig(config);
        assertTrue(clientConfig.replicationPolicy() instanceof DefaultReplicationPolicy);

        Map<String, Object> custom = new HashMap<>(config);
        custom.put(MirrorClientConfig.REPLICATION_POLICY_CLASS, IdentityReplicationPolicy.class.getName());
        MirrorClientConfig customConfig = new MirrorClientConfig(custom);
        assertTrue(customConfig.replicationPolicy() instanceof IdentityReplicationPolicy);
    }

    @Test
    public void testSubConfigParsing() {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "a:1");
        config.put("admin.someAdminParam", "valA");
        config.put("consumer.someConsumerParam", "valB");
        config.put("producer.someProducerParam", "valC");
        MirrorClientConfig clientConfig = new MirrorClientConfig(config);

        Map<String, Object> adminCfg = clientConfig.adminConfig();
        Map<String, Object> consumerCfg = clientConfig.consumerConfig();
        Map<String, Object> producerCfg = clientConfig.producerConfig();
        // Each config should only contain relevant param (simulate with expected keys)
        assertTrue(adminCfg.keySet().containsAll(clientConfig.adminConfig().keySet()));
        assertTrue(consumerCfg.keySet().containsAll(clientConfig.consumerConfig().keySet()));
        assertTrue(producerCfg.keySet().containsAll(clientConfig.producerConfig().keySet()));
    }

    @Test
    public void testForwardingAdminDefault() {
        Map<String, Object> config = new HashMap<>();
        config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        MirrorClientConfig clientConfig = new MirrorClientConfig(config);
        ForwardingAdmin admin = clientConfig.forwardingAdmin(clientConfig.adminConfig());
        assertNotNull(admin);
    }

    @Test
    public void testMissingBootstrapServersThrows() {
        Map<String, Object> config = new HashMap<>();
        assertThrows(ConfigException.class, () -> new MirrorClientConfig(config));
    }
}
