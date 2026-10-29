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
package org.apache.kafka.connect.connector.policy;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class AllowlistConnectorClientConfigOverridePolicyTest extends BaseConnectorClientConfigOverridePolicyTest {

    private static final List<String> ALL_CONFIGS = Stream.of(
                    ProducerConfig.configNames(),
                    ConsumerConfig.configNames(),
                    AdminClientConfig.configNames())
            .flatMap(Collection::stream)
            .toList();

    private AllowlistConnectorClientConfigOverridePolicy policy;

    @BeforeEach
    public void setUp() {
        policy = new AllowlistConnectorClientConfigOverridePolicy();
    }

    @Override
    protected ConnectorClientConfigOverridePolicy policyToTest() {
        return policy;
    }

    @Test
    public void testDenyAllByDefault() {
        for (String config : ALL_CONFIGS) {
            testInvalidOverride(Map.of(config, new Object()));
        }
    }

    @Test
    public void testAllowConfigs() {
        Set<String> allowedConfigs = Set.of(
                ProducerConfig.ACKS_CONFIG,
                ConsumerConfig.CLIENT_ID_CONFIG,
                AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG
        );
        policy.configure(Map.of(AllowlistConnectorClientConfigOverridePolicy.ALLOWLIST_CONFIG, String.join(",", allowedConfigs)));
        for (String config : ALL_CONFIGS) {
            if (!allowedConfigs.contains(config)) {
                testInvalidOverride(Map.of(config, new Object()));
            } else {
                testValidOverride(Map.of(config, new Object()));
            }
        }
    }
}
