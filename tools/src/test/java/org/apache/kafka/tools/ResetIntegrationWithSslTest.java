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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;

import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;

@ClusterTestDefaults(
    types = {Type.CO_KRAFT},
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "1"),
        @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "0"),
    }
)
public class ResetIntegrationWithSslTest extends AbstractResetIntegrationTest {

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL, 
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testResetWhenInternalTopicsAreSpecified(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runResetWhenInternalTopicsAreSpecified(cluster, admin, sslConfig, appId);
        }
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testReprocessingFromScratchAfterResetWithoutIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithoutIntermediateUserTopic(cluster, admin, sslConfig, appId);
        }
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testReprocessingFromScratchAfterResetWithIntermediateUserTopic(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, sslConfig, false, appId);
        }
    }

    @ClusterTest(
        brokerSecurityProtocol = SecurityProtocol.SSL,
        controllerSecurityProtocol = SecurityProtocol.SSL
    )
    public void testReprocessingFromScratchAfterResetWithIntermediateInternalTopic(ClusterInstance cluster) throws Exception {
        final Map<String, Object> sslConfig = cluster.setClientSslConfig(new HashMap<>());
        try (Admin admin = cluster.admin()) {
            final String appId = generateAppId();
            prepare(cluster, sslConfig, appId);
            runReprocessingFromScratchWithIntermediateUserTopic(cluster, admin, sslConfig, true, appId);
        }
    }
}
