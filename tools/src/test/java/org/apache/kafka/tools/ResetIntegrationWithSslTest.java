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

import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.test.api.ClusterConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.common.test.api.Type.KRAFT;

/**
 * Tests command line SSL setup for reset tool.
 */
public class ResetIntegrationWithSslTest extends AbstractResetIntegrationTest {
    public static List<ClusterConfig> clusterConfigs() {
        return List.of(ClusterConfig.defaultBuilder()
                .setTypes(Set.of(KRAFT))
                .setBrokers(1)
                .setBrokerSecurityProtocol(SecurityProtocol.SASL_SSL)
                .setControllerSecurityProtocol(SecurityProtocol.SASL_SSL)
                .setServerProperties(defaultBrokerProps())
                .build());
    }

    @Override
    Map<String, Object> getClientSecurityConfig() {
        return cluster.setClientSslConfig(Map.of());
    }
}
