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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class NoneConnectorClientConfigOverridePolicyTest extends BaseConnectorClientConfigOverridePolicyTest {

    ConnectorClientConfigOverridePolicy noneConnectorClientConfigOverridePolicy = new NoneConnectorClientConfigOverridePolicy();

    @Test
    public void testNoOverrides() {
        testValidOverride(Collections.emptyMap());
    }

    @Test
    public void testWithOverrides() {
        Map<String, Object> clientConfig = new HashMap<>();
        clientConfig.put(SaslConfigs.SASL_JAAS_CONFIG, "test");
        clientConfig.put(ProducerConfig.ACKS_CONFIG, "none");
        testInvalidOverride(clientConfig);
    }

    @Override
    protected ConnectorClientConfigOverridePolicy policyToTest() {
        return noneConnectorClientConfigOverridePolicy;
    }
}
