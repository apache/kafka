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

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.health.ConnectorType;
import org.apache.kafka.connect.runtime.WorkerTest;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public abstract class BaseConnectorClientConfigOverridePolicyTest {

    protected abstract ConnectorClientConfigOverridePolicy  policyToTest();

    protected void testValidOverride(Map<String, Object> clientConfig) {
        List<ConfigValue> configValues = configValues(clientConfig);
        assertNoError(configValues);
    }

    protected void testInvalidOverride(Map<String, Object> clientConfig) {
        List<ConfigValue> configValues = configValues(clientConfig);
        assertError(configValues);
    }

    private List<ConfigValue> configValues(Map<String, Object> clientConfig) {
        ConnectorClientConfigRequest connectorClientConfigRequest = new ConnectorClientConfigRequest(
            "test",
            ConnectorType.SOURCE,
            WorkerTest.WorkerTestConnector.class,
            clientConfig,
            ConnectorClientConfigRequest.ClientType.PRODUCER);
        return policyToTest().validate(connectorClientConfigRequest);
    }

    protected void assertNoError(List<ConfigValue> configValues) {
        Assert.assertTrue(configValues.stream().allMatch(configValue -> configValue.errorMessages().size() == 0));
    }

    protected void assertError(List<ConfigValue> configValues) {
        Assert.assertTrue(configValues.stream().anyMatch(configValue -> configValue.errorMessages().size() > 0));
    }
}
