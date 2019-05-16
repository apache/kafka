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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Disallow any client configuration to be overridden via the connector configs by setting {@code client.config.policy} to {@code None}.
 * This is the default behavior.
 */
public class NoneConnectorClientConfigOverridePolicy implements ConnectorClientConfigOverridePolicy {
    private static final Logger log = LoggerFactory.getLogger(NoneConnectorClientConfigOverridePolicy.class);

    @Override
    public List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest) {
        Map<String, Object> inputConfig = connectorClientConfigRequest.clientProps();
        return inputConfig.entrySet().stream().map(configEntry -> configValue(configEntry)).collect(Collectors.toList());
    }

    private static ConfigValue configValue(Map.Entry<String, Object> configEntry) {
        ConfigValue configValue =
            new ConfigValue(configEntry.getKey(), configEntry.getValue(), new ArrayList<Object>(), new ArrayList<String>());
        configValue.addErrorMessage("None policy doesn't allow any client overrides");
        return configValue;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Setting up None Policy for ConnectorClientConfigOverride. This will disallow any client configuration to be overridden");
    }
}
