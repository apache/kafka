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
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractConnectorClientConfigOverridePolicy implements ConnectorClientConfigOverridePolicy, Versioned {

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void close() {

    }

    @Override
    public final List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest) {
        Map<String, Object> inputConfig = connectorClientConfigRequest.clientProps();
        return inputConfig.entrySet().stream().map(this::configValue).collect(Collectors.toList());
    }

    protected ConfigValue configValue(Map.Entry<String, Object> configEntry) {
        ConfigValue configValue =
            new ConfigValue(configEntry.getKey(), configEntry.getValue(), new ArrayList<>(), new ArrayList<>());
        validate(configValue);
        return configValue;
    }

    protected void validate(ConfigValue configValue) {
        if (!isAllowed(configValue)) {
            configValue.addErrorMessage("The '" + policyName() + "' policy does not allow '" + configValue.name()
                                        + "' to be overridden in the connector configuration.");
        }
    }

    protected abstract String policyName();

    protected abstract boolean isAllowed(ConfigValue configValue);
}
