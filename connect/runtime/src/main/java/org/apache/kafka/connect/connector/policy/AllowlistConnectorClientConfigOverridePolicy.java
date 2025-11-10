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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Allows only client configurations specified via <code>connector.client.config.override.allowlist</code> to be
 * overridden by connectors. By default, <code>connector.client.config.override.allowlist</code> is empty so connectors
 * can't override any client configurations.
 */
public class AllowlistConnectorClientConfigOverridePolicy extends AbstractConnectorClientConfigOverridePolicy {

    public static final String ALLOWLIST_CONFIG = "connector.client.config.override.allowlist";

    private static final Logger LOGGER = LoggerFactory.getLogger(AllowlistConnectorClientConfigOverridePolicy.class);
    private static final List<String> ALLOWLIST_CONFIG_DEFAULT = List.of();
    private static final String ALLOWLIST_CONFIG_DOC = "List of client configurations that can be overridden by " +
            "connectors. If empty, connectors can't override any client configurations.";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ALLOWLIST_CONFIG, ConfigDef.Type.LIST, ALLOWLIST_CONFIG_DEFAULT, ConfigDef.Importance.MEDIUM, ALLOWLIST_CONFIG_DOC);

    private List<String> allowlist = ALLOWLIST_CONFIG_DEFAULT;

    @Override
    protected String policyName() {
        return "Allowlist";
    }

    @Override
    protected boolean isAllowed(ConfigValue configValue) {
        return allowlist.contains(configValue.name());
    }

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        allowlist = config.getList(ALLOWLIST_CONFIG);
        LOGGER.info("Setting up Allowlist policy for ConnectorClientConfigOverride. This will allow the following client configurations"
                + " to be overridden. {}", allowlist);
    }
}
