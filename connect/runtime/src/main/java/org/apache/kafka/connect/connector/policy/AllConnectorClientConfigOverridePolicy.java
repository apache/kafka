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

import java.util.Map;

/**
 * Allows all client configurations to be overridden via the connector configs by setting {@code connector.client.config.override.policy} to {@code All}
 */
public class AllConnectorClientConfigOverridePolicy extends AbstractConnectorClientConfigOverridePolicy {
    private static final Logger log = LoggerFactory.getLogger(AllConnectorClientConfigOverridePolicy.class);

    @Override
    protected String policyName() {
        return "All";
    }

    @Override
    protected boolean isAllowed(ConfigValue configValue) {
        return true;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        log.info("Setting up All Policy for ConnectorClientConfigOverride. This will allow all client configurations to be overridden");
    }
}
