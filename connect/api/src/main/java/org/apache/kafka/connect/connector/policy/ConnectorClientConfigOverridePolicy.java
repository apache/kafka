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

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigValue;

import java.util.List;

/**
 * <p>An interface for enforcing a policy on overriding of client configs via the connector configs.
 *
 * <p>Common use cases are ability to provide principal per connector, <code>sasl.jaas.config</code>
 * and/or enforcing that the producer/consumer configurations for optimizations are within acceptable ranges.
 */
public interface ConnectorClientConfigOverridePolicy extends Configurable, AutoCloseable {


    /**
     * Worker will invoke this while constructing the producer for the SourceConnectors,  DLQ for SinkConnectors and the consumer for the
     * SinkConnectors to validate if all of the overridden client configurations are allowed per the
     * policy implementation. This would also be invoked during the validate of connector configs via the Rest API.
     *
     * If there are any policy violations, the connector will not be started.
     *
     * @param connectorClientConfigRequest an instance of {@code ConnectorClientConfigRequest} that provides the configs to overridden and
     *                                     its context; never {@code null}
     * @return list of {@link ConfigValue} instances that describe each client configuration in the request and includes an 
               {@link ConfigValue#errorMessages error} if the configuration is not allowed by the policy; never null
     */
    List<ConfigValue> validate(ConnectorClientConfigRequest connectorClientConfigRequest);
}
