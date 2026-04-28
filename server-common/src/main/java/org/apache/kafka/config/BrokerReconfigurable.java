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
package org.apache.kafka.config;

import org.apache.kafka.common.config.AbstractConfig;

import java.util.Set;

/**
 * An interface for Kafka broker configs that support dynamic reconfiguration.
 * <p>
 * Components that implement this interface can have their configurations updated
 * at runtime without requiring a broker restart.
 * <p>
 * The reconfiguration process follows three steps:
 * <ol>
 *   <li>Determining which configurations can be dynamically updated via {@link #reconfigurableConfigs()}</li>
 *   <li>Validating the new configuration before applying it via {@link #validateReconfiguration(AbstractConfig)}</li>
 *   <li>Applying the new configuration via {@link #reconfigure(AbstractConfig, AbstractConfig)}</li>
 * </ol>
 */
public interface BrokerReconfigurable {
    /**
     * Returns the set of configuration keys that can be dynamically reconfigured.
     *
     * <p>
     * Only the configurations returned by this method will be considered for
     * dynamic updates by the broker.
     *
     * @return a set of configuration key names that can be dynamically updated
     */
    Set<String> reconfigurableConfigs();

    /**
     * Validates the new configuration before applying it.
     * <p>
     * This method should verify that the new configuration values are valid and
     * can be safely applied.
     *
     * @param newConfig the new configuration to validate
     */
    void validateReconfiguration(AbstractConfig newConfig);

    /**
     * Applies the new configuration.
     * <p>
     * This method is called after the new configuration has been validated.
     *
     * @param oldConfig the previous configuration
     * @param newConfig the new configuration to apply
     */
    void reconfigure(AbstractConfig oldConfig, AbstractConfig newConfig);
}
