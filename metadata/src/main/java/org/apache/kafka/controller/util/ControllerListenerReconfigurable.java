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

package org.apache.kafka.controller.util;

import java.util.Map;
import java.util.Set;

/**
 * Interface for controller components that support dynamic reconfiguration.
 * This is controller-specific and distinct from the broker's BrokerReconfigurable.
 */
public interface ControllerListenerReconfigurable {
    /**
     * Returns the names of configurations that may be reconfigured.
     */
    Set<String> reconfigurableConfigs();

    /**
     * Validates that the provided configuration can be applied.
     * Throws ConfigException if the new configuration is invalid.
     *
     * @param configs The new configuration to validate
     */
    void validateReconfiguration(Map<String, ?> configs);

    /**
     * Reconfigures this instance with the given key-value pairs.
     * This method is called after validation has passed.
     *
     * @param configs The new configuration to apply
     */
    void reconfigure(Map<String, ?> configs);
}
