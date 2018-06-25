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
package org.apache.kafka.common;

import org.apache.kafka.common.config.ConfigException;

import java.util.Map;
import java.util.Set;

/**
 * Interface for reconfigurable classes that support dynamic configuration.
 */
public interface Reconfigurable extends Configurable {

    /**
     * Returns the names of configs that may be reconfigured.
     */
    Set<String> reconfigurableConfigs();

    /**
     * Validates the provided configuration. The provided map contains
     * all configs including any reconfigurable configs that may be different
     * from the initial configuration. Reconfiguration will be not performed
     * if this method throws any exception.
     * @throws ConfigException if the provided configs are not valid. The exception
     *         message from ConfigException will be returned to the client in
     *         the AlterConfigs response.
     */
    void validateReconfiguration(Map<String, ?> configs) throws ConfigException;

    /**
     * Reconfigures this instance with the given key-value pairs. The provided
     * map contains all configs including any reconfigurable configs that
     * may have changed since the object was initially configured using
     * {@link Configurable#configure(Map)}. This method will only be invoked if
     * the configs have passed validation using {@link #validateReconfiguration(Map)}.
     */
    void reconfigure(Map<String, ?> configs);

}
