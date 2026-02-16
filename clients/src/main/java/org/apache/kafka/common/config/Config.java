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
package org.apache.kafka.common.config;

import java.util.List;

/**
 * The result of a configuration validation, containing a list of {@link ConfigValue} instances.
 * <p>
 * This class is typically returned by {@link org.apache.kafka.connect.connector.Connector#validate(java.util.Map)}
 * after validating connector configuration properties against their {@link ConfigDef} definitions. Each
 * {@link ConfigValue} in the list represents the validated result of a single configuration key,
 * including its resolved value, recommended values, and any validation error messages.
 *
 * @see ConfigDef
 * @see ConfigValue
 */
public class Config {
    private final List<ConfigValue> configValues;

    /**
     * @param configValues the list of configuration values.
     */
    public Config(List<ConfigValue> configValues) {
        this.configValues = configValues;
    }

    /**
     * Return the list of configuration values. Each entry contains the validated result for a
     * configuration key, including any error messages produced during validation. Users can iterate
     * over this list to check for validation errors via {@link ConfigValue#errorMessages()}.
     */
    public List<ConfigValue> configValues() {
        return configValues;
    }

}
