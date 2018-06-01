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

import java.util.Map;

/**
 * The result of a transformation from {@link ConfigTransformer}.
 */
public class ConfigTransformerResult {

    private Map<String, Long> ttls;
    private Map<String, String> data;

    /**
     * Creates a new ConfigTransformerResult with the given data and TTL values for a set of paths.
     *
     * @param data a Map of key-value pairs
     * @param ttls a Map of path and TTL values (in milliseconds)
     */
    public ConfigTransformerResult(Map<String, String> data, Map<String, Long> ttls) {
        this.data = data;
        this.ttls = ttls;
    }

    /**
     * Returns the transformed data, with variables replaced with corresponding values from the
     * ConfigProvider instances if found.
     *
     * <p>Modifying the transformed data that is returned does not affect the {@link ConfigProvider} nor the
     * original data that was used as the source of the transformation.
     *
     * @return data a Map of key-value pairs
     */
    public Map<String, String> data() {
        return data;
    }

    /**
     * Returns the TTL values (in milliseconds) returned from the ConfigProvider instances for a given set of paths.
     *
     * @return data a Map of path and TTL values
     */
    public Map<String, Long> ttls() {
        return ttls;
    }
}
