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
 * Configuration data from a {@link ConfigProvider}.
 */
public class ConfigData {

    private final Map<String, String> data;
    private final Long ttl;

    /**
     * Creates a new ConfigData with the given data and TTL (in milliseconds).
     *
     * @param data a Map of key-value pairs
     * @param ttl the time-to-live of the data in milliseconds, or null if there is no TTL
     */
    public ConfigData(Map<String, String> data, Long ttl) {
        this.data = data;
        this.ttl = ttl;
    }

    /**
     * Creates a new ConfigData with the given data.
     *
     * @param data a Map of key-value pairs
     */
    public ConfigData(Map<String, String> data) {
        this(data, null);
    }

    /**
     * Returns the data.
     *
     * @return data a Map of key-value pairs
     */
    public Map<String, String> data() {
        return data;
    }

    /**
     * Returns the TTL (in milliseconds).
     *
     * @return ttl the time-to-live (in milliseconds) of the data, or null if there is no TTL
     */
    public Long ttl() {
        return ttl;
    }
}
