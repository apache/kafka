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
package org.apache.kafka.connect.storage;

import org.apache.kafka.connect.runtime.WorkerConfigTransformer;

import java.util.Map;

/**
 * Wrapper class for a connector configuration that has been used to generate task configurations
 * Supports lazy {@link WorkerConfigTransformer#transform(Map) transformation}.
 */
public class AppliedConnectorConfig {

    private final Map<String, String> rawConfig;
    private Map<String, String> transformedConfig;

    /**
     * Create a new applied config that has not yet undergone
     * {@link WorkerConfigTransformer#transform(Map) transformation}.
     * @param rawConfig the non-transformed connector configuration; may be null
     */
    public AppliedConnectorConfig(Map<String, String> rawConfig) {
        this.rawConfig = rawConfig;
    }

    /**
     * If necessary, {@link WorkerConfigTransformer#transform(Map) transform} the raw
     * connector config, then return the result. Transformed configurations are cached and
     * returned in all subsequent calls.
     * <p>
     * This method is thread-safe: different threads may invoke it at any time and the same
     * transformed config should always be returned, with transformation still only ever
     * taking place once before its results are cached.
     * @param configTransformer the transformer to use, if no transformed connector
     *                          config has been cached yet; may be null
     * @return the possibly-cached, transformed, connector config; may be null
     */
    public synchronized Map<String, String> transformedConfig(WorkerConfigTransformer configTransformer) {
        if (transformedConfig != null || rawConfig == null)
            return transformedConfig;

        if (configTransformer != null) {
            transformedConfig = configTransformer.transform(rawConfig);
        } else {
            transformedConfig = rawConfig;
        }

        return transformedConfig;
    }

}
