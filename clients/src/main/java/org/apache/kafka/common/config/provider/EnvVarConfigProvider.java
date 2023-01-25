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
package org.apache.kafka.common.config.provider;

import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class EnvVarConfigProvider implements ConfigProvider {
    private final Map<String, String> envVarMap;

    public EnvVarConfigProvider() {
        envVarMap = getEnvVars();
    }

    public EnvVarConfigProvider(Map<String, String> envVarsAsArgument) {
        envVarMap = envVarsAsArgument;
    }

    private static final Logger log = LoggerFactory.getLogger(EnvVarConfigProvider.class);

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * @param s unused
     * @return returns environment variables as configuration
     */
    @Override
    public ConfigData get(String s) {
        return get(s, null);
    }

    /**
     * @param path    path, not used for environment variables
     * @param keys the keys whose values will be retrieved.
     * @return the configuration data.
     */
    @Override
    public ConfigData get(String path, Set<String> keys) {

        if (path != null && !path.isEmpty()) {
            log.error("Path is not supported for EnvVarConfigProvider, invalid value '{}'", path);
            throw new ConfigException("Path is not supported for EnvVarConfigProvider, invalid value '" + path + "'");
        }

        if (envVarMap == null) {
            return new ConfigData(new HashMap<>());
        }

        if (keys == null) {
            return new ConfigData(envVarMap);
        }

        HashMap<String, String> filteredData = new HashMap<>(envVarMap);
        filteredData.keySet().retainAll(keys);

        return new ConfigData(filteredData);
    }

    private Map<String, String> getEnvVars() {
        try {
            return System.getenv();
        } catch (Exception e) {
            log.error("Could not read environment variables", e);
            throw new ConfigException("Could not read environment variables");
        }
    }
}
