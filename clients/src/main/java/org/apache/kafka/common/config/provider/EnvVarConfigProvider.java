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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * An implementation of {@link ConfigProvider} based on environment variables.
 * Keys correspond to the names of the environment variables, paths are currently not being used.
 * Using an allowlist pattern {@link EnvVarConfigProvider#ALLOWLIST_PATTERN_CONFIG} that supports regular expressions,
 * it is possible to limit access to specific environment variables. Default allowlist pattern is ".*".
 */
public class EnvVarConfigProvider implements ConfigProvider {

    private static final Logger log = LoggerFactory.getLogger(EnvVarConfigProvider.class);

    public static final String ALLOWLIST_PATTERN_CONFIG = "allowlist.pattern";
    public static final String ALLOWLIST_PATTERN_CONFIG_DOC = "A pattern / regular expression that needs to match for environment variables" +
            " to be used by this config provider.";
    private final Map<String, String> envVarMap;
    private Map<String, String> filteredEnvVarMap;

    public EnvVarConfigProvider() {
        envVarMap = getEnvVars();
    }

    public EnvVarConfigProvider(Map<String, String> envVarsAsArgument) {
        envVarMap = envVarsAsArgument;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Pattern envVarPattern;

        if (configs.containsKey(ALLOWLIST_PATTERN_CONFIG)) {
            envVarPattern = Pattern.compile(
                    String.valueOf(configs.get(ALLOWLIST_PATTERN_CONFIG))
            );
        } else {
            envVarPattern = Pattern.compile(".*");
            log.info("No pattern for environment variables provided. Using default pattern '(.*)'.");
        }

        filteredEnvVarMap = envVarMap.entrySet().stream()
                .filter(envVar -> envVarPattern.matcher(envVar.getKey()).matches())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                );
    }

    @Override
    public void close() throws IOException {
    }

    /**
     * @param path unused
     * @return returns environment variables as configuration
     */
    @Override
    public ConfigData get(String path) {
        return get(path, null);
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

        if (keys == null) {
            return new ConfigData(filteredEnvVarMap);
        }

        Map<String, String> filteredData = new HashMap<>(filteredEnvVarMap);
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
