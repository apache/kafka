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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link ConfigProvider} which helps to retrieve environment variables
 * Also a default value could be set in case a non-existing environment variable is referenced
 * separating the variable name and the default value with a space.
 */
public class EnvConfigProvider implements ConfigProvider {

    private static void assertNoPath(String path) {
        if (path != null && !path.isEmpty()) {
            throw new ConfigException("EnvConfigProvider does not support paths. Found: " + path);
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // noop
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    @Override
    public ConfigData get(String path) {
        assertNoPath(path);
        return new ConfigData(System.getenv());
    }

    /**
     * Retrieves the data with the given keys at the given Properties file.
     *
     * @param path null or empty - not used
     * @param keys the environment variable names whose values will be retrieved, with optional default value separated with a space
     * @return the configuration data
     */
    @Override
    public ConfigData get(String path, Set<String> keys) {
        assertNoPath(path);
        Map<String, String> result = new HashMap<>();
        for (String key : keys) {
            String[] strings = key.split(" ");
            if (strings.length == 2) {
                String defVal = strings[1];
                String envVal = getEnv(strings[0]);
                result.put(key, envVal != null ? envVal : defVal);
            } else {
                String envVal = getEnv(key);
                if (envVal != null) {
                    result.put(key, envVal);
                }
            }
        }
        return new ConfigData(result);
    }

    // visible for testing
    String getEnv(String variable) {
        return System.getenv(variable);
    }
}
