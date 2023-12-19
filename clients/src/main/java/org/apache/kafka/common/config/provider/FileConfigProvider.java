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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * An implementation of {@link ConfigProvider} that represents a Properties file.
 * All property keys and values are stored as cleartext.
 */
public class FileConfigProvider implements ConfigProvider {

    private static final Logger log = LoggerFactory.getLogger(FileConfigProvider.class);

    public static final String ALLOWED_PATHS_CONFIG = "allowed.paths";
    public static final String ALLOWED_PATHS_DOC = "Path that this config provider is allowed to access";
    private List<Path> allowedPaths;

    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(ALLOWED_PATHS_CONFIG)) {
            String configValue = (String) configs.get(ALLOWED_PATHS_CONFIG);

            if (configValue != null && !configValue.isEmpty()) {
                allowedPaths = new ArrayList<>();
                Arrays.stream(configValue.split(",")).forEach(b -> allowedPaths.add(Paths.get(b)));
            }
        } else {
            allowedPaths = null;
        }
    }

    /**
     * Retrieves the data at the given Properties file.
     *
     * @param path the file where the data resides
     * @return the configuration data
     */
    public ConfigData get(String path) {
        Map<String, String> data = new HashMap<>();
        if (path == null || path.isEmpty()) {
            return new ConfigData(data);
        }

        Path filePath = Paths.get(path);
        if (allowedPaths != null) {
            long allowed = allowedPaths.stream().filter(allowedPath -> filePath.startsWith(allowedPath) || filePath.equals(allowedPath)).count();
            if (allowed == 0) {
                log.warn("The path {} is not allowed to be accessed", path);
                return new ConfigData(data);
            }
        }

        try (Reader reader = reader(filePath)) {
            Properties properties = new Properties();
            properties.load(reader);
            Enumeration<Object> keys = properties.keys();
            while (keys.hasMoreElements()) {
                String key = keys.nextElement().toString();
                String value = properties.getProperty(key);
                if (value != null) {
                    data.put(key, value);
                }
            }
            return new ConfigData(data);
        } catch (IOException e) {
            log.error("Could not read properties from file {}", path, e);
            throw new ConfigException("Could not read properties from file " + path);
        }
    }

    /**
     * Retrieves the data with the given keys at the given Properties file.
     *
     * @param path the file where the data resides
     * @param keys the keys whose values will be retrieved
     * @return the configuration data
     */
    public ConfigData get(String path, Set<String> keys) {
        Map<String, String> data = new HashMap<>();
        if (path == null || path.isEmpty()) {
            return new ConfigData(data);
        }

        Path filePath = Paths.get(path);
        if (allowedPaths != null) {
            long allowed = allowedPaths.stream().filter(allowedPath -> filePath.startsWith(allowedPath)).count();
            if (allowed == 0) {
                log.warn("The path {} is not allowed to be accessed", path);
                return new ConfigData(data);
            }
        }

        try (Reader reader = reader(filePath)) {
            Properties properties = new Properties();
            properties.load(reader);
            for (String key : keys) {
                String value = properties.getProperty(key);
                if (value != null) {
                    data.put(key, value);
                }
            }
            return new ConfigData(data);
        } catch (IOException e) {
            log.error("Could not read properties from file {}", path, e);
            throw new ConfigException("Could not read properties from file " + path);
        }
    }

    // visible for testing
    protected Reader reader(Path path) throws IOException {
        return Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    public void close() {
    }
}
