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
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * An implementation of {@link ConfigProvider} that can read from either a file or a directory.
 * If the given path is a file, it is interpreted as a Properties file containing key-value pairs.
 * If the given path is a directory, the keys are the file names contained in the directory and the values are
 * the corresponding contents of the files.
 * All property keys and values are stored as cleartext.
 */
public class FileConfigProvider implements ConfigProvider {

    public void configure(Map<String, ?> configs) {
    }

    /**
     * Retrieves the data at the given path.
     *
     * @param path the path corresponding to either a directory or a Properties file
     * @return the configuration data
     */
    public ConfigData get(String path) {
        if (path == null || path.isEmpty()) {
            return new ConfigData(new HashMap<>());
        }
        Path p = Paths.get(path);
        return Files.isDirectory(p) ? getFromDirectory(p) : getFromPropertiesFile(p);
    }

    /**
     * Retrieves the data with the given keys at the given path.
     *
     * @param path the path corresponding to either a directory or a Properties file
     * @param keys the keys whose values will be retrieved.  In the case of a directory, these are the file names.
     * @return the configuration data
     */
    public ConfigData get(String path, Set<String> keys) {
        if (path == null || path.isEmpty()) {
            return new ConfigData(new HashMap<>());
        }
        Path p = Paths.get(path);
        return Files.isDirectory(p) ? getFromDirectory(p, keys) : getFromPropertiesFile(p, keys);
    }

    private ConfigData getFromDirectory(Path path) {
        try {
            Map<String, String> data = Files.list(path)
                    .filter(Files::isRegularFile)
                    .collect(Collectors.toMap(file -> file.getFileName().toString(), this::toString));
            return new ConfigData(data);
        } catch (IOException e) {
            throw new ConfigException("Could not read from directory " + path);
        }
    }

    private ConfigData getFromDirectory(Path path, Set<String> keys) {
        Map<String, String> data = keys.stream()
                .map(path::resolve)
                .filter(Files::isRegularFile)
                .collect(Collectors.toMap(file -> file.getFileName().toString(), this::toString));
        return new ConfigData(data);
    }

    private ConfigData getFromPropertiesFile(Path path) {
        Map<String, String> data = new HashMap<>();
        try (Reader reader = reader(path)) {
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
            throw new ConfigException("Could not read properties from file " + path);
        }
    }

    private ConfigData getFromPropertiesFile(Path path, Set<String> keys) {
        Map<String, String> data = new HashMap<>();
        try (Reader reader = reader(path)) {
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
            throw new ConfigException("Could not read properties from file " + path);
        }
    }

    private String toString(Path path) {
        try {
            return new String(Files.readAllBytes(path), UTF_8);
        } catch (IOException e) {
            throw new ConfigException("Could not read from file " + path);
        }
    }

    private Reader reader(Path path) throws IOException {
        return Files.newBufferedReader(path);
    }

    public void close() {
    }
}
