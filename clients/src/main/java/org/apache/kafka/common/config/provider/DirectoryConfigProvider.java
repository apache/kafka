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

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.config.ConfigData;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;

/**
 * An implementation of {@link ConfigProvider} based on a directory of files.
 * Property keys correspond to the names of the regular (i.e. non-directory)
 * files in a directory given by the path parameter.
 * Property values are taken from the file contents corresponding to each key.
 */
public class DirectoryConfigProvider implements ConfigProvider {

    private static final Logger log = LoggerFactory.getLogger(DirectoryConfigProvider.class);

    public static final String ALLOWED_PATHS_CONFIG = "allowed.paths";
    public static final String ALLOWED_PATHS_DOC = "Path that this config provider is allowed to access";
    private List<Path> allowedPaths;

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(ALLOWED_PATHS_CONFIG)) {
            String configValue = (String) configs.get(ALLOWED_PATHS_CONFIG);

            if (configValue != null && !configValue.isEmpty()) {
                allowedPaths = new ArrayList<>();
                Arrays.stream(configValue.split(",")).forEach(b -> allowedPaths.add(Paths.get(b).normalize()));
            }
        } else {
            allowedPaths = null;
        }
    }

    @Override
    public void close() throws IOException { }

    /**
     * Retrieves the data contained in regular files in the directory given by {@code path}.
     * Non-regular files (such as directories) in the given directory are silently ignored.
     * @param path the directory where data files reside.
     * @return the configuration data.
     */
    @Override
    public ConfigData get(String path) {
        return get(path, Files::isRegularFile);
    }

    /**
     * Retrieves the data contained in the regular files named by {@code keys} in the directory given by {@code path}.
     * Non-regular files (such as directories) in the given directory are silently ignored.
     * @param path the directory where data files reside.
     * @param keys the keys whose values will be retrieved.
     * @return the configuration data.
     */
    @Override
    public ConfigData get(String path, Set<String> keys) {
        return get(path, pathname ->
                Files.isRegularFile(pathname)
                        && keys.contains(pathname.getFileName().toString()));
    }

    private ConfigData get(String path, Predicate<Path> fileFilter) {
        Map<String, String> map = emptyMap();
        if (path != null && !path.isEmpty()) {
            Path dir = Paths.get(path).normalize();
            if (!Files.isDirectory(dir)) {
                log.warn("The path {} is not a directory", path);
            } else {
                if (allowedPaths != null) {
                    long allowed = allowedPaths.stream().filter(allowedPath -> dir.startsWith(allowedPath)).count();
                    if (allowed == 0) {
                        log.warn("The path {} is not allowed to be accessed", path);
                        return new ConfigData(map);
                    }
                }

                try (Stream<Path> stream = Files.list(dir)) {
                    map = stream
                        .filter(fileFilter)
                        .collect(Collectors.toMap(
                            p -> p.getFileName().toString(),
                            p -> read(p)));
                } catch (IOException e) {
                    log.error("Could not list directory {}", dir, e);
                    throw new ConfigException("Could not list directory " + dir);
                }
            }
        }
        return new ConfigData(map);
    }

    private static String read(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("Could not read file {} for property {}", path, path.getFileName(), e);
            throw new ConfigException("Could not read file " + path + " for property " + path.getFileName());
        }
    }

}
