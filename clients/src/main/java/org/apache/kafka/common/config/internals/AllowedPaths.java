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
package org.apache.kafka.common.config.internals;

import org.apache.kafka.common.config.ConfigException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AllowedPaths {
    private final List<Path> allowedPaths;

    /**
     * Constructs AllowedPaths with a list of Paths retrieved from {@code configValue}.
     * @param configValue {@code allowed.paths} config value which is a string containing comma separated list of paths
     * @throws ConfigException if any of the given paths is not absolute or does not exist.
     */
    public AllowedPaths(String configValue) {
        this.allowedPaths = getAllowedPaths(configValue);
    }

    private List<Path> getAllowedPaths(String configValue) {
        if (configValue != null && !configValue.isEmpty()) {
            List<Path> allowedPaths = new ArrayList<>();

            Arrays.stream(configValue.split(",")).forEach(b -> {
                Path normalisedPath = Paths.get(b).normalize();

                if (!normalisedPath.isAbsolute()) {
                    throw new ConfigException("Path " + normalisedPath + " is not absolute");
                } else if (!Files.exists(normalisedPath)) {
                    throw new ConfigException("Path " + normalisedPath + " does not exist");
                } else {
                    allowedPaths.add(normalisedPath);
                }
            });

            return allowedPaths;
        }

        return null;
    }

    /**
     * Checks if the given {@code path} resides in the configured {@code allowed.paths}.
     * If {@code allowed.paths} is not configured, the given Path is returned as allowed.
     * @param path the Path to check if allowed
     * @return Path that can be accessed or null if the given Path does not reside in the configured {@code allowed.paths}.
     */
    public Path parseUntrustedPath(String path) {
        Path parsedPath = Paths.get(path);

        if (allowedPaths != null) {
            Path normalisedPath = parsedPath.normalize();
            long allowed = allowedPaths.stream().filter(allowedPath -> normalisedPath.startsWith(allowedPath)).count();
            if (allowed == 0) {
                return null;
            }
            return normalisedPath;
        }

        return parsedPath;
    }
}
