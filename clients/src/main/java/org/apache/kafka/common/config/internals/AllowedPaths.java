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
    private List<Path> allowedPaths;

    private AllowedPaths(List<Path> allowedPaths) {
        this.allowedPaths = allowedPaths;
    }

    /**
     * Constructs AllowedPaths with a list of Paths retrieved from {@code configValue}.
     * @param configValue {@code allowed.paths} config value which is a string containing comma separated list of paths
     * @return AllowedPaths with a list of Paths or null list if the {@code configValue} is null or empty string.
     */
    public static AllowedPaths configureAllowedPaths(String configValue) {
        if (configValue != null && !configValue.isEmpty()) {
            List<Path> allowedPaths = new ArrayList<>();

            Arrays.stream(configValue.split(",")).forEach(b -> {
                Path normalisedPath = Paths.get(b).normalize();

                if (normalisedPath.isAbsolute() && Files.exists(normalisedPath)) {
                    allowedPaths.add(normalisedPath);
                } else {
                    throw new ConfigException("Path " + normalisedPath + " is not valid. The path should be absolute and exist");
                }
            });

            return new AllowedPaths(allowedPaths);
        }

        return new AllowedPaths(null);
    }

    /**
     * Checks if the given {@code path} resides in the configured {@code allowed.paths}.
     * If {@code allowed.paths} is not configured, the given Path is returned as allowed.
     * @param path the Path to check if allowed
     * @return Path that can be accessed or null if the given Path does not reside in the configured {@code allowed.paths}.
     */
    public Path getIfPathIsAllowed(Path path) {
        Path normalisedPath = path.normalize();

        if (allowedPaths != null) {
            long allowed = allowedPaths.stream().filter(allowedPath -> path.normalize().startsWith(allowedPath)).count();
            if (allowed == 0) {
                return null;
            }
            return normalisedPath;
        }

        return path;
    }
}
