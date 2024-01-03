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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfigProviderUtils {

    /**
     * Reads the given {@code configValue} and creates a list of paths.
     * @param configValue allowed.paths config value which is a string containing comma separated list of paths
     * @return List of paths or null if the {@code configValue} is null or empty string.
     */
    public static List<Path> configureAllowedPaths(String configValue) {
        if (configValue != null && !configValue.isEmpty()) {
            List<Path> allowedPaths = new ArrayList<>();
            Arrays.stream(configValue.split(",")).forEach(b -> allowedPaths.add(Paths.get(b).normalize()));

            return allowedPaths;
        }
        return null;
    }

    /**
     * Checks if the given {@code path} resides one of the {@code allowedPaths}.
     * if {@code allowedPaths} is null, returns the path to indicate that it is allowed.
     * @param path the path to check if allowed
     * @param allowedPaths the list of the allowed paths to check
     * @return path that can be accessed or null if the given path does not reside in one of the allowed paths.
     */
    public static Path pathIsAllowed(Path path, List<Path> allowedPaths) {
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
