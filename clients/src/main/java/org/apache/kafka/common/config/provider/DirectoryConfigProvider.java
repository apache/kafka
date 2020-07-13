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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link ConfigProvider} based on a directory of files.
 * Property keys correspond to the names of the regular (i.e. non-directory)
 * files in a directory given by the path parameter.
 * Property values are taken from the file contents corresponding to each key.
 */
public class DirectoryConfigProvider implements ConfigProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public void configure(Map<String, ?> configs) { }

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
        return get(path, File::isFile);
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
                pathname.isFile()
                        && keys.contains(pathname.getName()));
    }

    private ConfigData get(String path, FileFilter fileFilter) {
        Map<String, String> map = new HashMap<>();
        if (path != null && !path.isEmpty()) {
            File dir = new File(path);
            if (!dir.isDirectory()) {
                log.warn("The path {} is not a directory", path);
            } else {
                for (File file : dir.listFiles(fileFilter)) {
                    try {
                        map.put(file.getName(), read(file.toPath()));
                    } catch (IOException e) {
                        throw new ConfigException("Could not read file " + file.getAbsolutePath() + " for property " + file.getName(), e);
                    }
                }
            }
        }
        return new ConfigData(map);
    }

    private String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

}
