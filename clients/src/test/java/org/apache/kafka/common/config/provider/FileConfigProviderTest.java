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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

import static org.apache.kafka.common.config.provider.DirectoryConfigProvider.ALLOWED_PATHS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FileConfigProviderTest {

    private FileConfigProvider configProvider;
    @TempDir
    private File parent;
    private String dir;
    private String dirFile;
    private String siblingDir;
    private String siblingDirFile;

    @BeforeEach
    public void setup() throws IOException {
        configProvider = new TestFileConfigProvider();
        configProvider.configure(Collections.emptyMap());

        dir = Files.createDirectory(Paths.get(parent.toString(), "dir")).toString();
        dirFile = Files.createFile(Paths.get(dir, "dirFile")).toString();

        siblingDir = Files.createDirectory(Paths.get(parent.toString(), "siblingDir")).toString();
        siblingDirFile = Files.createFile(Paths.get(siblingDir, "siblingDirFile")).toString();
    }

    @Test
    public void testGetAllKeysAtPath() {
        ConfigData configData = configProvider.get("dummy");
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        result.put("testKey2", "testResult2");
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testGetOneKeyAtPath() {
        ConfigData configData = configProvider.get("dummy", Collections.singleton("testKey"));
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testEmptyPath() {
        ConfigData configData = configProvider.get("", Collections.singleton("testKey"));
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testEmptyPathWithKey() {
        ConfigData configData = configProvider.get("");
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNullPath() {
        ConfigData configData = configProvider.get(null);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNullPathWithKey() {
        ConfigData configData = configProvider.get(null, Collections.singleton("testKey"));
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testServiceLoaderDiscovery() {
        ServiceLoader<ConfigProvider> serviceLoader = ServiceLoader.load(ConfigProvider.class);
        assertTrue(StreamSupport.stream(serviceLoader.spliterator(), false).anyMatch(configProvider -> configProvider instanceof FileConfigProvider));
    }

    public static class TestFileConfigProvider extends FileConfigProvider {

        @Override
        protected Reader reader(Path path) throws IOException {
            return new StringReader("testKey=testResult\ntestKey2=testResult2");
        }
    }

    @Test
    public void testAllowedDirPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dir);
        configProvider.configure(configs);

        ConfigData configData = configProvider.get(dirFile);
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        result.put("testKey2", "testResult2");
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testAllowedFilePath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dirFile);
        configProvider.configure(configs);

        ConfigData configData = configProvider.get(dirFile);
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        result.put("testKey2", "testResult2");
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testMultipleAllowedPaths() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dir + "," + siblingDir);
        configProvider.configure(configs);

        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        result.put("testKey2", "testResult2");

        ConfigData configData = configProvider.get(dirFile);
        assertEquals(result, configData.data());
        assertNull(configData.ttl());

        configData = configProvider.get(siblingDirFile);
        assertEquals(result, configData.data());
        assertNull(configData.ttl());
    }

    @Test
    public void testNotAllowedDirPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dir);
        configProvider.configure(configs);

        ConfigData configData = configProvider.get(siblingDirFile);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNotAllowedFilePath() throws IOException {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dirFile);
        configProvider.configure(configs);

        //another file under the same directory
        Path dirFile2 = Files.createFile(Paths.get(dir, "dirFile2"));
        ConfigData configData = configProvider.get(dirFile2.toString());
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNoTraversal() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dirFile);
        configProvider.configure(configs);

        // Check we can't escape outside the path directory
        ConfigData configData = configProvider.get(Paths.get(dirFile, "..", "..", "siblingDir", "siblingDirFile").toString());
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNonConfiguredProvider() {
        FileConfigProvider provider2 = new FileConfigProvider();
        IllegalStateException ise = assertThrows(IllegalStateException.class, () -> provider2.get(Paths.get(dirFile).toString()));
        assertEquals("The provider has not been configured yet.", ise.getMessage());
    }
}
