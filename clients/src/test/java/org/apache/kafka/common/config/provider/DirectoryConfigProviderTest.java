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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.StreamSupport;

import static org.apache.kafka.common.config.provider.DirectoryConfigProvider.ALLOWED_PATHS_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectoryConfigProviderTest {

    private DirectoryConfigProvider provider;
    @TempDir
    private Path parent;
    private String dir;
    private final String bar = "bar";
    private final String foo = "foo";
    private String subdir;
    private final String subdirFileName = "subdirFile";
    private String siblingDir;
    private final String siblingDirFileName = "siblingDirFile";
    private final String siblingFileName = "siblingFile";

    private static Path writeFile(Path path) throws IOException {
        return Files.write(path, String.valueOf(path.getFileName()).toUpperCase(Locale.ENGLISH).getBytes(StandardCharsets.UTF_8));
    }

    @BeforeEach
    public void setup() throws IOException {
        provider = new DirectoryConfigProvider();
        provider.configure(Collections.emptyMap());

        dir = Files.createDirectory(Paths.get(parent.toString(), "dir")).toString();
        writeFile(Files.createFile(Paths.get(dir, foo)));
        writeFile(Files.createFile(Paths.get(dir, bar)));

        subdir = Files.createDirectory(Paths.get(dir, "subdir")).toString();
        writeFile(Files.createFile(Paths.get(subdir, subdirFileName)));

        siblingDir = Files.createDirectory(Paths.get(parent.toString(), "siblingDir")).toString();
        writeFile(Files.createFile(Paths.get(siblingDir, siblingDirFileName)));

        writeFile(Files.createFile(Paths.get(parent.toString(), siblingFileName)));
    }

    @AfterEach
    public void close() throws IOException {
        provider.close();
    }

    @Test
    public void testGetAllKeysAtPath() {
        ConfigData configData = provider.get(dir);
        assertEquals(Set.of(foo, bar), configData.data().keySet());
        assertEquals("FOO", configData.data().get(foo));
        assertEquals("BAR", configData.data().get(bar));
        assertNull(configData.ttl());
    }

    @Test
    public void testGetSetOfKeysAtPath() {
        Set<String> keys = Set.of(foo, "baz");
        ConfigData configData = provider.get(dir, keys);
        assertEquals(Collections.singleton(foo), configData.data().keySet());
        assertEquals("FOO", configData.data().get(foo));
        assertNull(configData.ttl());
    }

    @Test
    public void testNoSubdirs() {
        // Only regular files directly in the path directory are allowed, not in subdirs
        Set<String> keys = Set.of(subdir, String.join(File.separator, subdir, subdirFileName));
        ConfigData configData = provider.get(dir, keys);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNoTraversal() {
        // Check we can't escape outside the path directory
        Set<String> keys = Set.of(
                String.join(File.separator, "..", siblingFileName),
                String.join(File.separator, "..", siblingDir),
                String.join(File.separator, "..", siblingDir, siblingDirFileName));
        ConfigData configData = provider.get(dir, keys);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testEmptyPath() {
        ConfigData configData = provider.get("");
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testEmptyPathWithKey() {
        ConfigData configData = provider.get("", Collections.singleton("foo"));
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNullPath() {
        ConfigData configData = provider.get(null);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNullPathWithKey() {
        ConfigData configData = provider.get(null, Collections.singleton("foo"));
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testServiceLoaderDiscovery() {
        ServiceLoader<ConfigProvider> serviceLoader = ServiceLoader.load(ConfigProvider.class);
        assertTrue(StreamSupport.stream(serviceLoader.spliterator(), false).anyMatch(configProvider -> configProvider instanceof DirectoryConfigProvider));
    }

    @Test
    public void testAllowedPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, parent.toString());
        provider.configure(configs);

        ConfigData configData = provider.get(dir);
        assertEquals(Set.of(foo, bar), configData.data().keySet());
        assertEquals("FOO", configData.data().get(foo));
        assertEquals("BAR", configData.data().get(bar));
        assertNull(configData.ttl());
    }

    @Test
    public void testMultipleAllowedPaths() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dir + "," + siblingDir);
        provider.configure(configs);

        ConfigData configData = provider.get(subdir);
        assertEquals(Set.of(subdirFileName), configData.data().keySet());
        assertEquals("SUBDIRFILE", configData.data().get(subdirFileName));
        assertNull(configData.ttl());

        configData = provider.get(siblingDir);
        assertEquals(Set.of(siblingDirFileName), configData.data().keySet());
        assertEquals("SIBLINGDIRFILE", configData.data().get(siblingDirFileName));
        assertNull(configData.ttl());
    }

    @Test
    public void testNotAllowedPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dir);
        provider.configure(configs);

        ConfigData configData = provider.get(siblingDir);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNoTraversalAllowedPath() {
        Map<String, String> configs = new HashMap<>();
        configs.put(ALLOWED_PATHS_CONFIG, dir);
        provider.configure(configs);

        ConfigData configData = provider.get(Paths.get(dir, "..", "siblingDir").toString());
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNonConfiguredProvider() {
        DirectoryConfigProvider provider2 = new DirectoryConfigProvider();
        IllegalStateException ise = assertThrows(IllegalStateException.class, () -> provider2.get(Paths.get(dir).toString()));
        assertEquals("The provider has not been configured yet.", ise.getMessage());
    }
}
