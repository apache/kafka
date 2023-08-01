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
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.apache.kafka.test.TestUtils.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DirectoryConfigProviderTest {

    private DirectoryConfigProvider provider;
    private File parent;
    private File dir;
    private File bar;
    private File foo;
    private File subdir;
    private File subdirFile;
    private File siblingDir;
    private File siblingDirFile;
    private File siblingFile;

    private static File writeFile(File file) throws IOException {
        Files.write(file.toPath(), file.getName().toUpperCase(Locale.ENGLISH).getBytes(StandardCharsets.UTF_8));
        return file;
    }

    @BeforeEach
    public void setup() throws IOException {
        provider = new DirectoryConfigProvider();
        provider.configure(Collections.emptyMap());
        parent = TestUtils.tempDirectory();
        dir = new File(parent, "dir");
        dir.mkdir();
        foo = writeFile(new File(dir, "foo"));
        bar = writeFile(new File(dir, "bar"));
        subdir = new File(dir, "subdir");
        subdir.mkdir();
        subdirFile = writeFile(new File(subdir, "subdirFile"));
        siblingDir = new File(parent, "siblingdir");
        siblingDir.mkdir();
        siblingDirFile = writeFile(new File(siblingDir, "siblingdirFile"));
        siblingFile = writeFile(new File(parent, "siblingFile"));
    }

    @AfterEach
    public void close() throws IOException {
        provider.close();
        Utils.delete(parent);
    }

    @Test
    public void testGetAllKeysAtPath() {
        ConfigData configData = provider.get(dir.getAbsolutePath());
        assertEquals(toSet(asList(foo.getName(), bar.getName())), configData.data().keySet());
        assertEquals("FOO", configData.data().get(foo.getName()));
        assertEquals("BAR", configData.data().get(bar.getName()));
        assertNull(configData.ttl());
    }

    @Test
    public void testGetSetOfKeysAtPath() {
        Set<String> keys = toSet(asList(foo.getName(), "baz"));
        ConfigData configData = provider.get(dir.getAbsolutePath(), keys);
        assertEquals(Collections.singleton(foo.getName()), configData.data().keySet());
        assertEquals("FOO", configData.data().get(foo.getName()));
        assertNull(configData.ttl());
    }

    @Test
    public void testNoSubdirs() {
        // Only regular files directly in the path directory are allowed, not in subdirs
        Set<String> keys = toSet(asList(subdir.getName(), String.join(File.separator, subdir.getName(), subdirFile.getName())));
        ConfigData configData = provider.get(dir.getAbsolutePath(), keys);
        assertTrue(configData.data().isEmpty());
        assertNull(configData.ttl());
    }

    @Test
    public void testNoTraversal() {
        // Check we can't escape outside the path directory
        Set<String> keys = toSet(asList(
                String.join(File.separator, "..", siblingFile.getName()),
                String.join(File.separator, "..", siblingDir.getName()),
                String.join(File.separator, "..", siblingDir.getName(), siblingDirFile.getName())));
        ConfigData configData = provider.get(dir.getAbsolutePath(), keys);
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
}

