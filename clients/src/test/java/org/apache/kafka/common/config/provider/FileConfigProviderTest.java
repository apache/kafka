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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileConfigProviderTest {

    private File testDir;
    private Path testFile;
    private File testDir2;
    private FileConfigProvider configProvider;

    @Before
    public void setup() throws IOException {
        testDir = TestUtils.tempDirectory();
        testFile = Paths.get(testDir.toPath().toString(), "testFile.properties");
        try (BufferedWriter writer = Files.newBufferedWriter(testFile)) {
            writer.write("testKey=testResult\ntestKey2=testResult2");
        }

        testDir2 = TestUtils.tempDirectory();
        Path testFile1 = Paths.get(testDir2.toPath().toString(), "testKey.txt");
        Path testFile2 = Paths.get(testDir2.toPath().toString(), "testKey2.txt");
        try (BufferedWriter writer1 = Files.newBufferedWriter(testFile1)) {
            writer1.write("testResult");
        }
        try (BufferedWriter writer2 = Files.newBufferedWriter(testFile2)) {
            writer2.write("testResult2");
        }

        configProvider = new FileConfigProvider();
    }

    @After
    public void cleanup() throws IOException {
        Utils.delete(testDir);
        Utils.delete(testDir2);
    }

    @Test
    public void testGetAllKeysInFile() throws Exception {
        ConfigData configData = configProvider.get(testFile.toString());
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        result.put("testKey2", "testResult2");
        assertEquals(result, configData.data());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testGetOneKeyInFile() throws Exception {
        ConfigData configData = configProvider.get(testFile.toString(), Collections.singleton("testKey"));
        Map<String, String> result = new HashMap<>();
        result.put("testKey", "testResult");
        assertEquals(result, configData.data());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testGetAllKeysInDir() throws Exception {
        ConfigData configData = configProvider.get(testDir2.getPath());
        Map<String, String> result = new HashMap<>();
        result.put("testKey.txt", "testResult");
        result.put("testKey2.txt", "testResult2");
        assertEquals(result, configData.data());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testGetOneKeyInDir() throws Exception {
        ConfigData configData = configProvider.get(testDir2.getPath(), Collections.singleton("testKey.txt"));
        Map<String, String> result = new HashMap<>();
        result.put("testKey.txt", "testResult");
        assertEquals(result, configData.data());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testEmptyPath() throws Exception {
        ConfigData configData = configProvider.get("", Collections.singleton("testKey"));
        assertTrue(configData.data().isEmpty());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testEmptyPathWithKey() throws Exception {
        ConfigData configData = configProvider.get("");
        assertTrue(configData.data().isEmpty());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testNullPath() throws Exception {
        ConfigData configData = configProvider.get(null);
        assertTrue(configData.data().isEmpty());
        assertEquals(null, configData.ttl());
    }

    @Test
    public void testNullPathWithKey() throws Exception {
        ConfigData configData = configProvider.get(null, Collections.singleton("testKey"));
        assertTrue(configData.data().isEmpty());
        assertEquals(null, configData.ttl());
    }
}
