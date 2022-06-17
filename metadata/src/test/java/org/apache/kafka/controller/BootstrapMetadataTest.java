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

package org.apache.kafka.controller;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BootstrapMetadataTest {
    private Path tmpDir;

    @BeforeEach
    public void createTestDir() {
        tmpDir = TestUtils.tempDirectory("BootstrapMetadataTest").toPath();
    }

    @AfterEach
    public void deleteTestDir() throws IOException {
        if (tmpDir != null)
            Utils.delete(tmpDir.toFile());
    }

    @Test
    public void testWriteAndReadBootstrapFile() throws Exception {
        BootstrapMetadata metadata = BootstrapMetadata.create(MetadataVersion.MINIMUM_KRAFT_VERSION);
        BootstrapMetadata.write(metadata, tmpDir);

        assertTrue(Files.exists(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE)));

        BootstrapMetadata newMetadata = BootstrapMetadata.load(tmpDir, () -> MetadataVersion.MINIMUM_KRAFT_VERSION);
        assertEquals(metadata, newMetadata);
    }

    @Test
    public void testNoBootstrapFile() throws Exception {
        BootstrapMetadata metadata = BootstrapMetadata.load(tmpDir, () -> MetadataVersion.MINIMUM_KRAFT_VERSION);
        assertEquals(MetadataVersion.MINIMUM_KRAFT_VERSION, metadata.metadataVersion());
        metadata = BootstrapMetadata.load(tmpDir, () -> MetadataVersion.IBP_3_2_IV0);
        assertEquals(MetadataVersion.IBP_3_2_IV0, metadata.metadataVersion());
    }

    @Test
    public void testExistingBootstrapFile() throws Exception {
        BootstrapMetadata.write(BootstrapMetadata.create(MetadataVersion.MINIMUM_KRAFT_VERSION), tmpDir);
        assertThrows(IOException.class, () -> {
            BootstrapMetadata.write(BootstrapMetadata.create(MetadataVersion.IBP_3_1_IV0), tmpDir);
        });
    }

    @Test
    public void testEmptyBootstrapFile() throws Exception {
        Files.createFile(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE));
        assertThrows(Exception.class, () -> BootstrapMetadata.load(tmpDir, () -> MetadataVersion.MINIMUM_KRAFT_VERSION),
            "Should fail to load if no metadata.version is set");
    }

    @Test
    public void testGarbageBootstrapFile() throws Exception {
        Files.createFile(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE));
        Random random = new Random(1);
        byte[] data = new byte[100];
        random.nextBytes(data);
        Files.write(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE), data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        assertThrows(Exception.class, () -> BootstrapMetadata.load(tmpDir, () -> MetadataVersion.MINIMUM_KRAFT_VERSION),
            "Should fail on invalid data");
    }
}
