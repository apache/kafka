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

import org.apache.kafka.server.common.MetadataVersion;
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
    @Test
    public void testWriteAndReadBootstrapFile() throws Exception {
        Path tmpDir = Files.createTempDirectory("BootstrapMetadataTest");
        BootstrapMetadata metadata = BootstrapMetadata.create(MetadataVersion.IBP_3_0_IV0);
        BootstrapMetadata.write(metadata, tmpDir);

        assertTrue(Files.exists(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE)));

        BootstrapMetadata newMetadata = BootstrapMetadata.load(tmpDir, MetadataVersion.IBP_3_0_IV0);
        assertEquals(metadata, newMetadata);
    }

    @Test
    public void testNoBootstrapFile() throws Exception {
        Path tmpDir = Files.createTempDirectory("BootstrapMetadataTest");
        BootstrapMetadata metadata = BootstrapMetadata.load(tmpDir, MetadataVersion.IBP_3_0_IV0);
        assertEquals(MetadataVersion.IBP_3_0_IV0, metadata.metadataVersion());
        metadata = BootstrapMetadata.load(tmpDir, MetadataVersion.IBP_3_2_IV0);
        assertEquals(MetadataVersion.IBP_3_2_IV0, metadata.metadataVersion());
    }

    @Test
    public void testExistingBootstrapFile() throws Exception {
        Path tmpDir = Files.createTempDirectory("BootstrapMetadataTest");
        BootstrapMetadata.write(BootstrapMetadata.create(MetadataVersion.IBP_3_0_IV0), tmpDir);
        assertThrows(IOException.class, () -> {
            BootstrapMetadata.write(BootstrapMetadata.create(MetadataVersion.IBP_3_1_IV0), tmpDir);
        });
    }

    @Test
    public void testEmptyBootstrapFile() throws Exception {
        Path tmpDir = Files.createTempDirectory("BootstrapMetadataTest");
        Files.createFile(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE));
        assertThrows(RuntimeException.class, () -> BootstrapMetadata.load(tmpDir, MetadataVersion.IBP_3_0_IV0),
            "Should fail to load if no metadata.version is set");
    }

    @Test
    public void testGarbageBootstrapFile() throws Exception {
        Path tmpDir = Files.createTempDirectory("BootstrapMetadataTest");
        Files.createFile(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE));
        Random random = new Random(1);
        byte[] data = new byte[100];
        random.nextBytes(data);
        Files.write(tmpDir.resolve(BootstrapMetadata.BOOTSTRAP_FILE), data, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        assertThrows(RuntimeException.class, () -> BootstrapMetadata.load(tmpDir, MetadataVersion.IBP_3_0_IV0),
            "Should fail on invalid data");
    }
}
