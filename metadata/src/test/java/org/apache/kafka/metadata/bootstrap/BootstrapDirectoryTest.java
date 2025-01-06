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

package org.apache.kafka.metadata.bootstrap;

import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.NoOpRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(40)
public class BootstrapDirectoryTest {
    static final List<ApiMessageAndVersion> SAMPLE_RECORDS1 = List.of(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                    setName(MetadataVersion.FEATURE_NAME).
                    setFeatureLevel((short) 7), (short) 0),
            new ApiMessageAndVersion(new NoOpRecord(), (short) 0),
            new ApiMessageAndVersion(new NoOpRecord(), (short) 0));

    static class BootstrapTestDirectory implements AutoCloseable {
        File directory = null;

        synchronized BootstrapTestDirectory createDirectory() {
            directory = TestUtils.tempDirectory("BootstrapTestDirectory");
            return this;
        }

        synchronized String path() {
            return directory.getAbsolutePath();
        }

        synchronized String binaryBootstrapPath() {
            return new File(directory, BootstrapDirectory.BINARY_BOOTSTRAP_FILENAME).getAbsolutePath();
        }

        @Override
        public synchronized void close() throws Exception {
            if (directory != null) {
                Utils.delete(directory);
            }
            directory = null;
        }
    }

    @Test
    public void testReadFromEmptyConfiguration() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            assertEquals(BootstrapMetadata.fromVersion(MetadataVersion.latestProduction(),
                    "the default bootstrap"),
                new BootstrapDirectory(testDirectory.path(), Optional.empty()).read());
        }
    }

    @Test
    public void testReadFromConfigurationWithAncientVersion() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            assertEquals(BootstrapMetadata.fromVersion(MetadataVersion.MINIMUM_BOOTSTRAP_VERSION,
                    "the minimum version bootstrap with metadata.version 3.3-IV0"),
                new BootstrapDirectory(testDirectory.path(), Optional.of("2.7")).read());
        }
    }

    @Test
    public void testReadFromConfiguration() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            assertEquals(BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_3_IV2,
                    "the configured bootstrap with metadata.version 3.3-IV2"),
                new BootstrapDirectory(testDirectory.path(), Optional.of("3.3-IV2")).read());
        }
    }

    @Test
    public void testMissingDirectory() {
        assertEquals("No such directory as ./non/existent/directory",
            assertThrows(RuntimeException.class, () ->
                new BootstrapDirectory("./non/existent/directory", Optional.empty()).read()).getMessage());
    }

    @Test
    public void testReadFromConfigurationFile() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            BootstrapDirectory directory = new BootstrapDirectory(testDirectory.path(), Optional.of("3.0-IV0"));
            BootstrapMetadata metadata = BootstrapMetadata.fromRecords(SAMPLE_RECORDS1,
                    "the binary bootstrap metadata file: " + testDirectory.binaryBootstrapPath());
            directory.writeBinaryFile(metadata);
            assertEquals(metadata, directory.read());
        }
    }
}
