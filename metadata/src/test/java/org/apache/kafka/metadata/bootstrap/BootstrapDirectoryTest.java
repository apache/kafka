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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableList;
import static org.apache.kafka.metadata.bootstrap.BootstrapDirectory.INTER_BROKER_PROTOCOL_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(40)
public class BootstrapDirectoryTest {
    final static List<ApiMessageAndVersion> SAMPLE_RECORDS1 = unmodifiableList(asList(
            new ApiMessageAndVersion(new FeatureLevelRecord().
                    setName(MetadataVersion.FEATURE_NAME).
                    setFeatureLevel((short) 7), (short) 0),
            new ApiMessageAndVersion(new NoOpRecord(), (short) 0),
            new ApiMessageAndVersion(new NoOpRecord(), (short) 0)));

    @Test
    public void testIbpStringFromConfigMap() {
        assertEquals("3.2", BootstrapDirectory.
                ibpStringFromConfigMap(singletonMap(INTER_BROKER_PROTOCOL_CONFIG_KEY, "3.2")));
        assertEquals("", BootstrapDirectory.
                ibpStringFromConfigMap(emptyMap()));
    }

    static class BootstrapTestDirectory implements AutoCloseable {
        File directory = null;

        synchronized BootstrapTestDirectory createDirectory() throws Exception {
            directory = TestUtils.tempDirectory("BootstrapTestDirectory");
            return this;
        }

        synchronized String path() {
            return directory.getAbsolutePath();
        }

        synchronized String binaryBootstrapPath() {
            return new File(directory, BootstrapDirectory.BINARY_BOOTSTRAP).getAbsolutePath();
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
            assertEquals(BootstrapMetadata.fromVersion(MetadataVersion.latest(),
                            "the default bootstrap file which sets the latest metadata.version, since no " +
                                    "bootstrap file was found, and inter.broker.protocol.version was not configured."),
                    new BootstrapDirectory(testDirectory.path(), "").read());
        }
    }

    @Test
    public void testReadFromConfigurationWithAncientVersion() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            assertEquals(BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_3_IV0,
                "a default bootstrap file setting the minimum supported KRaft metadata version, " +
                    "since no bootstrap file was found, and inter.broker.protocol.version was " +
                        "2.7-IV2, which is not currently supported by KRaft."),
                    new BootstrapDirectory(testDirectory.path(), "2.7").read());
        }
    }

    @Test
    public void testReadFromConfiguration() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            assertEquals(BootstrapMetadata.fromVersion(MetadataVersion.IBP_3_3_IV2,
                "a default bootstrap file setting the metadata.version to 3.3-IV2, as " +
                    "specified by inter.broker.protocol.version."),
                new BootstrapDirectory(testDirectory.path(), "3.3-IV2").read());
        }
    }

    @Test
    public void testMissingDirectory() throws Exception {
        assertEquals("No such directory as ./non/existent/directory",
            assertThrows(RuntimeException.class, () ->
                new BootstrapDirectory("./non/existent/directory", "").read()).getMessage());
    }

    @Test
    public void testReadFromConfigurationFile() throws Exception {
        try (BootstrapTestDirectory testDirectory = new BootstrapTestDirectory().createDirectory()) {
            BootstrapDirectory directory = new BootstrapDirectory(testDirectory.path(), "3.0-IV0");
            BootstrapMetadata metadata = BootstrapMetadata.fromRecords(SAMPLE_RECORDS1,
                    "the binary bootstrap metadata file: " + testDirectory.binaryBootstrapPath());
            directory.writeBinaryFile(metadata);
            assertEquals(metadata, directory.read());
        }
    }
}
