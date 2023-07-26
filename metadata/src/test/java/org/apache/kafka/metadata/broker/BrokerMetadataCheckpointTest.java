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

package org.apache.kafka.metadata.broker;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BrokerMetadataCheckpointTest {
    private final String clusterIdBase64 = "H3KKO4NTRPaCWtEmm3vW7A";

    @Test
    public void testReadWithNonExistentFile() throws IOException {
        assertEquals(Optional.empty(), new BrokerMetadataCheckpoint(new File("path/that/does/not/exist")).read());
    }

    @Test
    public void testCreateZkMetadataProperties() {
        ZkMetaProperties meta = new ZkMetaProperties("7bc79ca1-9746-42a3-a35a-efb3cde44492", 3);
        Properties properties = meta.toProperties();
        RawMetaProperties parsed = new RawMetaProperties(properties);
        assertEquals(0, parsed.getVersion());
        assertEquals(meta.getClusterId(), parsed.getClusterId().get());
        assertEquals(meta.getBrokerId(), parsed.getBrokerId().get());
    }

    @Test
    public void testParseRawMetaPropertiesWithoutVersion() {
        int brokerId = 1;
        String clusterId = "7bc79ca1-9746-42a3-a35a-efb3cde44492";

        Properties properties = new Properties();
        properties.put(RawMetaProperties.BROKER_ID_KEY, Integer.toString(brokerId));
        properties.put(RawMetaProperties.CLUSTER_ID_KEY, clusterId);

        RawMetaProperties parsed = new RawMetaProperties(properties);
        assertEquals(brokerId, (int) parsed.getBrokerId().get());
        assertEquals(clusterId, parsed.getClusterId().get());
        assertEquals(0, parsed.getVersion());
    }

    @Test
    public void testRawPropertiesWithInvalidBrokerId() {
        Properties properties = new Properties();
        properties.put(RawMetaProperties.BROKER_ID_KEY, "oof");
        RawMetaProperties parsed = new RawMetaProperties(properties);
        assertThrows(RuntimeException.class, parsed::getBrokerId);
    }

    @Test
    public void testCreateMetadataProperties() {
        confirmValidForMetaProperties(clusterIdBase64);
    }

    @Test
    public void testMetaPropertiesWithMissingVersion() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setClusterId(clusterIdBase64);
        properties.setNodeId(1);
        assertThrows(RuntimeException.class, () -> MetaProperties.parse(properties));
    }

    @Test
    public void testMetaPropertiesAllowsHexEncodedUUIDs() {
        String clusterId = "7bc79ca1-9746-42a3-a35a-efb3cde44492";
        confirmValidForMetaProperties(clusterId);
    }

    @Test
    public void testMetaPropertiesWithNonUuidClusterId() {
        String clusterId = "not a valid uuid";
        confirmValidForMetaProperties(clusterId);
    }

    private void confirmValidForMetaProperties(String clusterId) {
        MetaProperties meta = new MetaProperties(clusterId, 5);
        MetaProperties meta2 = MetaProperties.parse(new RawMetaProperties(meta.toProperties()));
        assertEquals(meta.toProperties(), meta2.toProperties());
    }

    @Test
    public void testMetaPropertiesWithMissingBrokerId() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setVersion(1);
        properties.setClusterId(clusterIdBase64);
        assertThrows(RuntimeException.class, () -> MetaProperties.parse(properties));
    }

    @Test
    public void testMetaPropertiesWithMissingControllerId() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setVersion(1);
        properties.setClusterId(clusterIdBase64);
        assertThrows(RuntimeException.class, () -> MetaProperties.parse(properties));
    }

    @Test
    public void testMetaPropertiesWithVersionZero() {
        Properties props = new Properties();
        RawMetaProperties properties = new RawMetaProperties(props);
        properties.setVersion(0);
        properties.setClusterId(clusterIdBase64);
        properties.setBrokerId(5);
        MetaProperties metaProps = MetaProperties.parse(properties);
        assertEquals(clusterIdBase64, metaProps.getClusterId());
        assertEquals(5, metaProps.getNodeId());
    }

    @Test
    public void testValidMetaPropertiesWithMultipleVersionsInLogDirs() {
        // Let's create two directories with meta.properties one in v0 and v1.
        Properties properties1 = new Properties();
        RawMetaProperties props1 = new RawMetaProperties(properties1);
        props1.setVersion(0);
        props1.setClusterId(clusterIdBase64);
        props1.setBrokerId(5);

        Properties properties2 = new Properties();
        RawMetaProperties props2 = new RawMetaProperties(properties2);
        props2.setVersion(1);
        props2.setClusterId(clusterIdBase64);
        props2.setNodeId(5);

        for (boolean ignoreMissing : new boolean[]{true, false}) {
            Pair<RawMetaProperties, List<String>> result = getMetadataWithMultipleMetaPropLogDirs(
                Arrays.asList(props1, props2), ignoreMissing, true
            );

            RawMetaProperties metaProps = result.getKey();
            List<String> offlineDirs = result.getValue();

            Assertions.assertEquals(MetaProperties.parse(props2), MetaProperties.parse(metaProps));
            Assertions.assertEquals(new ArrayList<>(), offlineDirs);
        }
    }

    @Test
    public void testInvalidMetaPropertiesWithMultipleVersionsInLogDirs() {
        // Let's create two directories with meta.properties one in v0 and v1.
        Properties properties1 = new Properties();
        RawMetaProperties props1 = new RawMetaProperties(properties1);
        props1.setVersion(0);
        props1.setBrokerId(5);

        Properties properties2 = new Properties();
        RawMetaProperties props2 = new RawMetaProperties(properties2);
        props2.setVersion(1);
        props2.setClusterId(clusterIdBase64);
        props2.setNodeId(5);

        for (boolean ignoreMissing : new boolean[]{true, false}) {
            Assertions.assertThrows(RuntimeException.class, () -> {
                getMetadataWithMultipleMetaPropLogDirs(Arrays.asList(props1, props2), ignoreMissing, true);
            });
        }
    }

    private Pair<RawMetaProperties, List<String>> getMetadataWithMultipleMetaPropLogDirs(
        List<RawMetaProperties> metaProperties,
        boolean ignoreMissing,
        boolean kraftMode
    ) {
        List<File> logDirs = new ArrayList<>();
        try {
            for (RawMetaProperties mp : metaProperties) {
                File logDir = TestUtils.tempDirectory();
                logDirs.add(logDir);
                File propFile = new File(logDir.getAbsolutePath(), "meta.properties");
                FileOutputStream fs = new FileOutputStream(propFile);
                try {
                    mp.getProps().store(fs, "");
                    fs.flush();
                    fs.getFD().sync();
                } finally {
                    Utils.closeQuietly(fs, propFile.getName());
                }
            }
            List<String> logDirPaths = new ArrayList<>();
            for (File dir : logDirs) {
                logDirPaths.add(dir.getAbsolutePath());
            }
            return BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(logDirPaths, ignoreMissing, kraftMode);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            for (File logDir : logDirs) {
                try {
                    Utils.delete(logDir);
                } catch (IOException e) {
                    // swallow the error
                }
            }
        }
    }

    @Test
    public void testGetBrokerMetadataAndOfflineDirsWithNonexistentDirectories() throws IOException {
        // Use a regular file as an invalid log dir to trigger an IO error
        File invalidDir = TestUtils.tempFile("blah");
        try {
            // The `ignoreMissing` and `kraftMode` flag has no effect if there is an IO error
            testEmptyGetBrokerMetadataAndOfflineDirs(invalidDir,
                Collections.singletonList(invalidDir), true, true);
            testEmptyGetBrokerMetadataAndOfflineDirs(invalidDir,
                Collections.singletonList(invalidDir), false, true);
        } finally {
            Utils.delete(invalidDir);
        }
    }

    @Test
    public void testGetBrokerMetadataAndOfflineDirsIgnoreMissing() throws IOException {
        File tempDir = TestUtils.tempDirectory();
        try {
            testEmptyGetBrokerMetadataAndOfflineDirs(tempDir,
                new ArrayList<>(), true, true);
            testEmptyGetBrokerMetadataAndOfflineDirs(tempDir,
                new ArrayList<>(), true, false);

            Assertions.assertThrows(RuntimeException.class, () -> {
                BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
                    Collections.singletonList(tempDir.getAbsolutePath()), false, false);
            });

            Assertions.assertThrows(RuntimeException.class, () -> {
                BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
                    Collections.singletonList(tempDir.getAbsolutePath()), false, true);
            });
        } finally {
            Utils.delete(tempDir);
        }
    }

    private void testEmptyGetBrokerMetadataAndOfflineDirs(
        File logDir,
        List<File> expectedOfflineDirs,
        boolean ignoreMissing,
        boolean kraftMode
    ) {
        List<String> logDirPaths = new ArrayList<>();
        logDirPaths.add(logDir.getAbsolutePath());

        Pair<RawMetaProperties, List<String>> result = BrokerMetadataCheckpoint.getBrokerMetadataAndOfflineDirs(
            logDirPaths,
            ignoreMissing,
            false
        );

        RawMetaProperties metaProperties = result.getKey();
        List<String> offlineDirs = result.getValue();

        List<String> expectedOfflineDirPaths = new ArrayList<>();
        for (File dir : expectedOfflineDirs) {
            expectedOfflineDirPaths.add(dir.getAbsolutePath());
        }

        Assertions.assertEquals(expectedOfflineDirPaths, offlineDirs);
        Assertions.assertEquals(new Properties(), metaProperties.getProps());
    }
}

