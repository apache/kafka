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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.PropertiesUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataQuorumCommandUnitTest {
    @Test
    public void testRemoveControllerDryRun() {
        List<String> outputs = List.of(
            ToolsTestUtils.captureStandardOut(() ->
                assertEquals(0, MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092",
                    "remove-controller",
                    "--controller-id", "2",
                    "--controller-directory-id", "_KWDkTahTVaiVVVTaugNew",
                    "--dry-run"))).split("\n"));
        assertTrue(outputs.contains("DRY RUN of removing  KRaft controller 2 with directory id _KWDkTahTVaiVVVTaugNew"),
            "Failed to find expected output in stdout: " + outputs);
    }

    @Test
    public void testGetControllerIdWithoutId() {
        Properties props = new Properties();
        props.setProperty("process.roles", "controller");
        assertEquals("node.id not found in configuration file. Is this a valid controller configuration file?",
            assertThrows(TerseException.class,
                () -> assertEquals(4, MetadataQuorumCommand.getControllerId(props))).
                    getMessage());
    }

    @Test
    public void testGetControllerId() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "4");
        props.setProperty("process.roles", "controller");
        assertEquals(4, MetadataQuorumCommand.getControllerId(props));
    }

    @Test
    public void testGetNegativeControllerId() {
        Properties props = new Properties();
        props.setProperty("node.id", "-4");
        props.setProperty("process.roles", "controller");
        assertEquals("node.id was negative in configuration file. Is this a valid controller configuration file?",
            assertThrows(TerseException.class,
                () -> assertEquals(4, MetadataQuorumCommand.getControllerId(props))).
                    getMessage());
    }

    @Test
    public void testGetControllerIdWithoutProcessRoles() {
        Properties props = new Properties();
        props.setProperty("node.id", "4");
        assertEquals("process.roles did not contain 'controller' in configuration file. Is this a valid controller configuration file?",
            assertThrows(TerseException.class,
                () -> assertEquals(4, MetadataQuorumCommand.getControllerId(props))).
                    getMessage());
    }

    @Test
    public void testGetControllerIdWithBrokerProcessRoles() {
        Properties props = new Properties();
        props.setProperty("node.id", "4");
        props.setProperty("process.roles", "broker");
        assertEquals("process.roles did not contain 'controller' in configuration file. Is this a valid controller configuration file?",
            assertThrows(TerseException.class,
                () -> assertEquals(4, MetadataQuorumCommand.getControllerId(props))).
                    getMessage());
    }

    @Test
    public void testGetMetadataDirectory() throws Exception {
        Properties props = new Properties();
        props.setProperty("metadata.log.dir", "/tmp/meta");
        props.setProperty("log.dirs", "/tmp/log,/tmp/log2");
        assertEquals("/tmp/meta", MetadataQuorumCommand.getMetadataDirectory(props));
    }

    @Test
    public void testGetMetadataDirectoryFromLogDirs() throws Exception {
        Properties props = new Properties();
        props.setProperty("log.dirs", "/tmp/log");
        assertEquals("/tmp/log", MetadataQuorumCommand.getMetadataDirectory(props));
    }

    @Test
    public void testGetMetadataDirectoryFromMultipleLogDirs() throws Exception {
        Properties props = new Properties();
        props.setProperty("log.dirs", "/tmp/log,/tmp/log2");
        assertEquals("/tmp/log", MetadataQuorumCommand.getMetadataDirectory(props));
    }

    @Test
    public void testGetMetadataDirectoryFailure() {
        Properties props = new Properties();
        assertEquals("Neither metadata.log.dir nor log.dirs were found. Is this a valid controller configuration file?",
            assertThrows(TerseException.class,
                () -> MetadataQuorumCommand.getMetadataDirectory(props)).
                    getMessage());
    }

    static class MetadataQuorumCommandUnitTestEnv implements AutoCloseable {
        File metadataDir;

        MetadataQuorumCommandUnitTestEnv(Optional<Uuid> directoryId) throws Exception {
            this.metadataDir = TestUtils.tempDirectory();
            new MetaPropertiesEnsemble.Copier(MetaPropertiesEnsemble.EMPTY).
                setMetaLogDir(Optional.of(metadataDir.getAbsolutePath())).
                setLogDirProps(metadataDir.getAbsolutePath(),
                    new MetaProperties.Builder().
                        setClusterId("Ig-WB32JRqqzct3VafTr0w").
                        setNodeId(2).
                        setDirectoryId(directoryId).
                            build()).
                    writeLogDirChanges();
        }

        File writePropertiesFile() throws IOException {
            Properties props = new Properties();
            props.setProperty("metadata.log.dir", metadataDir.getAbsolutePath());
            props.setProperty("log.dirs", metadataDir.getAbsolutePath());
            props.setProperty("process.roles", "controller,broker");
            props.setProperty("node.id", "5");
            props.setProperty("controller.listener.names", "CONTROLLER,CONTROLLER_SSL");
            props.setProperty("listeners", "CONTROLLER://:9093,CONTROLLER_SSL://:9094");
            props.setProperty("advertised.listeners", "CONTROLLER://example.com:9093,CONTROLLER_SSL://example.com:9094");
            File file = new File(metadataDir, "controller.properties");
            PropertiesUtils.writePropertiesFile(props, file.getAbsolutePath(), false);
            return file;
        }

        @Override
        public void close() throws Exception {
            Utils.delete(metadataDir);
        }
    }

    @Test
    public void testGetMetadataDirectoryId() throws Exception {
        try (MetadataQuorumCommandUnitTestEnv testEnv =
                new MetadataQuorumCommandUnitTestEnv(Optional.
                    of(Uuid.fromString("wZoXPqWoSu6F6c8MkmdyAg")))) {
            assertEquals(Uuid.fromString("wZoXPqWoSu6F6c8MkmdyAg"),
                MetadataQuorumCommand.getMetadataDirectoryId(testEnv.metadataDir.getAbsolutePath()));
        }
    }

    @Test
    public void testGetMetadataDirectoryIdWhenThereIsNoId() throws Exception {
        try (MetadataQuorumCommandUnitTestEnv testEnv =
                 new MetadataQuorumCommandUnitTestEnv(Optional.empty())) {
            assertEquals("No directory id found in " + testEnv.metadataDir.getAbsolutePath(),
                assertThrows(TerseException.class,
                    () -> MetadataQuorumCommand.getMetadataDirectoryId(testEnv.metadataDir.getAbsolutePath())).
                        getMessage());
        }
    }

    @Test
    public void testGetMetadataDirectoryIdWhenThereIsNoDirectory() throws Exception {
        try (MetadataQuorumCommandUnitTestEnv testEnv =
                     new MetadataQuorumCommandUnitTestEnv(Optional.empty())) {
            testEnv.close();
            assertEquals("Unable to read meta.properties from " + testEnv.metadataDir.getAbsolutePath(),
                    assertThrows(TerseException.class,
                        () -> MetadataQuorumCommand.getMetadataDirectoryId(testEnv.metadataDir.getAbsolutePath())).
                            getMessage());
        }
    }

    @Test
    public void testGetControllerAdvertisedListenersWithNoControllerListenerNames() {
        Properties props = new Properties();
        assertEquals("controller.listener.names was not found. Is this a valid controller configuration file?",
            assertThrows(TerseException.class,
                () -> MetadataQuorumCommand.getControllerAdvertisedListeners(props)).
                    getMessage());
    }

    @Test
    public void testGetControllerAdvertisedListenersWithNoControllerListenerInformation() {
        Properties props = new Properties();
        props.setProperty("controller.listener.names", "CONTROLLER,CONTROLLER2");
        assertEquals("Cannot find information about controller listener name: CONTROLLER",
            assertThrows(TerseException.class,
                () -> MetadataQuorumCommand.getControllerAdvertisedListeners(props)).
                    getMessage());
    }

    @Test
    public void testGetControllerAdvertisedListenersWithRegularListeners() throws Exception {
        Properties props = new Properties();
        props.setProperty("controller.listener.names", "CONTROLLER,CONTROLLER2");
        props.setProperty("listeners", "CONTROLLER://example.com:9092,CONTROLLER2://:9093");
        assertEquals(Set.of(
            new RaftVoterEndpoint("CONTROLLER", "example.com", 9092),
            new RaftVoterEndpoint("CONTROLLER2", "localhost", 9093)),
                MetadataQuorumCommand.getControllerAdvertisedListeners(props));
    }

    @Test
    public void testGetControllerAdvertisedListenersWithRegularListenersAndAdvertisedListeners() throws Exception {
        Properties props = new Properties();
        props.setProperty("controller.listener.names", "CONTROLLER,CONTROLLER2");
        props.setProperty("listeners", "CONTROLLER://:9092,CONTROLLER2://:9093");
        props.setProperty("advertised.listeners", "CONTROLLER://example.com:9092,CONTROLLER2://example.com:9093");
        assertEquals(Set.of(
            new RaftVoterEndpoint("CONTROLLER", "example.com", 9092),
            new RaftVoterEndpoint("CONTROLLER2", "example.com", 9093)),
                MetadataQuorumCommand.getControllerAdvertisedListeners(props));
    }

    @Test
    public void testAddControllerDryRun() throws Exception {
        try (MetadataQuorumCommandUnitTestEnv testEnv =
                 new MetadataQuorumCommandUnitTestEnv(Optional.
                     of(Uuid.fromString("wZoXPqWoSu6F6c8MkmdyAg")))) {
            File propsFile = testEnv.writePropertiesFile();
            List<String> outputs = List.of(
                ToolsTestUtils.captureStandardOut(() ->
                    assertEquals(0, MetadataQuorumCommand.mainNoExit("--bootstrap-server", "localhost:9092",
                        "--command-config", propsFile.getAbsolutePath(),
                        "add-controller",
                        "--dry-run"))).split("\n"));
            assertTrue(outputs.contains("DRY RUN of adding controller 5 with directory id " +
                "wZoXPqWoSu6F6c8MkmdyAg and endpoints: CONTROLLER://example.com:9093, CONTROLLER_SSL://example.com:9094"),
                    "Failed to find expected output in stdout: " + outputs);
        }
    }
}
