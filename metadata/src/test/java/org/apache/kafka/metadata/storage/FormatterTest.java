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

package org.apache.kafka.metadata.storage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.bootstrap.BootstrapDirectory;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.Features;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.TestFeatureVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static org.apache.kafka.metadata.storage.ScramParserTest.TEST_SALT;
import static org.apache.kafka.metadata.storage.ScramParserTest.TEST_SALTED_PASSWORD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(value = 40)
public class FormatterTest {
    private static final Logger LOG = LoggerFactory.getLogger(FormatterTest.class);

    private static final int DEFAULT_NODE_ID = 1;

    private static final Uuid DEFAULT_CLUSTER_ID = Uuid.fromString("b3dGE68sQQKzfk80C_aLZw");

    static class TestEnv implements AutoCloseable {
        final List<String> directories;

        TestEnv(int numDirs) {
            this.directories = new ArrayList<>(numDirs);
            for (int i = 0; i < numDirs; i++) {
                this.directories.add(TestUtils.tempDirectory().getAbsolutePath());
            }
        }

        FormatterContext newFormatter() {
            Formatter formatter = new Formatter().
                setNodeId(DEFAULT_NODE_ID).
                setClusterId(DEFAULT_CLUSTER_ID.toString());
            directories.forEach(d -> formatter.addDirectory(d));
            return new FormatterContext(formatter);
        }

        String directory(int i) {
            return this.directories.get(i);
        }

        void deleteDirectory(int i) throws IOException {
            Utils.delete(new File(directories.get(i)));
        }

        @Override
        public void close() throws Exception {
            for (int i = 0; i < directories.size(); i++) {
                try {
                    deleteDirectory(i);
                } catch (Exception e) {
                    LOG.error("Error deleting directory " + directories.get(i), e);
                }
            }
        }
    }

    static class FormatterContext {
        final Formatter formatter;
        final ByteArrayOutputStream stream;

        FormatterContext(Formatter formatter) {
            this.formatter = formatter;
            this.stream = new ByteArrayOutputStream();
            this.formatter.setPrintStream(new PrintStream(stream));
        }

        String output() {
            return stream.toString();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    public void testDirectories(int numDirs) throws Exception {
        try (TestEnv testEnv = new TestEnv(numDirs)) {
            testEnv.newFormatter().formatter.run();
            MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
                addLogDirs(testEnv.directories).
                load();
            assertEquals(OptionalInt.of(DEFAULT_NODE_ID), ensemble.nodeId());
            assertEquals(Optional.of(DEFAULT_CLUSTER_ID.toString()), ensemble.clusterId());
            assertEquals(new HashSet<>(testEnv.directories), ensemble.logDirProps().keySet());
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0), Optional.empty()).read();
            assertEquals(MetadataVersion.latestProduction(), bootstrapMetadata.metadataVersion());
        }
    }

    @Test
    public void testFormatterFailsOnAlreadyFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            testEnv.newFormatter().formatter.run();
            assertEquals("Log directory " + testEnv.directory(0) + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.",
                    assertThrows(FormatterException.class,
                        () -> testEnv.newFormatter().formatter.run()).getMessage());
        }
    }

    @Test
    public void testFormatterFailsOnUnwritableDirectory() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            new File(testEnv.directory(0)).setReadOnly();
            FormatterContext formatter1 = testEnv.newFormatter();
            String expectedPrefix = "Error while writing meta.properties file";
            assertEquals(expectedPrefix,
                assertThrows(FormatterException.class,
                    () -> formatter1.formatter.run()).
                        getMessage().substring(0, expectedPrefix.length()));
        }
    }

    @Test
    public void testIgnoreFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.run();
            assertEquals("Formatting " + testEnv.directory(0) + " with metadata.version " +
                MetadataVersion.latestProduction() + ".",
                    formatter1.output().trim());

            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter.setIgnoreFormatted(true);
            formatter2.formatter.run();
            assertEquals("All of the log directories are already formatted.",
                formatter2.output().trim());
        }
    }

    @Test
    public void testOneDirectoryFormattedAndOthersNotFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            testEnv.newFormatter().formatter.setDirectories(Arrays.asList(testEnv.directory(0))).run();
            assertEquals("Log directory " + testEnv.directory(0) + " is already formatted. " +
                "Use --ignore-formatted to ignore this directory and format the others.",
                    assertThrows(FormatterException.class,
                        () -> testEnv.newFormatter().formatter.run()).getMessage());
        }
    }

    @Test
    public void testOneDirectoryFormattedAndOthersNotFormattedWithIgnoreFormatted() throws Exception {
        try (TestEnv testEnv = new TestEnv(2)) {
            testEnv.newFormatter().formatter.setDirectories(Arrays.asList(testEnv.directory(0))).run();

            FormatterContext formatter2 = testEnv.newFormatter();
            formatter2.formatter.setIgnoreFormatted(true);
            formatter2.formatter.run();
            assertEquals("Formatting " + testEnv.directory(1) + " with metadata.version " +
                MetadataVersion.latestProduction() + ".",
                    formatter2.output().trim());
        }
    }

    @Test
    public void testFormatWithOlderReleaseVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_5_IV0);
            formatter1.formatter.run();
            assertEquals("Formatting " + testEnv.directory(0) + " with metadata.version " +
                MetadataVersion.IBP_3_5_IV0 + ".", formatter1.output().trim());
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0), Optional.empty()).read();
            assertEquals(MetadataVersion.IBP_3_5_IV0, bootstrapMetadata.metadataVersion());
            assertEquals(1, bootstrapMetadata.records().size());
        }
    }

    @Test
    public void testFormatWithUnstableReleaseVersionFailsWithoutEnableUnstable() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.latestTesting());
            assertEquals("metadata.version " + MetadataVersion.latestTesting() + " is not yet stable.",
                assertThrows(FormatterException.class, () -> formatter1.formatter.run()).getMessage());
        }
    }

    @Test
    public void testFormatWithUnstableReleaseVersion() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.latestTesting());
            formatter1.formatter.setUnstableFeatureVersionsEnabled(true);
            formatter1.formatter.run();
            assertEquals("Formatting " + testEnv.directory(0) + " with metadata.version " +
                            MetadataVersion.latestTesting() + ".",
                    formatter1.output().trim());
            MetaPropertiesEnsemble ensemble = new MetaPropertiesEnsemble.Loader().
                    addLogDirs(testEnv.directories).
                    load();
            BootstrapMetadata bootstrapMetadata =
                    new BootstrapDirectory(testEnv.directory(0), Optional.empty()).read();
            assertEquals(MetadataVersion.latestTesting(), bootstrapMetadata.metadataVersion());
        }
    }

    @Test
    public void testFormatWithScramFailsOnUnsupportedReleaseVersions() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_4_IV0);
            formatter1.formatter.setScramArguments(Arrays.asList(
                "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]",
                "SCRAM-SHA-512=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]"));
            assertEquals("SCRAM is only supported in metadata.version 3.5-IV2 or later.",
                assertThrows(FormatterException.class,
                    () -> formatter1.formatter.run()).getMessage());
        }
    }

    @Test
    public void testFormatWithScram() throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setReleaseVersion(MetadataVersion.IBP_3_8_IV0);
            formatter1.formatter.setScramArguments(Arrays.asList(
                "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]",
                "SCRAM-SHA-512=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\"," +
                    "saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\"]"));
            formatter1.formatter.run();
            assertEquals("Formatting " + testEnv.directory(0) + " with metadata.version " +
                MetadataVersion.IBP_3_8_IV0 + ".", formatter1.output().trim());
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0), Optional.empty()).read();
            assertEquals(MetadataVersion.IBP_3_8_IV0, bootstrapMetadata.metadataVersion());
            List<ApiMessageAndVersion> scramRecords = bootstrapMetadata.records().stream().
                filter(r -> r.message() instanceof UserScramCredentialRecord).
                    collect(Collectors.toList());
            ScramFormatter scram256 = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
            ScramFormatter scram512 = new ScramFormatter(ScramMechanism.SCRAM_SHA_512);
            assertEquals(Arrays.asList(
                new ApiMessageAndVersion(new UserScramCredentialRecord().
                    setName("alice").
                    setMechanism(ScramMechanism.SCRAM_SHA_256.type()).
                    setSalt(TEST_SALT).
                    setStoredKey(scram256.storedKey(scram256.clientKey(TEST_SALTED_PASSWORD))).
                    setServerKey(scram256.serverKey(TEST_SALTED_PASSWORD)).
                    setIterations(4096), (short) 0),
                new ApiMessageAndVersion(new UserScramCredentialRecord().
                    setName("alice").
                    setMechanism(ScramMechanism.SCRAM_SHA_512.type()).
                    setSalt(TEST_SALT).
                    setStoredKey(scram512.storedKey(scram512.clientKey(TEST_SALTED_PASSWORD))).
                    setServerKey(scram512.serverKey(TEST_SALTED_PASSWORD)).
                    setIterations(4096), (short) 0)),
                scramRecords);
        }
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1})
    public void testFeatureFlag(short version) throws Exception {
        try (TestEnv testEnv = new TestEnv(1)) {
            FormatterContext formatter1 = testEnv.newFormatter();
            formatter1.formatter.setSupportedFeatures(Arrays.asList(Features.values()));
            formatter1.formatter.setFeatureLevel(TestFeatureVersion.FEATURE_NAME, version);
            formatter1.formatter.run();
            BootstrapMetadata bootstrapMetadata =
                new BootstrapDirectory(testEnv.directory(0), Optional.empty()).read();
            List<ApiMessageAndVersion> expected = new ArrayList<>();
            expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(MetadataVersion.latestProduction().featureLevel()),
                    (short) 0));
            if (version > 0) {
                expected.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                    setName(TestFeatureVersion.FEATURE_NAME).
                    setFeatureLevel(version), (short) 0));
            }
            assertEquals(expected, bootstrapMetadata.records());
        }
    }

//    @Test
//    def testFormatEmptyDirectoryWithStandaloneMode(): Unit = {
//        val tempDir = TestUtils.tempDir()
//        try {
//            val nodeId = 2
//            val metaProperties = new MetaProperties.Builder().
//                    setVersion(MetaPropertiesVersion.V1).
//                    setClusterId("XcZZOzUqS5yHOjhMQB2JLR").
//                    setNodeId(nodeId).
//                    build()
//            val stream = new ByteArrayOutputStream()
//            val bootstrapMetadata = StorageTool.buildBootstrapMetadata(MetadataVersion.latestTesting(), None,
//                    "test format command")
//            val host = "localhost"
//            val port = 9092
//            val newEndPoint = new EndPoint(host = host, port = port, listenerName = new ListenerName("PLAINTEXT"),
//                    SecurityProtocol.PLAINTEXT)
//
//            val listeners: util.Map[ListenerName, InetSocketAddress] = new util.HashMap()
//            listeners.put(newEndPoint.listenerName, new InetSocketAddress(host, port))
//
//            assertEquals(0, StorageTool.
//                    formatCommand(new PrintStream(stream), Seq(tempDir.toString), metaProperties, bootstrapMetadata,
//                            MetadataVersion.latestTesting(), ignoreFormatted = false, listeners = listeners))
//            val checkpointDir = tempDir + "/" + CLUSTER_METADATA_TOPIC_NAME
//            assertTrue(stringAfterFirstLine(stream.toString()).contains("Snapshot written to %s".format(checkpointDir)))
//            val checkpointFilePath = Snapshots.snapshotPath(Paths.get(checkpointDir), BOOTSTRAP_SNAPSHOT_ID)
//            assertTrue(checkpointFilePath.toFile.exists)
//            assertTrue(Utils.readFileAsString(checkpointFilePath.toFile.getPath).contains(host))
//        } finally Utils.delete(tempDir)
//    }
//
//    def stringAfterFirstLine(input: String): String = {
//        val firstNewline = input.indexOf("\n")
//        input.substring(firstNewline + 1)
//    }
//
//    @Test
//    def testFormatEmptyDirectoryWithControllerQuorumVoters(): Unit = {
//        val tempDir = TestUtils.tempDir()
//        val nodeId = 2
//        try {
//            val metaProperties = new MetaProperties.Builder().
//                    setVersion(MetaPropertiesVersion.V1).
//                    setClusterId("XcZZOzUqS5yHOjhMQB2JLS").
//                    setNodeId(nodeId).
//                    build()
//            val stream = new ByteArrayOutputStream()
//            val bootstrapMetadata = StorageTool.buildBootstrapMetadata(MetadataVersion.latestTesting(), None,
//                    "test format command")
//            val host = "localhost"
//            val port = 9092
//            val newEndPoint = new EndPoint(host = host, port = port, listenerName = new ListenerName("PLAINTEXT"),
//                    SecurityProtocol.PLAINTEXT)
//            val listeners: util.Map[ListenerName, InetSocketAddress] = new util.HashMap()
//            listeners.put(newEndPoint.listenerName, new InetSocketAddress(host, port))
//
//            assertEquals(0, StorageTool.
//                    formatCommand(new PrintStream(stream), Seq(tempDir.toString), metaProperties, bootstrapMetadata,
//                            MetadataVersion.latestTesting(), ignoreFormatted = false, listeners = listeners))
//            val checkpointDir = tempDir + "/" + CLUSTER_METADATA_TOPIC_NAME
//            assertTrue(stringAfterFirstLine(stream.toString()).contains("Snapshot written to %s".format(checkpointDir)))
//            val checkpointFilePath = Snapshots.snapshotPath(Paths.get(checkpointDir), BOOTSTRAP_SNAPSHOT_ID)
//            assertTrue(checkpointFilePath.toFile.exists)
//            assertTrue(Utils.readFileAsString(checkpointFilePath.toFile.getPath).contains(host))
//        } finally Utils.delete(tempDir)
//    }
//@Test
//def testDefaultMetadataVersion(): Unit = {
//        val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"))
//        val mv = StorageTool.getMetadataVersion(namespace, Map.empty, defaultVersionString = None)
//        assertEquals(MetadataVersion.LATEST_PRODUCTION.featureLevel(), mv.featureLevel(),
//                "Expected the default metadata.version to be the latest production version")
//    }
//
//    @Test
//    def testConfiguredMetadataVersion(): Unit = {
//        val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"))
//        val mv = StorageTool.getMetadataVersion(namespace, Map.empty, defaultVersionString = Some(MetadataVersion.IBP_3_3_IV2.toString))
//        assertEquals(MetadataVersion.IBP_3_3_IV2.featureLevel(), mv.featureLevel(),
//                "Expected the default metadata.version to be 3.3-IV2")
//    }

//    @Test
//    def testStandaloneModeWithArguments(): Unit = {
//        val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yPOjhMQB6JAY",
//                "-s"))
//        val properties: Properties = newSelfManagedProperties()
//        val logDir1 = "/tmp/other1"
//        val logDir2 = "/tmp/other2"
//        properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, logDir1 + "," + logDir2)
//        properties.setProperty("listeners", "PLAINTEXT://localhost:9092")
//        val config = new KafkaConfig(properties)
//        try {
//            val exitCode = StorageTool.runFormatCommand(namespace, config)
//            val tempDirs = config.logDirs
//            tempDirs.foreach(tempDir => {
//                    val checkpointDir = tempDir + "/" + CLUSTER_METADATA_TOPIC_NAME
//                    val checkpointFilePath = Snapshots.snapshotPath(Paths.get(checkpointDir), BOOTSTRAP_SNAPSHOT_ID)
//                    assertTrue(checkpointFilePath.toFile.exists)
//                    assertTrue(Utils.readFileAsString(checkpointFilePath.toFile.getPath).contains("localhost"))
//                    Utils.delete(new File(tempDir))
//            })
//            assertEquals(0, exitCode)
//        }
//        finally{
//            Utils.delete(new File(logDir1))
//            Utils.delete(new File(logDir2))
//        }
//    }
//
//    @Test
//    def testControllerQuorumVotersWithArguments(): Unit = {
//        val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS2yQQjhMQB7JAT",
//                "--controller-quorum-voters", "2@localhost:9092"))
//        val properties: Properties = newSelfManagedProperties()
//        val logDir1 = "/tmp/other1"
//        val logDir2 = "/tmp/other2"
//        properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, logDir1 + "," + logDir2)
//        properties.setProperty("listeners", "PLAINTEXT://localhost:9092")
//        val config = new KafkaConfig(properties)
//        try{
//            val exitCode = StorageTool.runFormatCommand(namespace, config)
//            val tempDirs = config.logDirs
//            tempDirs.foreach(tempDir => {
//                    val checkpointDir = tempDir + "/" + CLUSTER_METADATA_TOPIC_NAME
//                    val checkpointFilePath = Snapshots.snapshotPath(Paths.get(checkpointDir), BOOTSTRAP_SNAPSHOT_ID)
//                    assertTrue(checkpointFilePath.toFile.exists)
//                    assertTrue(Utils.readFileAsString(checkpointFilePath.toFile.getPath).contains("localhost"))
//                    Utils.delete(new File(tempDir))
//            })
//            assertEquals(0, exitCode)
//        }
//        finally {
//            Utils.delete(new File(logDir1))
//            Utils.delete(new File(logDir2))
//        }
//    }
//
//    @Test
//    def testWithStandaloneAndControllerQuorumVotersFailure(): Unit = {
//        val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yPOjhMQB6JAT",
//                "-s", "--controller-quorum-voters", "1@localhost:9092"))
//        val config = Mockito.spy(new KafkaConfig(TestUtils.createBrokerConfig(1, null)))
//        assertEquals("Both --standalone and --controller-quorum-voters were set. Only one of the two flags can be set.",
//                assertThrows(classOf[TerseFailure], () => StorageTool.runFormatCommand(namespace, config)).getMessage)
//    }
//
//    @Test
//    def testSettingFeatureAndReleaseVersionFails(): Unit = {
//        val namespace = StorageTool.parseArguments(Array("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ",
//                "--release-version", "3.0-IV1", "--feature", "metadata.version=4"))
//        assertThrows(classOf[IllegalArgumentException], () => StorageTool.getMetadataVersion(namespace, parseFeatures(namespace), defaultVersionString = None))
//    }


}
