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

import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.properties.MetaProperties;
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;
import org.apache.kafka.metadata.properties.MetaPropertiesVersion;
import org.apache.kafka.metadata.properties.PropertiesUtils;
import org.apache.kafka.raft.QuorumConfig;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.config.KRaftConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.test.TestUtils.tempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@Timeout(value = 40)
public class StorageToolTest {
    private Properties newSelfManagedProperties() {
        Properties properties = new Properties();
        properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/foo,/tmp/bar");
        properties.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller");
        properties.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2");
        properties.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092");
        return properties;
    }

    @Test
    public void testConfigToLogDirectories() {
        LogConfig config = new LogConfig(newSelfManagedProperties());
        assertEquals(new ArrayList<>(Arrays.asList("/tmp/bar", "/tmp/foo")), StorageTool.configToLogDirectories(config));
    }

    @Test
    public void testConfigToLogDirectoriesWithMetaLogDir() {
        Properties properties = newSelfManagedProperties();
        properties.setProperty(KRaftConfigs.METADATA_LOG_DIR_CONFIG, "/tmp/baz");
        LogConfig config = new LogConfig(properties);
        assertEquals(new ArrayList<>(Arrays.asList("/tmp/bar", "/tmp/baz", "/tmp/foo")), StorageTool.configToLogDirectories(config));
    }

    @Test
    public void testInfoCommandOnEmptyDirectory() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
        assertEquals("Found log directory:\n  " + tempDir + "\n\nFound problem:\n  " + tempDir + " is not formatted.\n\n", stream.toString());
    }

    public static File tempDir() {
        return TestUtils.tempDirectory();
    }

    @Test
    public void testInfoCommandOnMissingDirectory() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        tempDir.delete();
        assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
        assertEquals("Found problem:\n  " + tempDir + " does not exist\n\n", stream.toString());
    }

    @Test
    public void testInfoCommandOnDirectoryAsFile() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempFile = tempFile();
        assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempFile.toString()))));
        assertEquals("Found problem:\n  " + tempFile + " is not a directory\n\n", stream.toString());
    }

    @Test
    public void testInfoWithMismatchedLegacyKafkaConfig() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        Files.write(tempDir.toPath().resolve("meta.properties"), String.join("\n", 
            Arrays.asList("version=1", "node.id=1", "cluster.id=XcZZOzUqS4yHOjhMQB6JLQ")).getBytes(StandardCharsets.UTF_8));
        assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), false, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
        assertEquals("Found log directory:\n  " + tempDir + "\n\nFound metadata: {cluster.id=XcZZOzUqS4yHOjhMQB6JLQ, node.id=1, version=1}\n\n" +
            "Found problem:\n  The kafka configuration file appears to be for a legacy cluster, " +
            "but the directories are formatted for a cluster in KRaft mode.\n\n", stream.toString());
    }

    @Test
    public void testInfoWithMismatchedSelfManagedKafkaConfig() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        Files.write(tempDir.toPath().resolve("meta.properties"), String.join("\n", 
            Arrays.asList("version=0", "broker.id=1", "cluster.id=26c36907-4158-4a35-919d-6534229f5241")).getBytes(StandardCharsets.UTF_8));
        assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
        assertEquals("Found log directory:\n  " + tempDir + "\n\nFound metadata: {broker.id=1, cluster.id=26c36907-4158-4a35-919d-6534229f5241, version=0}" +
            "\n\nFound problem:\n  The kafka configuration file appears to be for a cluster in KRaft mode, " +
            "but the directories are formatted for legacy mode.\n\n", stream.toString());
    }

    @Test
    public void testFormatEmptyDirectory() throws IOException, TerseException {
        File tempDir = tempDir();
        MetaProperties metaProperties = new MetaProperties.Builder()
            .setVersion(MetaPropertiesVersion.V1)
            .setClusterId("XcZZOzUqS4yHOjhMQB6JLQ")
            .setNodeId(2)
            .build();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        BootstrapMetadata bootstrapMetadata = StorageTool.buildBootstrapMetadata(
            MetadataVersion.latestTesting(), Optional.empty(), "test format command");

        assertEquals(0, StorageTool.formatCommand(new PrintStream(stream),
            new ArrayList<>(Collections.singletonList(tempDir.toString())),
            metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), false));
        assertTrue(stream.toString().startsWith("Formatting " + tempDir));

        try {
            assertEquals(1, StorageTool.formatCommand(new PrintStream(new ByteArrayOutputStream()),
                new ArrayList<>(Collections.singletonList(tempDir.toString())),
                metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), false));
        } catch (Exception e) {
            assertEquals("Log directory " + tempDir + " is already formatted. Use --ignore-formatted " +
                "to ignore this directory and format the others.", e.getMessage());
        }

        ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
        assertEquals(0, StorageTool.formatCommand(new PrintStream(stream2),
            new ArrayList<>(Collections.singletonList(tempDir.toString())),
            metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), true));
        assertEquals(String.format("All of the log directories are already formatted.%n"), stream2.toString());
    }

    private int runFormatCommand(ByteArrayOutputStream stream, List<String> directories, boolean ignoreFormatted) throws TerseException, IOException {
        MetaProperties metaProperties = new MetaProperties.Builder()
            .setVersion(MetaPropertiesVersion.V1)
            .setClusterId("XcZZOzUqS4yHOjhMQB6JLQ")
            .setNodeId(2)
            .build();
        BootstrapMetadata bootstrapMetadata = StorageTool.buildBootstrapMetadata(MetadataVersion.latestTesting(), null, "test format command");
        return StorageTool.formatCommand(new PrintStream(stream), directories, metaProperties, bootstrapMetadata, MetadataVersion.latestTesting(), ignoreFormatted);
    }

    @Test
    public void testFormatSucceedsIfAllDirectoriesAreAvailable() throws TerseException, IOException {
        String availableDir1 = TestUtils.tempDirectory().getAbsolutePath();
        String availableDir2 = TestUtils.tempDirectory().getAbsolutePath();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        assertEquals(0, runFormatCommand(stream, Arrays.asList(availableDir1, availableDir2), false));
        assertTrue(stream.toString().contains(String.format("Formatting %s", availableDir1)));
        assertTrue(stream.toString().contains(String.format("Formatting %s", availableDir2)));
    }

    @Test
    public void testFormatSucceedsIfAtLeastOneDirectoryIsAvailable() throws TerseException, IOException {
        String availableDir1 = TestUtils.tempDirectory().getAbsolutePath();
        String unavailableDir1 = TestUtils.tempFile().getAbsolutePath();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        assertEquals(0, runFormatCommand(stream, Arrays.asList(availableDir1, unavailableDir1), false));
        assertTrue(stream.toString().contains(String.format("I/O error trying to read log directory %s. Ignoring...", unavailableDir1)));
        assertTrue(stream.toString().contains(String.format("Formatting %s", availableDir1)));
        assertFalse(stream.toString().contains(String.format("Formatting %s", unavailableDir1)));
    }

    @Test
    public void testFormatFailsIfAllDirectoriesAreUnavailable() throws IOException {
        String unavailableDir1 = TestUtils.tempFile().getAbsolutePath();
        String unavailableDir2 = TestUtils.tempFile().getAbsolutePath();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        assertEquals("No available log directories to format.", assertThrows(TerseException.class,
            () -> runFormatCommand(stream, Arrays.asList(unavailableDir1, unavailableDir2), false)).getMessage());
        assertTrue(stream.toString().contains(String.format("I/O error trying to read log directory %s. Ignoring...", unavailableDir1)));
        assertTrue(stream.toString().contains(String.format("I/O error trying to read log directory %s. Ignoring...", unavailableDir2)));
    }

    @Test
    public void testFormatSucceedsIfAtLeastOneFormattedDirectoryIsAvailable() throws TerseException, IOException {
        String availableDir1 = TestUtils.tempDirectory().getAbsolutePath();
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        assertEquals(0, runFormatCommand(stream, Collections.singletonList(availableDir1), false));

        ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
        String unavailableDir1 = TestUtils.tempFile().getAbsolutePath();
        assertEquals(0, runFormatCommand(stream2, Arrays.asList(availableDir1, unavailableDir1), true));
    }

    @Test
    public void testDefaultMetadataVersion() {
        Namespace namespace = StorageTool.parseArguments(new String[]{"format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"});
        MetadataVersion mv = StorageTool.getMetadataVersion(namespace, Optional.empty());
        assertEquals(MetadataVersion.LATEST_PRODUCTION.featureLevel(), mv.featureLevel(), 
            "Expected the default metadata.version to be the latest production version");
    }

    @Test
    public void testConfiguredMetadataVersion() {
        Namespace namespace = StorageTool.parseArguments(new String[]{"format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"});
        MetadataVersion mv = StorageTool.getMetadataVersion(namespace, Optional.of(MetadataVersion.IBP_3_3_IV2.toString()));
        assertEquals(MetadataVersion.IBP_3_3_IV2.featureLevel(), mv.featureLevel(), "Expected the default metadata.version to be 3.3-IV2");
    }

    @Test
    public void testMetadataVersionFlags() {
        MetadataVersion mv = parseMetadataVersion("--release-version", "3.0");
        assertEquals("3.0", mv.shortVersion());

        mv = parseMetadataVersion("--release-version", "3.0-IV1");
        assertEquals(MetadataVersion.IBP_3_0_IV1, mv);

        assertThrows(IllegalArgumentException.class, () -> parseMetadataVersion("--release-version", "0.0"));
    }

    private MetadataVersion parseMetadataVersion(String... strings) {
        List<String> args = new ArrayList<>(Arrays.asList("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"));
        args.addAll(Arrays.asList(strings));
        Namespace namespace = StorageTool.parseArguments(args.toArray(new String[0]));
        return StorageTool.getMetadataVersion(namespace, null);
    }

    @Test
    public void testAddScram() {
        try {
            // Validate we can add multiple SCRAM creds.
            Optional<List<UserScramCredentialRecord>> scramRecords = parseAddScram();
            assertEquals(Optional.empty(), scramRecords);

            scramRecords = parseAddScram(
                "-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]",
                "-S", "SCRAM-SHA-256=[name=george,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]"
            );
            assertEquals(2, scramRecords.get().size());

            // Require name subfield.
            try {
                parseAddScram("-S", "SCRAM-SHA-256=[salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]");
            } catch (TerseException e) {
                assertEquals("You must supply 'name' to add-scram", e.getMessage());
            }

            // Require password xor saltedpassword
            try {
                parseAddScram("-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]");
            } catch (TerseException e) {
                assertEquals("You must only supply one of 'password' or 'saltedpassword' to add-scram", e.getMessage());
            }

            try {
                parseAddScram("-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",iterations=8192]");
            } catch (TerseException e) {
                assertEquals("You must supply one of 'password' or 'saltedpassword' to add-scram", e.getMessage());
            }

            // Validate salt is required with saltedpassword
            try {
                parseAddScram("-S", "SCRAM-SHA-256=[name=alice,saltedpassword=\"mT0yyUUxnlJaC99HXgRTSYlbuqa4FSGtJCJfTMvjYCE=\",iterations=8192]");
            } catch (TerseException e) {
                assertEquals("You must supply 'salt' with 'saltedpassword' to add-scram", e.getMessage());
            }

            // Validate salt is optional with password
            assertEquals(1, parseAddScram("-S", "SCRAM-SHA-256=[name=alice,password=alice,iterations=4096]").get().size());

            // Require 4096 <= iterations <= 16384
            try {
                parseAddScram("-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=16385]");
            } catch (TerseException e) {
                assertEquals("The 'iterations' value must be <= 16384 for add-scram", e.getMessage());
            }

            assertEquals(1, parseAddScram("-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=16384]").get().size());

            try {
                parseAddScram("-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=4095]");
            } catch (TerseException e) {
                assertEquals("The 'iterations' value must be >= 4096 for add-scram", e.getMessage());
            }

            assertEquals(1, parseAddScram("-S", "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=4096]").get().size());

            // Validate iterations is optional
            assertEquals(1, parseAddScram("-S", "SCRAM-SHA-256=[name=alice,password=alice]").get().size());
        } catch (StorageToolTestException | TerseException | NoSuchAlgorithmException | InvalidKeyException e) {
            System.out.println("Test Exception: " + e.getMessage());
        }
    }

    static class StorageToolTestException extends KafkaException {
        public StorageToolTestException(String message) {
            super(message);
        }
    }

    public Optional<List<UserScramCredentialRecord>> parseAddScram(String... strings) 
            throws TerseException, NoSuchAlgorithmException, InvalidKeyException {
        List<String> argsList = new ArrayList<>(Arrays.asList("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"));
        Collections.addAll(argsList, strings);

        String[] args = argsList.toArray(new String[0]);
        Namespace namespace = StorageTool.parseArguments(args);
        return StorageTool.getUserScramCredentialRecords(namespace);
    }

    @Test
    public void testScramWithBadMetadataVersion() throws IOException {
        AtomicReference<String> exitString = new AtomicReference<>("");
        Exit.Procedure exitProcedure = (code, message) -> {
            if (message == null) {
                message = "";
            }
            exitString.set(message);
            throw new StorageToolTestException(exitString.get());
        };
        Exit.setExitProcedure(exitProcedure);

        Properties properties = newSelfManagedProperties();
        File propsFile = tempFile();
        try (FileOutputStream propsStream = new FileOutputStream(propsFile)) {
            properties.store(propsStream, "config.props");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String[] args = {
            "format", "-c", propsFile.toString(), "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "--release-version", "3.4", "-S",
            "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=8192]"
        };

        try {
            StorageTool.main(args);
        } catch (StorageToolTestException e) {
            assertEquals("SCRAM is only supported in metadata.version " + MetadataVersion.IBP_3_5_IV2 + " or later.", exitString.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testNoScramWithMetadataVersion() throws IOException {
        AtomicReference<String> exitString = new AtomicReference<>("");
        AtomicInteger exitStatus = new AtomicInteger(1);
        Exit.Procedure exitProcedure = (code, message) -> {
            exitStatus.set(code);
            if (message == null) {
                message = "";
            }
            exitString.set(message);
            throw new StorageToolTestException(exitString.get());
        };
        Exit.setExitProcedure(exitProcedure);

        Properties properties = newSelfManagedProperties();
        File propsFile = tempFile();
        try (FileOutputStream propsStream = new FileOutputStream(propsFile)) {
            // This test does format the directory specified so use a tempdir
            properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, tempDir().toString());
            properties.store(propsStream, "config.props");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String[] args = {
            "format", "-c", propsFile.toString(), "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "--release-version", "3.4"
        };

        try {
            StorageTool.main(args);
        } catch (StorageToolTestException e) {
            assertEquals("", exitString.get());
            assertEquals(1, exitStatus.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testDirUuidGeneration() throws IOException, TerseException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        try {
            MetaProperties metaProperties = 
                new MetaProperties.Builder()
                    .setClusterId("XcZZOzUqS4yHOjhMQB6JLQ")
                    .setNodeId(2)
                    .build();
            BootstrapMetadata bootstrapMetadata = StorageTool
                .buildBootstrapMetadata(MetadataVersion.latestTesting(), Optional.empty(), "test format command");
            assertEquals(0, StorageTool.formatCommand(
                new PrintStream(stream),
                new ArrayList<>(Collections.singletonList(tempDir.toString())), 
                metaProperties, 
                bootstrapMetadata, 
                MetadataVersion.latestTesting(), false));
            File metaPropertiesFile = Paths.get(tempDir.toURI()).resolve(MetaPropertiesEnsemble.META_PROPERTIES_NAME).toFile();
            assertTrue(metaPropertiesFile.exists());
            MetaProperties metaProps = new MetaProperties.Builder(
                PropertiesUtils.readPropertiesFile(metaPropertiesFile.getAbsolutePath())).build();
            assertTrue(metaProps.directoryId().isPresent());
            assertFalse(DirectoryId.reserved(metaProps.directoryId().get()));
        } finally {
            Utils.delete(tempDir);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testFormattingUnstableMetadataVersionBlocked(boolean enableUnstable) throws IOException {
        AtomicReference<String> exitString = new AtomicReference<>("");
        AtomicInteger exitStatus = new AtomicInteger(1);

        Exit.Procedure exitProcedure = (code, message) -> {
            exitStatus.set(code);
            if (message == null) {
                message = "";
            }
            exitString.set(message);
            throw new StorageToolTestException(exitString.get());
        };
        Exit.setExitProcedure(exitProcedure);

        Properties properties = newSelfManagedProperties();
        File propsFile = tempFile();
        try (OutputStream propsStream = Files.newOutputStream(propsFile.toPath())) {
            properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, tempDir().toString());
            properties.setProperty(ServerConfigs.UNSTABLE_METADATA_VERSIONS_ENABLE_CONFIG, Boolean.toString(enableUnstable));
            properties.store(propsStream, "config.props");
        }
        
        String[] args = {
            "format", "-c", propsFile.toString(), "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "--release-version", MetadataVersion.latestTesting().toString()
        };
        
        try {
            StorageTool.main(args);
        } catch (StorageToolTestException e) {
        } finally {
            Exit.resetExitProcedure();
        }

        if (enableUnstable) {
            assertEquals("", exitString.get());
            assertEquals(1, exitStatus.get());
        } else {
            assertEquals("The metadata.version " + MetadataVersion.latestTesting() + " is not ready for production use yet.", exitString.get());
            assertEquals(1, exitStatus.get());
        }
    }
}
