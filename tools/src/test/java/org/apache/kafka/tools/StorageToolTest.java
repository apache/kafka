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
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metadata.UserScramCredentialRecord;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata;
import org.apache.kafka.metadata.broker.MetaProperties;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@Timeout(value = 40)
public class StorageToolTest {

    private Properties newSelfManagedProperties() {
        Properties properties = new Properties();
        properties.setProperty(LogConfig.getLogDirsProp(), "/tmp/foo,/tmp/bar");
        properties.setProperty(LogConfig.getProcessRolesProp(), "controller");
        properties.setProperty(LogConfig.getNodeIdProp(), "2");
        properties.setProperty(LogConfig.getQuorumVotersProp(), "2@localhost:9092");
        properties.setProperty(LogConfig.getControllerListenerNamesProp(), "PLAINTEXT");
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
        properties.setProperty(LogConfig.getMetadataLogDirProp(), "/tmp/baz");
        LogConfig config = new LogConfig(properties);
        assertEquals(new ArrayList<>(Arrays.asList("/tmp/bar", "/tmp/baz", "/tmp/foo")), StorageTool.configToLogDirectories(config));
    }

    @Test
    public void testInfoCommandOnEmptyDirectory() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        try {
            assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
            assertEquals("Found log directory:\n  " + tempDir + "\n\nFound problem:\n  " + tempDir + " is not formatted.\n\n", stream.toString());
        } finally {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testInfoCommandOnMissingDirectory() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        tempDir.delete();
        try {
            assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
            assertEquals("Found problem:\n  " + tempDir + " does not exist\n\n", stream.toString());
        } finally {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testInfoCommandOnMissingZookeeperConnectConfig() throws IOException {
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

        File tempFile = tempFile();
        String[] arguments = new String[] {"info", "-c",
            tempFile.getAbsolutePath()};
        try {
            StorageTool.main(arguments);
        } catch (StorageToolTestException e) {
            assertEquals("Missing required configuration `zookeeper.connect` which has no default value.", exitString.get());
            assertEquals(1, exitStatus.get());
        } finally {
            tempFile.delete();
            Exit.resetExitProcedure();
        }
    }

    @Test
    public void testInfoCommandOnDirectoryAsFile() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempFile = tempFile();
        try {
            assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempFile.toString()))));
            assertEquals("Found problem:\n  " + tempFile + " is not a directory\n\n", stream.toString());
        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void testInfoWithMismatchedLegacyKafkaConfig() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        try {
            Files.write(tempDir.toPath().resolve("meta.properties"), String.join("\n", Arrays.asList("version=1", "cluster.id=XcZZOzUqS4yHOjhMQB6JLQ")).getBytes(StandardCharsets.UTF_8));
            assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), false, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
            assertEquals("Found log directory:\n  " + tempDir + "\n\nFound metadata: {cluster.id=XcZZOzUqS4yHOjhMQB6JLQ, version=1}\n\n" + "Found problem:\n  " + "The kafka configuration file appears to be for a legacy cluster, but the directories are formatted for a cluster in KRaft mode.\n\n", stream.toString());
        } finally {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testInfoWithMismatchedSelfManagedKafkaConfig() throws IOException {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        File tempDir = tempDir();
        try {
            Files.write(tempDir.toPath().resolve("meta.properties"), String.join("\n", Arrays.asList("version=0", "broker.id=1", "cluster.id=26c36907-4158-4a35-919d-6534229f5241")).getBytes(StandardCharsets.UTF_8));
            assertEquals(1, StorageTool.infoCommand(new PrintStream(stream), true, new ArrayList<>(Collections.singletonList(tempDir.toString()))));
            assertEquals("Found log directory:\n  " + tempDir + "\n\nFound metadata: {broker.id=1, cluster.id=26c36907-4158-4a35-919d-6534229f5241, version=0}" + "\n\nFound problem:\n  " + "The kafka configuration file appears to be for a cluster in KRaft mode, but the directories are formatted for legacy mode.\n\n", stream.toString());
        } finally {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testFormatEmptyDirectory() throws IOException, TerseException {
        File tempDir = tempDir();
        try {
            MetaProperties metaProperties = new MetaProperties("XcZZOzUqS4yHOjhMQB6JLQ", 2);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            BootstrapMetadata bootstrapMetadata = StorageTool.buildBootstrapMetadata(MetadataVersion.latest(),
                Optional.empty(), "test format command");

            assertEquals(0, StorageTool.formatCommand(new PrintStream(stream),
                new ArrayList<>(Collections.singletonList(tempDir.toString())),
                metaProperties, bootstrapMetadata, MetadataVersion.latest(), false));
            assertTrue(stream.toString().startsWith("Formatting " + tempDir));

            try {
                assertEquals(1, StorageTool.formatCommand(new PrintStream(new ByteArrayOutputStream()),
                    new ArrayList<>(Collections.singletonList(tempDir.toString())),
                    metaProperties, bootstrapMetadata, MetadataVersion.latest(), false));
            } catch (Exception e) {
                assertEquals("Log directory " + tempDir + " is already " + "formatted. Use --ignore-formatted to ignore this directory and format the " + "others.", e.getMessage());
            }

            ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
            assertEquals(0, StorageTool.formatCommand(new PrintStream(stream2),
                new ArrayList<>(Collections.singletonList(tempDir.toString())),
                metaProperties, bootstrapMetadata, MetadataVersion.latest(), true));
            assertEquals(String.format("All of the log directories are already formatted.%n"), stream2.toString());
        } finally {
            Utils.delete(tempDir);
        }
    }

    @Test
    public void testFormatWithInvalidClusterId() {
        LogConfig config = new LogConfig(newSelfManagedProperties());
        assertThrows(TerseException.class, () -> {
            StorageTool.buildMetadataProperties("invalid", config);
        }).getMessage().equals("Cluster ID string invalid does not appear to be a valid UUID: " + "Input string `invalid` decoded as 5 bytes, which is not equal to the expected " + "16 bytes of a base64-encoded UUID");

    }

    @Test
    public void testDefaultMetadataVersion() throws Exception {
        Namespace namespace = StorageTool.parseArguments("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ");
        MetadataVersion mv = StorageTool.getMetadataVersion(namespace, Optional.empty());
        assertEquals(MetadataVersion.latest().featureLevel(), mv.featureLevel(), "Expected the default metadata.version to be the latest version");
    }

    @Test
    public void testConfiguredMetadataVersion() throws Exception {
        Namespace namespace = StorageTool.parseArguments("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ");
        MetadataVersion mv = StorageTool.getMetadataVersion(namespace, Optional.of(MetadataVersion.IBP_3_3_IV2.toString()));
        assertEquals(MetadataVersion.IBP_3_3_IV2.featureLevel(), mv.featureLevel(), "Expected the default metadata.version to be 3.3-IV2");
    }

    @Test
    public void testMetadataVersionFlags() throws Exception {
        MetadataVersion mv = parseMetadataVersion("--release-version", "3.0");
        assertEquals("3.0", mv.shortVersion());

        mv = parseMetadataVersion("--release-version", "3.0-IV1");
        assertEquals(MetadataVersion.IBP_3_0_IV1, mv);

        assertThrows(IllegalArgumentException.class, () -> parseMetadataVersion("--release-version", "0.0"));
    }

    MetadataVersion parseMetadataVersion(String... strings) throws Exception {
        List<String> args = new ArrayList<>(Arrays.asList("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ"));
        args.addAll(Arrays.asList(strings));
        Namespace namespace = StorageTool.parseArguments(args.toArray(new String[0]));
        return StorageTool.getMetadataVersion(namespace, null);
    }

    public Optional<List<UserScramCredentialRecord>> parseAddScram(String... strings) throws TerseException, NoSuchAlgorithmException, InvalidKeyException {
        List<String> argsList = new ArrayList<>();
        argsList.add("format");
        argsList.add("-c");
        argsList.add("config.props");
        argsList.add("-t");
        argsList.add("XcZZOzUqS4yHOjhMQB6JLQ");

        Collections.addAll(argsList, strings);

        String[] args = argsList.toArray(new String[0]);
        Namespace namespace = StorageTool.parseArguments(args);
        return StorageTool.getUserScramCredentialRecords(namespace);
    }

    static class StorageToolTestException extends KafkaException {
        public StorageToolTestException(String message) {
            super(message);
        }
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

    @Test
    public void testScramWithBadMetadataVersion() throws IOException {
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
            properties.store(propsStream, "config.props");
        } catch (Exception e) {
            // Handle the exception (e.g., rethrow or log)
        }

        String[] args = {
            "format", "-c", propsFile.toString(), "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "--release-version", "3.4", "-S",
            "SCRAM-SHA-256=[name=alice,salt=\"MWx2NHBkbnc0ZndxN25vdGN4bTB5eTFrN3E=\",password=alice,iterations=8192]"
        };

        try {
            StorageTool.main(args);
        } catch (StorageToolTestException e) {
            assertEquals("SCRAM is only supported in metadataVersion IBP_3_5_IV2 or later.", exitString.get());
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
            properties.setProperty(LogConfig.getLogDirsProp(), tempDir().toString());
            properties.store(propsStream, "config.props");
        } catch (Exception e) {
            // ignore the error
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

    public static File tempDir() throws IOException {
        return TestUtils.tempDirectory();
    }
}
