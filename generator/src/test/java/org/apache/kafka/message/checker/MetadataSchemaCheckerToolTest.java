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

package org.apache.kafka.message.checker;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static org.apache.kafka.message.checker.CheckerTestUtils.messageSpecStringToTempFile;
import static org.apache.kafka.message.checker.CheckerTestUtils.messageSpecStringToRawTempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class MetadataSchemaCheckerToolTest {
    @Test
    public void testVerifyEvolutionGit() throws Exception {
        // Try to find the Git root directory
        Path rootKafkaDirectory = Paths.get("").toAbsolutePath();
        boolean gitFound = false;
        
        while (rootKafkaDirectory != null) {
            if (Files.exists(rootKafkaDirectory.resolve(".git"))) {
                gitFound = true;
                break;
            }
            rootKafkaDirectory = rootKafkaDirectory.getParent();
        }
        
        assumeTrue(gitFound, "Skipping test - not in a Git repository");
        
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            Path schemaPath = rootKafkaDirectory.resolve("metadata/src/main/resources/common/metadata/AbortTransactionRecord.json");
            MetadataSchemaCheckerTool.run(
                // In the CI environment because the CI fetch command only creates HEAD and refs/remotes/pull/... references.
                // Since there may not be other branches like refs/heads/trunk in CI, HEAD serves as the baseline reference.
                new String[]{"verify-evolution-git", "--path", schemaPath.toString(), "--ref", "HEAD"},
                new PrintStream(stream)
            );
            assertEquals("Successfully verified evolution of file: " + schemaPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testSuccessfulParse() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String path = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");
            MetadataSchemaCheckerTool.run(new String[] {"parse", "--path", path}, new PrintStream(stream));
            assertEquals("Successfully parsed file as MessageSpec: " + path, stream.toString().trim());
        }
    }

    @Test
    public void testSuccessfulVerifyEvolution() throws Exception {
        String parentPath = messageSpecStringToTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");
        String childPath = messageSpecStringToTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "{'name': 'ControllerId', 'type': 'int32', 'versions': '1+'}]}");
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testSuccessfulVerifyEvolutionWithCommonStruct() throws Exception {
        String parentPath = messageSpecStringToRawTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'Metadata', 'type': 'Metadata', 'versions': '0+'}]," +
                "'commonStructs': [{" +
                "'name': 'Metadata', 'versions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}]}");
        String childPath = messageSpecStringToRawTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'Metadata', 'type': 'Metadata', 'versions': '0+'}]," +
                "'commonStructs': [{" +
                "'name': 'Metadata', 'versions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "{'name': 'ControllerId', 'type': 'int32', 'versions': '1+'}]}]}");
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testVerifyEvolutionRejectsRemovedFieldWithoutHanging() throws Exception {
        String parentPath = messageSpecStringToTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0+', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "{'name': 'ControllerId', 'type': 'int32', 'versions': '0+'}]}");
        String childPath = messageSpecStringToTempFile(
            "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0+', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");

        assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            UnificationException exception = assertThrows(UnificationException.class, () ->
                MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                    "--path", childPath, "--parent_path", parentPath}, System.out));
            assertEquals("field1 ControllerId is present in message1, but should not be, based on its versions.",
                exception.getMessage());
        });
    }
}
