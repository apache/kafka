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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.kafka.message.checker.CheckerTestUtils.messageSpecStringToTempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

public class MetadataSchemaCheckerToolTest {
    @Test
    public void testVerifyEvolutionGit() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            Path rootKafkaDirectory = Paths.get("").toAbsolutePath();
            while (!Files.exists(rootKafkaDirectory.resolve(".git"))) {
                rootKafkaDirectory = rootKafkaDirectory.getParent();
                if (rootKafkaDirectory == null) {
                    throw new RuntimeException("Invalid directory, need to be within a Git repository");
                }
            }
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
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String path = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");
            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", path, "--parent_path", path}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path: " + path + " from parent: " + path,
                stream.toString().trim());
        }
    }

    @Test
    public void testVerifyEvolutionWithDifferentSchemas() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String parentPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");

            String childPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");

            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));

            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testVerifyEvolutionWithMultipleMatchingFields() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String parentPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'BrokerHost', 'type': 'string', 'versions': '0+'}," +
                "  {'name': 'Port', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Rack', 'type': 'string', 'versions': '0+'}" +
                "]}");

            String childPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'BrokerHost', 'type': 'string', 'versions': '0+'}," +
                "  {'name': 'Port', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Rack', 'type': 'string', 'versions': '0+'}" +
                "]}");

            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));

            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testVerifyEvolutionWithNestedStructs() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String parentPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Feature', 'type': 'FeatureInfo', 'versions': '0+', " +
                "   'fields': [" +
                "     {'name': 'FeatureName', 'type': 'string', 'versions': '0+'}," +
                "     {'name': 'FeatureVersion', 'type': 'int16', 'versions': '0+'}" +
                "   ]}" +
                "]}");

            String childPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Feature', 'type': 'FeatureInfo', 'versions': '0+', " +
                "   'fields': [" +
                "     {'name': 'FeatureName', 'type': 'string', 'versions': '0+'}," +
                "     {'name': 'FeatureVersion', 'type': 'int16', 'versions': '0+'}" +
                "   ]}" +
                "]}");

            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));

            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testStandardizedCommandPathArguments() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String path = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [{'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}]}");

            // Test with --path format (new standardized format)
            MetadataSchemaCheckerTool.run(new String[] {"parse", "--path", path}, new PrintStream(stream));
            assertEquals("Successfully parsed file as MessageSpec: " + path, stream.toString().trim());
            stream.reset();

            // Test with -p format (legacy format)
            MetadataSchemaCheckerTool.run(new String[] {"parse", "-p", path}, new PrintStream(stream));
            assertEquals("Successfully parsed file as MessageSpec: " + path, stream.toString().trim());
            stream.reset();

            // Test verify-evolution with --path and --parent_path format
            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution", "--path", path,
                "--parent_path", path}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path: " + path + " from parent: " + path,
                stream.toString().trim());
            stream.reset();

            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", path, "--parent_path", path}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path: " + path + " from parent: " + path,
                stream.toString().trim());
        }
    }

    @Test
    public void testVerifyEvolutionWithArrayAndComplexTypes() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String parentPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Features', 'type': '[]FeatureInfo', 'versions': '0+', " +
                "   'fields': [" +
                "     {'name': 'FeatureName', 'type': 'string', 'versions': '0+'}," +
                "     {'name': 'FeatureVersion', 'type': 'int16', 'versions': '0+'}" +
                "   ]}" +
                "]}");

            String childPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Features', 'type': '[]FeatureInfo', 'versions': '0+', " +
                "   'fields': [" +
                "     {'name': 'FeatureName', 'type': 'string', 'versions': '0+'}," +
                "     {'name': 'FeatureVersion', 'type': 'int16', 'versions': '0+'}" +
                "   ]}" +
                "]}");

            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));

            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }

    @Test
    public void testVerifyEvolutionWithDifferentFieldVersions() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            String parentPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-1', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'BrokerHost', 'type': 'string', 'versions': '0+'}," +
                "  {'name': 'Port', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Rack', 'type': 'string', 'versions': '0-1'}" + // Only in versions 0-1
                "]}");

            String childPath = messageSpecStringToTempFile(
                "{'apiKey':62, 'type': 'request', 'name': 'BrokerRegistrationRequest', " +
                "'validVersions': '0-2', 'flexibleVersions': '0+', " +
                "'fields': [" +
                "  {'name': 'BrokerId', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'BrokerHost', 'type': 'string', 'versions': '0+'}," +
                "  {'name': 'Port', 'type': 'int32', 'versions': '0+'}," +
                "  {'name': 'Rack', 'type': 'string', 'versions': '0-1'}," + // Still only in versions 0-1
                "  {'name': 'SecurityProtocol', 'type': 'int8', 'versions': '2+'}" + // New field in version 2+
                "]}");

            MetadataSchemaCheckerTool.run(new String[] {"verify-evolution",
                "--path", childPath, "--parent_path", parentPath}, new PrintStream(stream));

            assertEquals("Successfully verified evolution of path: " + childPath + " from parent: " + parentPath,
                stream.toString().trim());
        }
    }
}
