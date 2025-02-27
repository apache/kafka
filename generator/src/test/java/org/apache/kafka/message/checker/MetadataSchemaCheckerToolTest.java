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

import static org.apache.kafka.message.checker.CheckerTestUtils.messageSpecStringToTempFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetadataSchemaCheckerToolTest {
    @Test
    public void testVerifyEvolutionGit() throws Exception {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            MetadataSchemaCheckerTool.run(
                // In the CI environment because the CI fetch command only creates HEAD and refs/remotes/pull/... references.
                // Since there may not be other branches like refs/heads/trunk in CI, HEAD serves as the baseline reference.
                new String[]{"verify-evolution-git", "--file", "AbortTransactionRecord.json", "--ref", "HEAD"},
                new PrintStream(stream)
            );
            assertEquals("Successfully verified evolution of file: AbortTransactionRecord.json",
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
                "--path1", path, "--path2", path}, new PrintStream(stream));
            assertEquals("Successfully verified evolution of path1: " + path + ", and path2: " + path,
                stream.toString().trim());
        }
    }
}
