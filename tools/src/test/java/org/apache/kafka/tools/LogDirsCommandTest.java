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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LogDirsCommandTest {

    @Test
    public void shouldThrowWhenQueryingNonExistentBrokers() {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            assertThrows(RuntimeException.class, () -> execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "0,1,2", "--describe"), adminClient));
        }
    }

    @Test
    public void shouldThrowWhenDuplicatedBrokers() {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            assertThrows(RuntimeException.class, () -> execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "0,0,1,2,2", "--describe"), adminClient));
        }
    }

    @Test
    public void shouldQueryAllBrokersIfNonSpecified() {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertTrue(standardOutputLines.length != 0);
            assertTrue(standardOutputLines[0].contains("Querying brokers for log directories information"));
        }
    }

    @Test
    public void shouldQuerySpecifiedBroker() {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "1", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertTrue(standardOutputLines.length != 0);
            assertTrue(standardOutputLines[0].contains("Querying brokers for log directories information"));
        }
    }

    private LogDirsCommand.LogDirsCommandOptions fromArgsToOptions(String... args) {
        return new LogDirsCommand.LogDirsCommandOptions(args);
    }

    private String execute(LogDirsCommand.LogDirsCommandOptions options, Admin adminClient) {
        Runnable runnable = () -> {
            try {
                LogDirsCommand.execute(options, adminClient);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return ToolsTestUtils.captureStandardOut(runnable);
    }
}
