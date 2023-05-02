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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LogDirsCommandTest {

    @Test
    public void shouldThrowWhenQueryingNonExistentBrokers() {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            assertThrows(RuntimeException.class, () -> execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "0,1,2", "--describe"), adminClient));
        }
    }

    @Test
    public void shouldNotThrowWhenDuplicatedBrokers() throws JsonProcessingException {
        Node broker = new Node(1, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "1,1", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertEquals(3, standardOutputLines.length);
            @SuppressWarnings("unchecked")
            Map<String, Object> information = new ObjectMapper().readValue(standardOutputLines[2], HashMap.class);
            @SuppressWarnings("unchecked")
            List<Object> brokerInformation = (List<Object>) information.get("brokers");
            @SuppressWarnings("unchecked")
            Integer brokerId = (Integer) ((HashMap<String, Object>) brokerInformation.get(0)).get("broker");
            assertEquals(1, brokerInformation.size());
            assertEquals(1, brokerId);
        }
    }

    @Test
    public void shouldQueryAllBrokersIfNonSpecified() throws JsonProcessingException {
        Node brokerOne = new Node(1, "hostname", 9092);
        Node brokerTwo = new Node(2, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Arrays.asList(brokerOne, brokerTwo), brokerOne)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertEquals(3, standardOutputLines.length);
            @SuppressWarnings("unchecked")
            Map<String, Object> information = new ObjectMapper().readValue(standardOutputLines[2], HashMap.class);
            @SuppressWarnings("unchecked")
            List<Object> brokerInformation = (List<Object>) information.get("brokers");
            @SuppressWarnings("unchecked")
            Integer brokerOneId = (Integer) ((HashMap<String, Object>) brokerInformation.get(0)).get("broker");
            @SuppressWarnings("unchecked")
            Integer brokerTwoId = (Integer) ((HashMap<String, Object>) brokerInformation.get(1)).get("broker");
            assertEquals(2, brokerInformation.size());
            assertEquals(1, brokerOneId);
            assertEquals(2, brokerTwoId);
        }
    }

    @Test
    public void shouldQuerySpecifiedBroker() throws JsonProcessingException {
        Node brokerOne = new Node(1, "hostname", 9092);
        Node brokerTwo = new Node(2, "hostname", 9092);
        try (MockAdminClient adminClient = new MockAdminClient(Arrays.asList(brokerOne, brokerTwo), brokerOne)) {
            String standardOutput = execute(fromArgsToOptions("--bootstrap-server", "EMPTY", "--broker-list", "1", "--describe"), adminClient);
            String[] standardOutputLines = standardOutput.split("\n");
            assertEquals(3, standardOutputLines.length);
            System.out.println(Arrays.toString(standardOutputLines));
            @SuppressWarnings("unchecked")
            Map<String, Object> information = new ObjectMapper().readValue(standardOutputLines[2], HashMap.class);
            @SuppressWarnings("unchecked")
            List<Object> brokerInformation = (List<Object>) information.get("brokers");
            @SuppressWarnings("unchecked")
            Integer brokerId = (Integer) ((HashMap<String, Object>) brokerInformation.get(0)).get("broker");
            assertEquals(1, brokerInformation.size());
            assertEquals(1, brokerId);
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
