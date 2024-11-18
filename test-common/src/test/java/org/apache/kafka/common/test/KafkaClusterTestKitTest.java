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

package org.apache.kafka.common.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaClusterTestKitTest {
    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void testCreateClusterWithBadNumDisksThrows(int disks) {
        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumDisksPerBroker(disks)
                        .setNumControllerNodes(1)
                        .build())
        );
        assertEquals("Invalid value for numDisksPerBroker", e.getMessage());
    }

    @Test
    public void testCreateClusterWithBadNumOfControllers() {
        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(1)
                .setNumControllerNodes(-1)
                .build())
        );
        assertEquals("Invalid negative value for numControllerNodes", e.getMessage());
    }

    @Test
    public void testCreateClusterWithBadNumOfBrokers() {
        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder()
                .setNumBrokerNodes(-1)
                .setNumControllerNodes(1)
                .build())
        );
        assertEquals("Invalid negative value for numBrokerNodes", e.getMessage());
    }

    @Test
    public void testCreateClusterWithBadPerServerProperties() {
        Map<Integer, Map<String, String>> perServerProperties = new HashMap<>();
        perServerProperties.put(100, Collections.singletonMap("foo", "foo1"));
        perServerProperties.put(200, Collections.singletonMap("bar", "bar1"));

        IllegalArgumentException e = assertThrowsExactly(IllegalArgumentException.class, () -> new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .setPerServerProperties(perServerProperties)
                        .build())
        );
        assertEquals("Unknown server id 100, 200 in perServerProperties, the existent server ids are 0, 3000", e.getMessage());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testCreateClusterAndCloseWithMultipleLogDirs(boolean combined) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder().
                        setNumBrokerNodes(5).
                        setNumDisksPerBroker(2).
                        setCombined(combined).
                        setNumControllerNodes(3).build()).build()) {

            TestKitNodes nodes = cluster.nodes();
            assertEquals(5, nodes.brokerNodes().size());
            assertEquals(3, nodes.controllerNodes().size());

            nodes.brokerNodes().forEach((brokerId, node) -> {
                assertEquals(2, node.logDataDirectories().size());
                Set<String> expected = new HashSet<>(Arrays.asList(String.format("broker_%d_data0", brokerId), String.format("broker_%d_data1", brokerId)));
                if (nodes.isCombined(node.id())) {
                    expected = new HashSet<>(Arrays.asList(String.format("combined_%d_0", brokerId), String.format("combined_%d_1", brokerId)));
                }
                assertEquals(
                        expected,
                        node.logDataDirectories().stream()
                                .map(p -> Paths.get(p).getFileName().toString())
                                .collect(Collectors.toSet())
                );
            });

            nodes.controllerNodes().forEach((controllerId, node) -> {
                String expected = combined ? String.format("combined_%d_0", controllerId) : String.format("controller_%d", controllerId);
                assertEquals(expected, Paths.get(node.metadataDirectory()).getFileName().toString());
            });
        }
    }

    @Test
    public void testCreateClusterWithSpecificBaseDir() throws Exception {
        Path baseDirectory = TestUtils.tempDirectory().toPath();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder().
                        setBaseDirectory(baseDirectory).
                        setNumBrokerNodes(1).
                        setCombined(true).
                        setNumControllerNodes(1).build()).build()) {
            assertEquals(cluster.nodes().baseDirectory(), baseDirectory.toFile().getAbsolutePath());
            cluster.nodes().controllerNodes().values().forEach(controller ->
                assertTrue(Paths.get(controller.metadataDirectory()).startsWith(baseDirectory)));
            cluster.nodes().brokerNodes().values().forEach(broker ->
                assertTrue(Paths.get(broker.metadataDirectory()).startsWith(baseDirectory)));
        }
    }
    @Test
    public void testExposedFaultHandlers() {
        TestKitNodes nodes = new TestKitNodes.Builder()
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(nodes).build()) {
            assertNotNull(cluster.fatalFaultHandler(), "Fatal fault handler should not be null");
            assertNotNull(cluster.nonFatalFaultHandler(), "Non-fatal fault handler should not be null");
        } catch (Exception e) {
            fail("Failed to initialize cluster", e);
        }
    }
}
