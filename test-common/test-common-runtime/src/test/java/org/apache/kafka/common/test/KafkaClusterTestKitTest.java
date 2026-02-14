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

import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        perServerProperties.put(100, Map.of("foo", "foo1"));
        perServerProperties.put(200, Map.of("bar", "bar1"));

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
    @CsvSource({
        "true,1,1,2", /* 1 combined node */
        "true,5,7,2", /* 5 combined nodes + 2 controllers */
        "true,7,5,2", /* 7 combined nodes */
        "false,1,1,2", /* 1 broker + 1 controller */
        "false,5,7,2", /* 5 brokers + 7 controllers */
        "false,7,5,2", /* 7 brokers + 5 controllers */
    })
    public void testCreateClusterFormatAndCloseWithMultipleLogDirs(boolean combined, int numBrokers, int numControllers, int numDisks) throws Exception {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
            new TestKitNodes.Builder().
                setNumBrokerNodes(numBrokers).
                setNumDisksPerBroker(numDisks).
                setCombined(combined).
                setNumControllerNodes(numControllers).build()).build()) {

            TestKitNodes nodes = cluster.nodes();
            assertEquals(numBrokers, nodes.brokerNodes().size());
            assertEquals(numControllers, nodes.controllerNodes().size());

            Set<String> logDirs = new HashSet<>();
            nodes.brokerNodes().forEach((brokerId, node) -> {
                assertEquals(numDisks, node.logDataDirectories().size());
                Set<String> expectedDisks = IntStream.range(0, numDisks)
                        .mapToObj(i -> {
                            if (nodes.isCombined(node.id())) {
                                return String.format("combined_%d_%d", brokerId, i);
                            } else {
                                return String.format("broker_%d_data%d", brokerId, i);
                            }
                        }).collect(Collectors.toSet());
                assertEquals(
                    expectedDisks,
                    node.logDataDirectories().stream()
                        .map(p -> Paths.get(p).getFileName().toString())
                        .collect(Collectors.toSet())
                );
                logDirs.addAll(node.logDataDirectories());
            });

            nodes.controllerNodes().forEach((controllerId, node) -> {
                String expected = nodes.isCombined(node.id()) ? String.format("combined_%d_0", controllerId) : String.format("controller_%d", controllerId);
                assertEquals(expected, Paths.get(node.metadataDirectory()).getFileName().toString());
                logDirs.addAll(node.logDataDirectories());
            });

            cluster.format();
            logDirs.forEach(logDir ->
                assertTrue(Files.exists(Paths.get(logDir, MetaPropertiesEnsemble.META_PROPERTIES_NAME)))
            );
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
