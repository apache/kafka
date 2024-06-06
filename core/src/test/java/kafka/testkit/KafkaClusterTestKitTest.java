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

package kafka.testkit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
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
    public void testCreateClusterAndCloseWithMultipleLogDirs(boolean combined) {
        try (KafkaClusterTestKit cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder().
                        setNumBrokerNodes(5).
                        setNumDisksPerBroker(2).
                        setCombined(combined).
                        setNumControllerNodes(3).build()).build()) {

            assertEquals(5, cluster.nodes().brokerNodes().size());
            assertEquals(3, cluster.nodes().controllerNodes().size());

            cluster.nodes().brokerNodes().forEach((brokerId, node) -> {
                assertEquals(2, node.logDataDirectories().size());
                Set<String> expected = new HashSet<>(Arrays.asList(String.format("broker_%d_data0", brokerId), String.format("broker_%d_data1", brokerId)));
                if (node.combined()) {
                    expected = new HashSet<>(Arrays.asList(String.format("combined_%d_0", brokerId), String.format("combined_%d_1", brokerId)));
                }
                assertEquals(
                        expected,
                        node.logDataDirectories().stream()
                                .map(p -> Paths.get(p).getFileName().toString())
                                .collect(Collectors.toSet())
                );
            });

            cluster.nodes().controllerNodes().forEach((controllerId, node) -> {
                String expected = combined ? String.format("combined_%d_0", controllerId) : String.format("controller_%d", controllerId);
                assertEquals(expected, Paths.get(node.metadataDirectory()).getFileName().toString());
            });
        } catch (Exception e) {
            fail("failed to init cluster", e);
        }
    }
}
