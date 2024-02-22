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

package org.apache.kafka.metadata.properties;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final public class MetaPropertiesTest {
    @Test
    public void testV0SerializationWithNothing() {
        testV0Serialization(Optional.empty(),
            OptionalInt.empty(),
             Optional.empty(),
            "MetaProperties(version=0)");
    }

    @Test
    public void testV0SerializationWithJustClusterId() {
        testV0Serialization(Optional.of("zd2vLVrZQlCLJj8-k7A10w"),
            OptionalInt.empty(),
            Optional.empty(),
            "MetaProperties(version=0, clusterId=zd2vLVrZQlCLJj8-k7A10w)");
    }

    @Test
    public void testV0SerializationWithJustNodeId() {
        testV0Serialization(Optional.empty(),
            OptionalInt.of(0),
            Optional.empty(),
            "MetaProperties(version=0, nodeId=0)");
    }

    @Test
    public void testV0SerializationWithJustClusterIdAndNodeId() {
        testV0Serialization(Optional.of("zd2vLVrZQlCLJj8-k7A10w"),
            OptionalInt.of(0),
            Optional.empty(),
            "MetaProperties(version=0, clusterId=zd2vLVrZQlCLJj8-k7A10w, nodeId=0)");
    }

    @Test
    public void testV0SerializationWithAll() {
        testV0Serialization(Optional.of("zd2vLVrZQlCLJj8-k7A10w"),
            OptionalInt.of(0),
            Optional.of(Uuid.fromString("3Adc4FjfTeypRWROmQDNIQ")),
            "MetaProperties(version=0, clusterId=zd2vLVrZQlCLJj8-k7A10w, nodeId=0, " +
                "directoryId=3Adc4FjfTeypRWROmQDNIQ)");
    }

    private void testV0Serialization(
        Optional<String> clusterId,
        OptionalInt nodeId,
        Optional<Uuid> directoryId,
        String expectedToStringOutput
    ) {
        MetaProperties metaProperties = new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V0).
                setClusterId(clusterId).
                setNodeId(nodeId).
                setDirectoryId(directoryId).
                build();
        assertEquals(MetaPropertiesVersion.V0, metaProperties.version());
        assertEquals(clusterId, metaProperties.clusterId());
        assertEquals(nodeId, metaProperties.nodeId());
        assertEquals(directoryId, metaProperties.directoryId());
        Properties props = new Properties();
        props.setProperty("version", "0");
        if (clusterId.isPresent()) {
            props.setProperty("cluster.id", clusterId.get());
        }
        if (nodeId.isPresent()) {
            props.setProperty("broker.id", "" + nodeId.getAsInt());
        }
        if (directoryId.isPresent()) {
            props.setProperty("directory.id", directoryId.get().toString());
        }
        Properties props2 = metaProperties.toProperties();
        assertEquals(props, props2);
        MetaProperties metaProperties2 = new MetaProperties.Builder(props2).build();
        System.out.println("metaProperties = " + metaProperties.toString());
        System.out.println("metaProperties2 = " + metaProperties2.toString());
        assertEquals(metaProperties, metaProperties2);
        assertEquals(metaProperties.hashCode(), metaProperties2.hashCode());
        assertEquals(metaProperties.toString(), metaProperties2.toString());
        assertEquals(expectedToStringOutput, metaProperties.toString());
    }

    @Test
    public void testV1SerializationWithoutDirectoryId() {
        testV1Serialization("zd2vLVrZQlCLJj8-k7A10w",
            0,
            Optional.empty(),
            "MetaProperties(version=1, clusterId=zd2vLVrZQlCLJj8-k7A10w, nodeId=0)");
    }

    @Test
    public void testV1SerializationWithDirectoryId() {
        testV1Serialization("zd2vLVrZQlCLJj8-k7A10w",
            1,
            Optional.of(Uuid.fromString("3Adc4FjfTeypRWROmQDNIQ")),
            "MetaProperties(version=1, clusterId=zd2vLVrZQlCLJj8-k7A10w, nodeId=1, " +
                "directoryId=3Adc4FjfTeypRWROmQDNIQ)");
    }

    @Test
    public void testV1SerializationWithNonUuidClusterId() {
        testV1Serialization("my@cluster@id",
            2,
            Optional.empty(),
            "MetaProperties(version=1, clusterId=my@cluster@id, nodeId=2)");
    }

    private void testV1Serialization(
        String clusterId,
        int nodeId,
        Optional<Uuid> directoryId,
        String expectedToStringOutput
    ) {
        MetaProperties metaProperties = new MetaProperties.Builder().
            setVersion(MetaPropertiesVersion.V1).
            setClusterId(clusterId).
            setNodeId(nodeId).
            setDirectoryId(directoryId).
            build();
        assertEquals(MetaPropertiesVersion.V1, metaProperties.version());
        assertEquals(Optional.of(clusterId), metaProperties.clusterId());
        assertEquals(OptionalInt.of(nodeId), metaProperties.nodeId());
        assertEquals(directoryId, metaProperties.directoryId());
        Properties props = new Properties();
        props.setProperty("version", "1");
        props.setProperty("cluster.id", clusterId);
        props.setProperty("node.id", "" + nodeId);
        if (directoryId.isPresent()) {
            props.setProperty("directory.id", directoryId.get().toString());
        }
        Properties props2 = metaProperties.toProperties();
        assertEquals(props, props2);
        MetaProperties metaProperties2 = new MetaProperties.Builder(props2).build();
        assertEquals(metaProperties, metaProperties2);
        assertEquals(metaProperties.hashCode(), metaProperties2.hashCode());
        assertEquals(metaProperties.toString(), metaProperties2.toString());
        assertEquals(expectedToStringOutput, metaProperties.toString());
    }

    @Test
    public void testClusterIdRequiredInV1() {
        assertEquals("cluster.id was not found.", assertThrows(RuntimeException.class,
            () -> new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setNodeId(1).
                build()).getMessage());
    }

    @Test
    public void testNodeIdRequiredInV1() {
        assertEquals("node.id was not found.", assertThrows(RuntimeException.class,
            () -> new MetaProperties.Builder().
                setVersion(MetaPropertiesVersion.V1).
                setClusterId("zd2vLVrZQlCLJj8-k7A10w").
                build()).getMessage());
    }
}
