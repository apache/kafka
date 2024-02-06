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

package org.apache.kafka.image.node;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.image.ConfigurationImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.ConfigResource.Type.BROKER;
import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class ConfigurationsImageNodeTest {
    private static final ConfigurationsImageNode NODE;

    static {
        Map<ConfigResource, ConfigurationImage> resourceMap = new HashMap<>();
        for (ConfigResource resource : Arrays.asList(
                new ConfigResource(BROKER, ""),
                new ConfigResource(BROKER, "0"),
                new ConfigResource(TOPIC, ""),
                new ConfigResource(TOPIC, "foobar"),
                new ConfigResource(TOPIC, ":colons:"),
                new ConfigResource(TOPIC, "__internal"))) {
            resourceMap.put(resource, new ConfigurationImage(resource,
                    Collections.singletonMap("foo", "bar")));
        }
        ConfigurationsImage image = new ConfigurationsImage(resourceMap);
        NODE = new ConfigurationsImageNode(image);
    }

    @Test
    public void testNodeChildNames() {
        List<String> childNames = new ArrayList<>(NODE.childNames());
        childNames.sort(String::compareTo);
        assertEquals(Arrays.asList(
            "BROKER",
            "BROKER:0",
            "TOPIC",
            "TOPIC::colons:",
            "TOPIC:__internal",
            "TOPIC:foobar"), childNames);
    }

    @Test
    public void testNodeChildNameParsing() {
        List<ConfigResource> childResources = NODE.childNames().stream().
            sorted().
            map(ConfigurationsImageNode::resourceFromName).
            collect(Collectors.toList());
        assertEquals(Arrays.asList(
            new ConfigResource(BROKER, ""),
            new ConfigResource(BROKER, "0"),
            new ConfigResource(TOPIC, ""),
            new ConfigResource(TOPIC, ":colons:"),
            new ConfigResource(TOPIC, "__internal"),
            new ConfigResource(TOPIC, "foobar")), childResources);
    }
}
