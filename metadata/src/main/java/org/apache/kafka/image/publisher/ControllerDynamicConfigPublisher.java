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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Publishes CONTROLLER resource configuration changes from the metadata log
 * to registered consumers (e.g., DynamicControllerConfig).
 */
public class ControllerDynamicConfigPublisher implements MetadataPublisher {
    private static final Logger log = LoggerFactory.getLogger(ControllerDynamicConfigPublisher.class);

    private final int nodeId;
    private final Consumer<Map<String, String>> configConsumer;

    public ControllerDynamicConfigPublisher(
        int nodeId,
        Consumer<Map<String, String>> configConsumer
    ) {
        this.nodeId = nodeId;
        this.configConsumer = configConsumer;
    }

    @Override
    public String name() {
        return "ControllerDynamicConfigPublisher";
    }

    @Override
    public void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage,
        LoaderManifest manifest
    ) {
        if (delta.configsDelta() == null) {
            return;
        }

        Map<String, String> effectiveConfig = new HashMap<>();
        ConfigResource clusterDefault = new ConfigResource(ConfigResource.Type.CONTROLLER, "");
        Map<String, String> clusterDefaultConfigs = newImage.configs().configMapForResource(clusterDefault);
        if (clusterDefaultConfigs != null) {
            effectiveConfig.putAll(clusterDefaultConfigs);
        }

        ConfigResource nodeResource = new ConfigResource(ConfigResource.Type.CONTROLLER, String.valueOf(nodeId));
        Map<String, String> nodeConfigs = newImage.configs().configMapForResource(nodeResource);
        if (nodeConfigs != null) {
            effectiveConfig.putAll(nodeConfigs);
        }

        if (!effectiveConfig.isEmpty() || delta.configsDelta().changes().containsKey(clusterDefault) ||
                delta.configsDelta().changes().containsKey(nodeResource)) {
            log.info("Publishing controller config update for node {}: {} configs", nodeId, effectiveConfig.size());
            configConsumer.accept(effectiveConfig);
        }
    }

}
