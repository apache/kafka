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

package org.apache.kafka.image;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Represents changes to the configurations in the metadata image.
 */
public final class ConfigurationsDelta {
    private final ConfigurationsImage image;
    private final Map<ConfigResource, ConfigurationDelta> changes = new HashMap<>();

    public ConfigurationsDelta(ConfigurationsImage image) {
        this.image = image;
    }

    public Map<ConfigResource, ConfigurationDelta> changes() {
        return changes;
    }

    public void finishSnapshot() {
        for (Entry<ConfigResource, ConfigurationImage> entry : image.resourceData().entrySet()) {
            ConfigResource resource = entry.getKey();
            ConfigurationImage configImage = entry.getValue();
            ConfigurationDelta configDelta = changes.computeIfAbsent(resource,
                __ -> new ConfigurationDelta(configImage));
            configDelta.finishSnapshot();
        }
    }

    public void handleMetadataVersionChange(MetadataVersion newVersion) {
        // no-op
    }

    public void replay(ConfigRecord record) {
        ConfigResource resource =
            new ConfigResource(Type.forId(record.resourceType()), record.resourceName());
        ConfigurationImage configImage = image.resourceData().getOrDefault(resource,
                new ConfigurationImage(resource, Collections.emptyMap()));
        ConfigurationDelta delta = changes.computeIfAbsent(resource,
            __ -> new ConfigurationDelta(configImage));
        delta.replay(record);
    }

    public void replay(RemoveTopicRecord record, String topicName) {
        ConfigResource resource =
            new ConfigResource(Type.TOPIC, topicName);
        ConfigurationImage configImage = image.resourceData().getOrDefault(resource,
                new ConfigurationImage(resource, Collections.emptyMap()));
        ConfigurationDelta delta = changes.computeIfAbsent(resource,
            __ -> new ConfigurationDelta(configImage));
        delta.deleteAll();
    }

    public ConfigurationsImage apply() {
        Map<ConfigResource, ConfigurationImage> newData = new HashMap<>();
        for (Entry<ConfigResource, ConfigurationImage> entry : image.resourceData().entrySet()) {
            ConfigResource resource = entry.getKey();
            ConfigurationDelta delta = changes.get(resource);
            if (delta == null) {
                newData.put(resource, entry.getValue());
            } else {
                ConfigurationImage newImage = delta.apply();
                if (!newImage.isEmpty()) {
                    newData.put(resource, newImage);
                }
            }
        }
        for (Entry<ConfigResource, ConfigurationDelta> entry : changes.entrySet()) {
            if (!newData.containsKey(entry.getKey())) {
                ConfigurationImage newImage = entry.getValue().apply();
                if (!newImage.isEmpty()) {
                    newData.put(entry.getKey(), newImage);
                }
            }
        }
        return new ConfigurationsImage(newData);
    }

    @Override
    public String toString() {
        return "ConfigurationsDelta(" +
            "changes=" + changes +
            ')';
    }
}
