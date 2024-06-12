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

import java.util.ArrayList;
import java.util.Collection;


public class ConfigurationsImageNode implements MetadataNode {
    /**
     * The name of this node.
     */
    public final static String NAME = "configs";

    /**
     * The configurations image.
     */
    private final ConfigurationsImage image;

    public ConfigurationsImageNode(ConfigurationsImage image) {
        this.image = image;
    }

    @Override
    public Collection<String> childNames() {
        ArrayList<String> childNames = new ArrayList<>();
        for (ConfigResource configResource : image.resourceData().keySet()) {
            if (configResource.isDefault()) {
                childNames.add(configResource.type().name());
            } else {
                childNames.add(configResource.type().name() + ":" + configResource.name());
            }
        }
        return childNames;
    }

    static ConfigResource resourceFromName(String name) {
        for (ConfigResource.Type type : ConfigResource.Type.values()) {
            if (name.startsWith(type.name())) {
                String key = name.substring(type.name().length());
                if (key.isEmpty()) {
                    return new ConfigResource(type, "");
                } else if (key.startsWith(":")) {
                    return new ConfigResource(type, key.substring(1));
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public MetadataNode child(String name) {
        ConfigResource resource = resourceFromName(name);
        if (resource == null) return null;
        ConfigurationImage configurationImage = image.resourceData().get(resource);
        if (configurationImage == null) return null;
        return new ConfigurationImageNode(configurationImage);
    }
}
