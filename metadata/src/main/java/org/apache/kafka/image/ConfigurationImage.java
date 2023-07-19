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
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.image.node.ConfigurationImageNode;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;


/**
 * Represents the configuration of a resource.
 *
 * This class is thread-safe.
 */
public final class ConfigurationImage {
    private final ConfigResource resource;

    private final Map<String, String> data;

    public ConfigurationImage(
        ConfigResource resource,
        Map<String, String> data
    ) {
        this.resource = resource;
        this.data = data;
    }

    public ConfigResource resource() {
        return resource;
    }

    public Map<String, String> data() {
        return data;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.putAll(data);
        return properties;
    }

    public Map<String, String> toMap() {
        return Collections.unmodifiableMap(data);
    }

    public void write(
        ConfigResource configResource,
        ImageWriter writer,
        ImageWriterOptions options
    ) {
        for (Map.Entry<String, String> entry : data.entrySet()) {
            writer.write(0, new ConfigRecord().
                setResourceType(configResource.type().id()).
                setResourceName(configResource.name()).
                setName(entry.getKey()).
                setValue(entry.getValue()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConfigurationImage)) return false;
        ConfigurationImage other = (ConfigurationImage) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return new ConfigurationImageNode(this).stringify();
    }
}
