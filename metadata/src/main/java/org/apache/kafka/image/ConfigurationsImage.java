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
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;


/**
 * Represents the configurations in the metadata image.
 *
 * This class is thread-safe.
 */
public final class ConfigurationsImage {
    public static final ConfigurationsImage EMPTY =
        new ConfigurationsImage(Collections.emptyMap());

    private final Map<ConfigResource, ConfigurationImage> data;

    public ConfigurationsImage(Map<ConfigResource, ConfigurationImage> data) {
        this.data = Collections.unmodifiableMap(data);
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    Map<ConfigResource, ConfigurationImage> resourceData() {
        return data;
    }

    public Properties configProperties(ConfigResource configResource) {
        ConfigurationImage configurationImage = data.get(configResource);
        if (configurationImage != null) {
            return configurationImage.toProperties();
        } else {
            return new Properties();
        }
    }

    public void write(Consumer<List<ApiMessageAndVersion>> out) {
        for (Entry<ConfigResource, ConfigurationImage> entry : data.entrySet()) {
            ConfigResource configResource = entry.getKey();
            ConfigurationImage configImage = entry.getValue();
            configImage.write(configResource, out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConfigurationsImage)) return false;
        ConfigurationsImage other = (ConfigurationsImage) o;
        return data.equals(other.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }

    @Override
    public String toString() {
        return "ConfigurationsImage(data=" + data.entrySet().stream().
            map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining(", ")) +
            ")";
    }
}
