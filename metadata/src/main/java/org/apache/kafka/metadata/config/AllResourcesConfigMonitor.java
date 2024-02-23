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

package org.apache.kafka.metadata.config;

import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * Monitors a configuration key that is associated with any resource.
 *
 * This class has no internal synchronization.
 */
public final class AllResourcesConfigMonitor<T> implements ConfigMonitor<T> {
    /**
     * The configuration key name and resource type that this monitor is associated with.
     */
    private final ConfigMonitorKey key;

    /**
     * True if the configuration key being watched is sensitive.
     */
    private final boolean isSensitive;

    /**
     * The callback to invoke when a change occurs. The first parameter is the resource name that
     * changed; the second is the new value.
     */
    private final BiConsumer<String, T> callback;

    /**
     * The statically configured default value of the configuration. This comes either from the
     * static node configuration, or from the hard-coded configuration.
     */
    private final T staticDefault;

    /**
     * A map from resource names to the currently configured values. The dynamic default is
     * maintained in this map as well, under the empty string.
     */
    private final Map<String, T> dynamicValues;

    AllResourcesConfigMonitor(
        ConfigMonitorKey key,
        boolean isSensitive,
        BiConsumer<String, T> callback,
        T staticDefault
    ) {
        this.key = key;
        this.isSensitive = isSensitive;
        this.callback = callback;
        this.staticDefault = staticDefault;
        this.dynamicValues = new HashMap<>();
    }

    @Override
    public ConfigMonitorKey key() {
        return key;
    }

    @Override
    public boolean isSensitive() {
        return isSensitive;
    }

    @Override
    public void update(Logger log, String resourceName, T newValue) {
        T oldValue = dynamicValues.put(resourceName, newValue);
        if (Objects.equals(newValue, oldValue)) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping callbacks because configuration {} for {} remains unchanged as '{}'",
                    key.keyName(), new ConfigResource(key.resourceType(), resourceName),
                    isSensitive ? "[redacted]" : newValue);
            }
            return;
        }
        if (resourceName.isEmpty()) {
            if (newValue == null) {
                // The dynamic default was deleted, so update the default to be the static default.
                if (log.isDebugEnabled()) {
                    log.debug("Removed dynamic default for configuration {} for {}. " +
                        "Now using static default of '{}'", key.keyName(), key.resourceType(),
                        isSensitive ? "[redacted]" : staticDefault);
                }
                callback.accept("", staticDefault);
            } else {
                // Update the dynamic default.
                if (log.isDebugEnabled()) {
                    log.debug("Set dynamic default for configuration {} for {} to {}." +
                        key.keyName(), key.resourceType(), isSensitive ? "[redacted]" : newValue);
                }
                callback.accept("", newValue);
            }
        } else {
            // Update a per-resource configuration value. If the per-resource config was deleted,
            // we'll pass null as the new value here.
            if (log.isDebugEnabled()) {
                log.debug("Set configuration {} for {} to {}.",
                    key.keyName(), new ConfigResource(key.resourceType(), resourceName),
                    isSensitive ? "[redacted]" : newValue);
            }
            callback.accept(resourceName, newValue);
        }
    }
}
