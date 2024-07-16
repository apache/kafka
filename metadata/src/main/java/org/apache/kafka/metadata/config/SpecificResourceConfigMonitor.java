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

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Monitors a configuration key that is associated with a specific resource.
 *
 * This class has no internal synchronization.
 *
 * @param <T> The configuration value type.
 */
public final class SpecificResourceConfigMonitor<T> implements ConfigMonitor<T> {
    /**
     * The configuration name and resource type that this monitor is associated with.
     */
    private final ConfigMonitorKey key;

    /**
     * True if the configuration key being watched is sensitive.
     */
    private final boolean isSensitive;

    /**
     * The resource name that we are interested in.
     */
    private final String targetResourceName;

    /**
     * The callback to invoke when a change occurs.
     */
    private final Consumer<T> callback;

    /**
     * The statically configured default value of the configuration. This comes either from the
     * static node configuration, or from the hard-coded configuration.
     */
    private final T staticDefault;

    /**
     * The dynamic default of the configuration.
     */
    private T dynamicDefault;

    /**
     * The dynamic value of the configuration.
     */
    private T dynamicValue;

    SpecificResourceConfigMonitor(
        ConfigMonitorKey key,
        boolean isSensitive,
        String targetResourceName,
        Consumer<T> callback,
        T staticDefault
    ) {
        this.key = key;
        this.isSensitive = isSensitive;
        this.targetResourceName = targetResourceName;
        this.callback = callback;
        this.staticDefault = staticDefault;
        this.dynamicDefault = null;
        this.dynamicValue = null;
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
        T prevEffectiveValue;

        if (resourceName.isEmpty()) {
            // Handle the dynamic default
            if (Objects.equals(dynamicDefault, newValue)) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping callback because default configuration {} for {} remains " +
                        "unchanged as '{}'", key.keyName(), key.resourceType(),
                        isSensitive ? "[redacted]" : newValue);
                }
                return;
            }
            prevEffectiveValue = effectiveValue();
            dynamicDefault = newValue;
        } else if (!resourceName.equals(targetResourceName)) {
            // We don't care about this resource.
            if (log.isTraceEnabled()) {
                log.trace("Ignoring change to configuration {} for {}.",
                    key.keyName(), new ConfigResource(key.resourceType(), resourceName));
            }
            return;
        } else {
            // Handle a change to the resource we care about.
            if (Objects.equals(dynamicValue, newValue)) {
                if (log.isDebugEnabled()) {
                    log.debug("Skipping callback because configuration {} for {} remains " +
                        "unchanged as '{}'", key.keyName(),
                        new ConfigResource(key.resourceType(), resourceName),
                        isSensitive ? "[redacted]" : newValue);
                }
                return;
            }
            prevEffectiveValue = effectiveValue();
            dynamicValue = newValue;
        }
        T newEffectiveValue = effectiveValue();
        if (!Objects.equals(prevEffectiveValue, newEffectiveValue)) {
            if (log.isDebugEnabled()) {
                log.debug("Set configuration for {} for {} to '{}'",
                    key.keyName(), key.resourceType(), isSensitive ? "[redacted]" : newValue);
            }
            callback.accept(newEffectiveValue);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("Skipping callback because effective configuration for {} for {} " +
                    "remains unchanged as '{}'", key.keyName(), key.resourceType(),
                    isSensitive ? "[redacted]" : newValue);
            }
        }
    }

    /**
     * Find the effective value of the dynamic configuration that is seen by the specific
     * configuration resource we are monitoring.
     *
     * To find the effective value, there are three levels of overrides. The per-resource dynamic
     * configuration always has highest priority. For example, if we set a dynamic configuration on
     * resourceType=BROKER, resourceName=123 for num.io.threads=10, that will override everything else.
     * If the per-resource dynamic configuration does not exist, the dynamic default is used.  To
     * continue our example, that would be the config associated with resourceType="",
     * resourceName=123. If the dynamic default does not exist, the static default is used. This comes
     * either from the static configuration or the hard-coded defaults.
     */
    private T effectiveValue() {
        if (dynamicValue != null) return dynamicValue;
        if (dynamicDefault != null) return dynamicDefault;
        return staticDefault;
    }
}
