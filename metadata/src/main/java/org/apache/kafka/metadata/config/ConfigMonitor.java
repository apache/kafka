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

import org.slf4j.Logger;

/**
 * Monitors a specific configuration key that is associated with a resource.
 *
 * This class has no internal synchronization.
 */
public interface ConfigMonitor<T> {
    /**
     * Get the configuration key and resource type that this monitor is associated with.
     *
     * @return the key.
     */
    ConfigMonitorKey key();

    /**
     * Returns whether this configuration is sensitive.
     *
     * @return true if this configuration cannot be logged.
     */
    boolean isSensitive();

    /**
     * Handle a change to the configuration that we are monitoring.
     *
     * @param logger        The slf4j object to use for logging.
     * @param resourceName  The resource name which changed, or the empty string if the dynamic
     *                      default changed.
     * @param newValue      The new value, or null if the dynamic configuration was deleted.
     */
    void update(Logger logger, String resourceName, T newValue);
}
