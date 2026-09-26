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

package org.apache.kafka.metadata;

import org.apache.kafka.common.config.ConfigResource;

/**
 * Interface for checking if a configuration name is supported for a given resource type.
 */
@FunctionalInterface
public interface SupportedConfigChecker {
    /**
     * Check if a configuration name is supported for the given resource type.
     *
     * @param resourceType the type of resource (broker, topic, user, etc.)
     * @param configName   the name of the configuration
     * @return true if the configuration is supported for the resource type, false otherwise
     */
    boolean isSupported(ConfigResource.Type resourceType, String configName);

    /**
     * A SupportedConfigChecker that always returns true, accepting all configurations.
     */
    SupportedConfigChecker TRUE = (resourceType, configName) -> true;
}
