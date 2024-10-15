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
package org.apache.kafka.common.metrics;

/**
 * Plugins can implement this interface to register their own metrics.
 */
public interface Monitorable {

    /**
     * Provides a {@link PluginMetrics} instance from the component that instantiates the plugin.
     * PluginMetrics can be used by the plugin to register and unregister metrics
     * at any point in their lifecycle prior to their close method being called.
     * Any metrics registered will be automatically removed when the plugin is closed.
     */
    void withPluginMetrics(PluginMetrics metrics);

}
