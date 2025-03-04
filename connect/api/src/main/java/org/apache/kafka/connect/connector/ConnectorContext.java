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
package org.apache.kafka.connect.connector;

import org.apache.kafka.common.metrics.PluginMetrics;

/**
 * ConnectorContext allows {@link Connector}s to proactively interact with the Kafka Connect runtime.
 */
public interface ConnectorContext {
    /**
     * Requests that the runtime reconfigure the Tasks for this source. This should be used to
     * indicate to the runtime that something about the input/output has changed (e.g. partitions
     * added/removed) and the running Tasks will need to be modified.
     */
    void requestTaskReconfiguration();

    /**
     * Raise an unrecoverable exception to the Connect framework. This will cause the status of the
     * connector to transition to FAILED.
     * @param e Exception to be raised.
     */
    void raiseError(Exception e);

    /**
     * Get a {@link PluginMetrics} that can be used to define metrics
     *
     * <p>This method was added in Apache Kafka 4.1. Connectors that use this method but want to
     * maintain backward compatibility so they can also be deployed to older Connect runtimes
     * should guard the call to this method with a try-catch block, since calling this method will result in a
     * {@link NoSuchMethodError} or {@link NoClassDefFoundError} when the connector is deployed to
     * Connect runtimes older than Kafka 4.1. For example:
     * <pre>
     *     PluginMetrics pluginMetrics;
     *     try {
     *         pluginMetrics = context.pluginMetrics();
     *     } catch (NoSuchMethodError | NoClassDefFoundError e) {
     *         pluginMetrics = null;
     *     }
     * </pre>
     *
     * @return the pluginMetrics instance
     * @since 4.1
     */
    PluginMetrics pluginMetrics();
}
