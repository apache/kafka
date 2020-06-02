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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.config.ConfigException;

/**
 * A plugin interface to allow things to listen as new metrics are created so they can be reported.
 * <p>
 * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
 */
public interface MetricsReporter extends Reconfigurable, AutoCloseable {

    /**
     * This is called when the reporter is first registered to initially register all existing metrics
     * @param metrics All currently existing metrics
     */
    void init(List<KafkaMetric> metrics);

    /**
     * This is called whenever a metric is updated or added
     * @param metric
     */
    void metricChange(KafkaMetric metric);

    /**
     * This is called whenever a metric is removed
     * @param metric
     */
    void metricRemoval(KafkaMetric metric);

    /**
     * Called when the metrics repository is closed.
     */
    void close();

    // default methods for backwards compatibility with reporters that only implement Configurable
    default Set<String> reconfigurableConfigs() {
        return Collections.emptySet();
    }

    default void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    }

    default void reconfigure(Map<String, ?> configs) {
    }

    /**
     * Sets the context labels for the service or library exposing metrics. This will be called before {@link #init(List)} and may be called anytime after that.
     *
     * @param metricsContext the metric context
     */
    @InterfaceStability.Evolving
    default void contextChange(MetricsContext metricsContext) {
    }
}
