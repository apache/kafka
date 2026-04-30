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

package org.apache.kafka.server.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.controller.QuorumFeatures;
import org.apache.kafka.metadata.VersionRange;

import java.util.Map;

public final class NodeMetrics implements AutoCloseable {
    private static final String METRIC_GROUP_NAME = "node-metrics";
    private static final String FEATURE_NAME_TAG = "feature-name";
    private static final String MAXIMUM_SUPPORTED_LEVEL_NAME = "maximum-supported-level";
    private static final String MINIMUM_SUPPORTED_LEVEL_NAME = "minimum-supported-level";

    private final Metrics metrics;
    private final Map<String, VersionRange> supportedFeatureRanges;

    public NodeMetrics(Metrics metrics, boolean enableUnstableVersions) {
        this.metrics = metrics;
        this.supportedFeatureRanges = QuorumFeatures.defaultSupportedFeatureMap(enableUnstableVersions);
        supportedFeatureRanges.forEach((featureName, versionRange) -> {
            addSupportedLevelMetric(MAXIMUM_SUPPORTED_LEVEL_NAME, featureName, versionRange.max());
            addSupportedLevelMetric(MINIMUM_SUPPORTED_LEVEL_NAME, featureName, versionRange.min());
        });
    }

    private void addSupportedLevelMetric(String metricName, String featureName, short value) {
        metrics.addMetric(
            getFeatureNameTagMetricName(
                metricName,
                METRIC_GROUP_NAME,
                featureName
            ),
            (Gauge<Short>) (config, now) -> value
        );
    }

    @Override
    public void close() {
        for (var featureName : supportedFeatureRanges.keySet()) {
            metrics.removeMetric(
                getFeatureNameTagMetricName(
                    MAXIMUM_SUPPORTED_LEVEL_NAME,
                    METRIC_GROUP_NAME,
                    featureName
                )
            );
            metrics.removeMetric(
                getFeatureNameTagMetricName(
                    MINIMUM_SUPPORTED_LEVEL_NAME,
                    METRIC_GROUP_NAME,
                    featureName
                )
            );
        }
    }

    private MetricName getFeatureNameTagMetricName(String name, String group, String featureName) {
        return metrics.metricName(
            name,
            group,
            Map.of(FEATURE_NAME_TAG, featureName.replace(".", "-"))
        );
    }
}
