package org.apache.kafka.server.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.controller.QuorumFeatures;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.Feature;

import java.util.LinkedHashMap;
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
        for (var featureName : Feature.PRODUCTION_FEATURE_NAMES) {
            addSupportedLevelMetric(MAXIMUM_SUPPORTED_LEVEL_NAME, featureName);
            addSupportedLevelMetric(MINIMUM_SUPPORTED_LEVEL_NAME, featureName);
        }
    }

    private void addSupportedLevelMetric(String metricName, String featureName) {
        metrics.addMetric(
            getFeatureNameTagMetricName(
                metricName,
                METRIC_GROUP_NAME,
                featureName
            ),
            (config, now) -> {
                if (metricName.equals(MAXIMUM_SUPPORTED_LEVEL_NAME)) {
                    return supportedFeatureRanges.get(featureName).max();
                } else {
                    return supportedFeatureRanges.get(featureName).min();
                }
            }
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
        LinkedHashMap<String, String> featureNameTag = new LinkedHashMap<>();
        featureNameTag.put(FEATURE_NAME_TAG, featureName);
        return metrics.metricName(name, group, featureNameTag);
    }
}
