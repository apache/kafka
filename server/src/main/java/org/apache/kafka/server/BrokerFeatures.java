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
package org.apache.kafka.server;

import org.apache.kafka.common.feature.Features;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * A class that encapsulates the latest features supported by the Broker and also provides APIs to
 * check for incompatibilities between the features supported by the Broker and finalized features.
 * This class is immutable in production. It provides few APIs to mutate state only for the purpose
 * of testing.
 */
public class BrokerFeatures {

    private volatile Features<SupportedVersionRange> supportedFeatures;
    private static final Logger log = LoggerFactory.getLogger(BrokerFeatures.class);

    private BrokerFeatures(Features<SupportedVersionRange> supportedFeatures) {
        this.supportedFeatures = supportedFeatures;
    }

    public static BrokerFeatures createDefault(boolean unstableFeatureVersionsEnabled) {
        return new BrokerFeatures(defaultSupportedFeatures(unstableFeatureVersionsEnabled));
    }

    public static Map<String, VersionRange> createDefaultFeatureMap(BrokerFeatures features) {
        Map<String, SupportedVersionRange> supportedFeatures = features.supportedFeatures.features();
        Map<String, VersionRange> result = new HashMap<>();
        for (Map.Entry<String, SupportedVersionRange> entry : supportedFeatures.entrySet()) {
            result.put(entry.getKey(), VersionRange.of(entry.getValue().min(), entry.getValue().max()));
        }
        return result;
    }

    public static Features<SupportedVersionRange> defaultSupportedFeatures(boolean unstableFeatureVersionsEnabled) {
        Map<String, SupportedVersionRange> features = new HashMap<>();
        features.put(MetadataVersion.FEATURE_NAME,
                new SupportedVersionRange(
                        MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
                        unstableFeatureVersionsEnabled ? MetadataVersion.latestTesting().featureLevel()
                                : MetadataVersion.latestProduction().featureLevel()));
        org.apache.kafka.server.common.Features.PRODUCTION_FEATURES.forEach(feature -> {
            int maxVersion = unstableFeatureVersionsEnabled ? feature.latestTesting() : feature.latestProduction();
            if (maxVersion > 0) {
                features.put(feature.featureName(), new SupportedVersionRange(feature.minimumProduction(), (short) maxVersion));
            }
        });
        return Features.supportedFeatures(features);
    }

    public static BrokerFeatures createEmpty() {
        return new BrokerFeatures(Features.emptySupportedFeatures());
    }

    /**
     * Returns true if any of the provided finalized features are incompatible with the provided
     * supported features.
     *
     * @param supportedFeatures The supported features to be compared
     * @param finalizedFeatures The finalized features to be compared
     * @return - True if there are any feature incompatibilities found.
     * - False otherwise.
     */
    public static boolean hasIncompatibleFeatures(Features<SupportedVersionRange> supportedFeatures,
                                                  Map<String, Short> finalizedFeatures) {
        return !incompatibleFeatures(supportedFeatures, finalizedFeatures, false).isEmpty();
    }

    // For testing only
    public void setSupportedFeatures(Features<SupportedVersionRange> newFeatures) {
        Map<String, SupportedVersionRange> combined = new HashMap<>(supportedFeatures.features());
        combined.putAll(newFeatures.features());
        supportedFeatures = Features.supportedFeatures(combined);
    }

    /**
     * Returns the default finalized features that a new Kafka cluster with IBP config >= IBP_2_7_IV0
     * needs to be bootstrapped with.
     */
    public Map<String, Short> defaultFinalizedFeatures() {
        return supportedFeatures.features().entrySet()
                .stream()
                .collect(HashMap::new, (m, e) -> {
                    String name = e.getKey();
                    SupportedVersionRange versionRange = e.getValue();
                    if ("kraft.version".equals(name)) {
                        m.put(name, (short) 0);
                    } else {
                        m.put(name, versionRange.max());
                    }
                }, HashMap::putAll);
    }

    /**
     * Returns the set of feature names found to be incompatible.
     * A feature incompatibility is a version mismatch between the latest feature supported by the
     * Broker, and a provided finalized feature. This can happen because a provided finalized
     * feature:
     * 1) Does not exist in the Broker (i.e. it is unknown to the Broker).
     * [OR]
     * 2) Exists but the FinalizedVersionRange does not match with the SupportedVersionRange
     * of the supported feature.
     *
     * @param finalized The finalized features against which incompatibilities need to be checked for.
     * @return The subset of input features which are incompatible. If the returned object
     * is empty, it means there were no feature incompatibilities found.
     */
    public Map<String, Short> incompatibleFeatures(Map<String, Short> finalized) {
        return BrokerFeatures.incompatibleFeatures(supportedFeatures, finalized, true);
    }

    public Features<SupportedVersionRange> supportedFeatures() {
        return supportedFeatures;
    }

    private static Map<String, Short> incompatibleFeatures(Features<SupportedVersionRange> supportedFeatures,
                                                           Map<String, Short> finalizedFeatures,
                                                           boolean logIncompatibilities) {
        List<IncompatibleFeaturesInfo> incompatibleFeaturesInfo = finalizedFeatures
                .entrySet()
                .stream()
                .map(entry -> transferToIncompatibleFeaturesInfo(supportedFeatures, entry))
                .filter(info -> info.errorReason != null)
                .collect(Collectors.toList());
        if (logIncompatibilities && !incompatibleFeaturesInfo.isEmpty()) {
            log.warn("Feature incompatibilities seen: {}", incompatibleFeaturesInfo.stream()
                    .map(info -> info.errorReason)
                    .collect(Collectors.joining(", ")));
        }
        return incompatibleFeaturesInfo.stream()
                .collect(Collectors.toMap(info -> info.feature, info -> info.versionLevels));
    }

    private static IncompatibleFeaturesInfo transferToIncompatibleFeaturesInfo(Features<SupportedVersionRange> supportedFeatures,
                                                                               Map.Entry<String, Short> entry) {
        String feature = entry.getKey();
        Short versionLevels = entry.getValue();
        SupportedVersionRange supportedVersions = supportedFeatures.get(feature);
        if (supportedVersions == null) {
            return new IncompatibleFeaturesInfo(
                    feature,
                    versionLevels,
                    format("{feature=%s, reason='Unsupported feature'}", feature)
            );
        } else if (supportedVersions.isIncompatibleWith(versionLevels)) {
            return new IncompatibleFeaturesInfo(
                    feature,
                    versionLevels,
                    format("{feature=%s, reason='%s is incompatible with %s'}", feature, versionLevels, supportedVersions)
            );
        } else {
            return new IncompatibleFeaturesInfo(feature, versionLevels, null);
        }
    }

    private static class IncompatibleFeaturesInfo {
        final String feature;
        final short versionLevels;
        final String errorReason;

        IncompatibleFeaturesInfo(String feature, short versionLevels, String errorReason) {
            this.feature = feature;
            this.versionLevels = versionLevels;
            this.errorReason = errorReason;
        }
    }
}