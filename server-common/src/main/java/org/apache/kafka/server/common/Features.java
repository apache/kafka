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
package org.apache.kafka.server.common;

import org.apache.kafka.common.feature.SupportedVersionRange;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This is enum for the various features implemented for Kafka clusters.
 * KIP-584: Versioning Scheme for Features introduced the idea of various features, but only added one feature -- MetadataVersion.
 * KIP-1022: Formatting and Updating Features allowed for more features to be added. In order to set and update features,
 * they need to be specified via the StorageTool or FeatureCommand tools.
 * <br>
 * Having a unified enum for the features that will use a shared type in the API used to set and update them
 * makes it easier to process these features.
 */
public enum Features {

    /**
     * Features defined. If a feature is included in this list, and marked to be used in production they will also be specified when
     * formatting a cluster via the StorageTool. MetadataVersion is handled separately, so it is not included here.
     *
     * See {@link TestFeatureVersion} as an example. See {@link FeatureVersion} when implementing a new feature.
     */
    TEST_VERSION("test.feature.version", TestFeatureVersion.values()),
    KRAFT_VERSION("kraft.version", KRaftVersion.values()),
    TRANSACTION_VERSION("transaction.version", TransactionVersion.values()),
    GROUP_VERSION("group.version", GroupVersion.values()),
    ELIGIBLE_LEADER_REPLICAS_VERSION("eligible.leader.replicas.version", EligibleLeaderReplicasVersion.values());

    public static final Features[] FEATURES;
    public static final List<Features> PRODUCTION_FEATURES;

    public static final List<String> PRODUCTION_FEATURE_NAMES;
    private final String name;
    private final FeatureVersion[] featureVersions;

    Features(String name,
             FeatureVersion[] featureVersions) {
        this.name = name;
        this.featureVersions = featureVersions;
    }

    static {
        Features[] enumValues = Features.values();
        FEATURES = Arrays.copyOf(enumValues, enumValues.length);

        PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature ->
                !feature.name.equals(TEST_VERSION.featureName())).collect(Collectors.toList());
        PRODUCTION_FEATURE_NAMES = PRODUCTION_FEATURES.stream().map(feature ->
                feature.name).collect(Collectors.toList());
    }

    public String featureName() {
        return name;
    }

    public FeatureVersion[] featureVersions() {
        return featureVersions;
    }

    public short latestProduction() {
        return defaultValue(MetadataVersion.LATEST_PRODUCTION);
    }

    public short minimumProduction() {
        return featureVersions[0].featureLevel();
    }

    public short latestTesting() {
        return featureVersions[featureVersions.length - 1].featureLevel();
    }

    public SupportedVersionRange supportedVersionRange() {
        return new SupportedVersionRange(
            minimumProduction(),
            latestTesting()
        );
    }

    /**
     * Creates a FeatureVersion from a level.
     *
     * @param level                        the level of the feature
     * @param allowUnstableFeatureVersions whether unstable versions can be used
     * @return the FeatureVersionUtils.FeatureVersion for the feature the enum is based on.
     * @throws IllegalArgumentException    if the feature is not known.
     */
    public FeatureVersion fromFeatureLevel(short level,
                                           boolean allowUnstableFeatureVersions) {
        return Arrays.stream(featureVersions).filter(featureVersion ->
            featureVersion.featureLevel() == level && (allowUnstableFeatureVersions || level <= latestProduction())).findFirst().orElseThrow(
                () -> new IllegalArgumentException("No feature:" + featureName() + " with feature level " + level));
    }

    /**
     * A method to validate the feature can be set. If a given feature relies on another feature, the dependencies should be
     * captured in {@link FeatureVersion#dependencies()}
     * <p>
     * For example, say feature X level x relies on feature Y level y:
     * if feature X >= x then throw an error if feature Y < y.
     *
     * All feature levels above 0 in kraft require metadata.version=4 (IBP_3_3_IV0) in order to write the feature records to the cluster.
     *
     * @param feature                   the feature we are validating
     * @param features                  the feature versions we have (or want to set)
     * @throws IllegalArgumentException if the feature is not valid
     */
    public static void validateVersion(FeatureVersion feature, Map<String, Short> features) {
        Short metadataVersion = features.get(MetadataVersion.FEATURE_NAME);

        if (feature.featureLevel() >= 1 && (metadataVersion == null || metadataVersion < MetadataVersion.IBP_3_3_IV0.featureLevel()))
            throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                    " because it depends on metadata.version=4 (" + MetadataVersion.IBP_3_3_IV0 + ")");

        for (Map.Entry<String, Short> dependency: feature.dependencies().entrySet()) {
            Short featureLevel = features.get(dependency.getKey());

            if (featureLevel == null || featureLevel < dependency.getValue()) {
                throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                        " because it depends on " + dependency.getKey() + " level " + dependency.getValue());
            }
        }
    }

    /**
     * A method to return the default (latest production) level of a feature based on the metadata version provided.
     *
     * Every time a new feature is added, it should create a mapping from metadata version to feature version
     * with {@link FeatureVersion#bootstrapMetadataVersion()}. When the feature version is production ready, the metadata
     * version should be made production ready as well.
     *
     * @param metadataVersion the metadata version we want to use to set the default.
     * @return the default version level given the feature and provided metadata version
     */
    public short defaultValue(MetadataVersion metadataVersion) {
        short level = 0;
        for (Iterator<FeatureVersion> it = Arrays.stream(featureVersions).iterator(); it.hasNext(); ) {
            FeatureVersion feature = it.next();
            if (feature.bootstrapMetadataVersion().isLessThan(metadataVersion) || feature.bootstrapMetadataVersion().equals(metadataVersion))
                level = feature.featureLevel();
            else
                return level;
        }
        return level;
    }

    public static Features featureFromName(String featureName) {
        for (Features features : FEATURES) {
            if (features.name.equals(featureName))
                return features;
        }
        throw new IllegalArgumentException("Feature " + featureName + " not found.");
    }

    /**
     * Utility method to map a list of FeatureVersion to a map of feature name to feature level
     */
    public static Map<String, Short> featureImplsToMap(List<FeatureVersion> features) {
        return features.stream().collect(Collectors.toMap(FeatureVersion::featureName, FeatureVersion::featureLevel));
    }
}
