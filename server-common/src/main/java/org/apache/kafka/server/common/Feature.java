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

import static org.apache.kafka.server.common.UnitTestFeatureVersion.FV0.UT_FV0_0;

/**
 * This is enum for the various features implemented for Kafka clusters.
 * KIP-584: Versioning Scheme for Features introduced the idea of various features, but only added one feature -- MetadataVersion.
 * KIP-1022: Formatting and Updating Features allowed for more features to be added. In order to set and update features,
 * they need to be specified via the StorageTool or FeatureCommand tools.
 * <br>
 * Having a unified enum for the features that will use a shared type in the API used to set and update them
 * makes it easier to process these features.
 */
public enum Feature {

    /**
     * Features defined. If a feature is included in this list, and marked to be used in production they will also be specified when
     * formatting a cluster via the StorageTool. MetadataVersion is handled separately, so it is not included here.
     *
     * See {@link TestFeatureVersion} as an example. See {@link FeatureVersion} when implementing a new feature.
     */
    KRAFT_VERSION(KRaftVersion.FEATURE_NAME, KRaftVersion.values(), KRaftVersion.LATEST_PRODUCTION),
    TRANSACTION_VERSION(TransactionVersion.FEATURE_NAME, TransactionVersion.values(), TransactionVersion.LATEST_PRODUCTION),
    GROUP_VERSION(GroupVersion.FEATURE_NAME, GroupVersion.values(), GroupVersion.LATEST_PRODUCTION),
    ELIGIBLE_LEADER_REPLICAS_VERSION(EligibleLeaderReplicasVersion.FEATURE_NAME, EligibleLeaderReplicasVersion.values(), EligibleLeaderReplicasVersion.LATEST_PRODUCTION),

    /**
     * Features defined only for unit tests and are not used in production.
     */
    TEST_VERSION(TestFeatureVersion.FEATURE_NAME, TestFeatureVersion.values(), TestFeatureVersion.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_0(UnitTestFeatureVersion.FV0.FEATURE_NAME, new FeatureVersion[]{UT_FV0_0}, UnitTestFeatureVersion.FV0.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_1(UnitTestFeatureVersion.FV1.FEATURE_NAME, UnitTestFeatureVersion.FV1.values(), UnitTestFeatureVersion.FV1.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_2(UnitTestFeatureVersion.FV2.FEATURE_NAME, UnitTestFeatureVersion.FV2.values(), UnitTestFeatureVersion.FV2.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_3(UnitTestFeatureVersion.FV3.FEATURE_NAME, UnitTestFeatureVersion.FV3.values(), UnitTestFeatureVersion.FV3.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_4(UnitTestFeatureVersion.FV4.FEATURE_NAME, UnitTestFeatureVersion.FV4.values(), UnitTestFeatureVersion.FV4.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_5(UnitTestFeatureVersion.FV5.FEATURE_NAME, UnitTestFeatureVersion.FV5.values(), UnitTestFeatureVersion.FV5.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_6(UnitTestFeatureVersion.FV6.FEATURE_NAME, UnitTestFeatureVersion.FV6.values(), UnitTestFeatureVersion.FV6.LATEST_PRODUCTION),
    UNIT_TEST_VERSION_7(UnitTestFeatureVersion.FV7.FEATURE_NAME, UnitTestFeatureVersion.FV7.values(), UnitTestFeatureVersion.FV7.LATEST_PRODUCTION);

    public static final Feature[] FEATURES;

    // The list of features that are not unit test features.
    public static final List<Feature> TEST_AND_PRODUCTION_FEATURES;

    public static final List<Feature> PRODUCTION_FEATURES;

    public static final List<String> PRODUCTION_FEATURE_NAMES;
    private final String name;
    private final FeatureVersion[] featureVersions;

    // The latest production version of the feature, owned and updated by the feature owner
    // in the respective feature definition. The value should not be smaller than the default
    // value calculated with {@link #defaultValue(MetadataVersion)}.
    public final FeatureVersion latestProduction;

    Feature(String name,
            FeatureVersion[] featureVersions,
            FeatureVersion latestProduction) {
        this.name = name;
        this.featureVersions = featureVersions;
        this.latestProduction = latestProduction;
    }

    static {
        Feature[] enumValues = Feature.values();
        FEATURES = Arrays.copyOf(enumValues, enumValues.length);

        TEST_AND_PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature ->
            !feature.name.startsWith("unit." + TestFeatureVersion.FEATURE_NAME)
        ).collect(Collectors.toList());

        PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature ->
            !feature.name.equals(TEST_VERSION.featureName()) &&
            !feature.name.startsWith("unit." + TestFeatureVersion.FEATURE_NAME)
        ).collect(Collectors.toList());
        PRODUCTION_FEATURE_NAMES = PRODUCTION_FEATURES.stream().map(feature ->
                feature.name).collect(Collectors.toList());

        validateDefaultValueAndLatestProductionValue(TEST_VERSION);
        for (Feature feature : PRODUCTION_FEATURES) {
            validateDefaultValueAndLatestProductionValue(feature);
        }
    }

    public String featureName() {
        return name;
    }

    public FeatureVersion[] featureVersions() {
        return featureVersions;
    }

    public short latestProduction() {
        return latestProduction.featureLevel();
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
     * A method to return the default (latest production) version of a feature based on the metadata version provided.
     *
     * Every time a new feature is added, it should create a mapping from metadata version to feature version
     * with {@link FeatureVersion#bootstrapMetadataVersion()}. The feature version should be marked as production ready
     * before the metadata version is made production ready.
     *
     * @param metadataVersion the metadata version we want to use to set the default.
     * @return the default version given the feature and provided metadata version
     */
    public FeatureVersion defaultVersion(MetadataVersion metadataVersion) {
        FeatureVersion version = featureVersions[0];
        for (Iterator<FeatureVersion> it = Arrays.stream(featureVersions).iterator(); it.hasNext(); ) {
            FeatureVersion feature = it.next();
            if (feature.bootstrapMetadataVersion().isLessThan(metadataVersion) || feature.bootstrapMetadataVersion().equals(metadataVersion))
                version = feature;
            else
                return version;
        }
        return version;
    }

    public short defaultLevel(MetadataVersion metadataVersion) {
        return defaultVersion(metadataVersion).featureLevel();
    }

    public static Feature featureFromName(String featureName) {
        for (Feature feature : FEATURES) {
            if (feature.name.equals(featureName))
                return feature;
        }
        throw new IllegalArgumentException("Feature " + featureName + " not found.");
    }

    public boolean isProductionReady(short featureVersion) {
        return featureVersion <= latestProduction();
    }

    public boolean hasFeatureVersion(FeatureVersion featureVersion) {
        for (FeatureVersion v : featureVersions()) {
            if (v == featureVersion) {
                return true;
            }
        }
        return false;
    }

    /**
     * The method ensures that the following statements are met:
     * 1. The latest production value is one of the feature values.
     * 2. The latest production value >= the default value.
     * 3. The dependencies of the latest production value <= their latest production values.
     * 4. The dependencies of all default values <= their default values.
     * 5. If the latest production depends on MetadataVersion, the value should be <= MetadataVersion.LATEST_PRODUCTION.
     * 6. If any default value depends on MetadataVersion, the value should be <= the default value bootstrap MV.
     *
     * Suppose we have feature X as the feature being validated.
     * Invalid examples:
     *     - The feature X has default version = XV_10 (dependency = {}), latest production = XV_5 (dependency = {})
     *       (Violating rule 2. The latest production value XV_5 is smaller than the default value)
     *     - The feature X has latest production = XV_11 (dependency = {Y: YV_4})
     *       The feature Y has latest production = YV_3 (dependency = {})
     *       (Violating rule 3. For latest production XV_11, Y's latest production YV_3 is smaller than the dependency value YV_4)
     *     - The feature X has default version = XV_10 (dependency = {Y: YV_4})
     *       The feature Y has default version = YV_3 (dependency = {})
     *       (Violating rule 4. For default version XV_10, Y's default value YV_3 is smaller than the dependency value YV_4)
     *     - The feature X has latest production = XV_11 (dependency = {MetadataVersion: IBP_4_0_IV1}), MetadataVersion.LATEST_PRODUCTION is IBP_4_0_IV0
     *       (Violating rule 5. The dependency MV IBP_4_0_IV1 is behind MV latest production IBP_4_0_IV0)
     *     - The feature X has default version = XV_10 (dependency = {MetadataVersion: IBP_4_0_IV1}) and bootstrap MV = IBP_4_0_IV0
     *       (Violating rule 6. When MV latest production is IBP_4_0_IV0, feature X will be set to XV_10 by default whereas it depends on MV IBP_4_0_IV1)
     * Valid examples:
     *     - The feature X has default version = XV_10 (dependency = {}), latest production = XV_10 (dependency = {})
     *     - The feature X has default version = XV_10 (dependency = {Y: YV_3}), latest production = XV_11 (dependency = {Y: YV_4})
     *       The feature Y has default version = YV_3 (dependency = {}), latest production = YV_4 (dependency = {})
     *     - The feature X has default version = XV_10 (dependency = {MetadataVersion: IBP_4_0_IV0}), boostrap MV = IBP_4_0_IV0,
     *                       latest production = XV_11 (dependency = {MetadataVersion: IBP_4_0_IV1}), MV latest production = IBP_4_0_IV1
     *
     * @param feature the feature to validate.
     * @return true if the feature is valid, false otherwise.
     * @throws IllegalArgumentException if the feature violates any of the rules thus is not valid.
     */
    public static void validateDefaultValueAndLatestProductionValue(
        Feature feature
    ) throws IllegalArgumentException {
        FeatureVersion defaultVersion = feature.defaultVersion(MetadataVersion.LATEST_PRODUCTION);
        FeatureVersion latestProduction = feature.latestProduction;

        if (!feature.hasFeatureVersion(latestProduction)) {
            throw new IllegalArgumentException(String.format("Feature %s has latest production version %s " +
                    "which is not one of its feature versions.", feature.name(), latestProduction));
        }

        if (latestProduction.featureLevel() < defaultVersion.featureLevel()) {
            throw new IllegalArgumentException(String.format("Feature %s has latest production value %s " +
                    "smaller than its default version %s with latest production MV.",
                feature.name(), latestProduction, defaultVersion));
        }

        for (Map.Entry<String, Short> dependency: latestProduction.dependencies().entrySet()) {
            String dependencyFeatureName = dependency.getKey();
            if (!dependencyFeatureName.equals(MetadataVersion.FEATURE_NAME)) {
                Feature dependencyFeature = featureFromName(dependencyFeatureName);
                if (!dependencyFeature.isProductionReady(dependency.getValue())) {
                    throw new IllegalArgumentException(String.format("Feature %s has latest production FeatureVersion %s " +
                            "with dependency %s that is not production ready. (%s latest production: %s)",
                        feature.name(), latestProduction, dependencyFeature.fromFeatureLevel(dependency.getValue(), true),
                        dependencyFeature, dependencyFeature.latestProduction));
                }
            } else {
                if (dependency.getValue() > MetadataVersion.LATEST_PRODUCTION.featureLevel()) {
                    throw new IllegalArgumentException(String.format("Feature %s has latest production FeatureVersion %s " +
                            "with MV dependency %s that is not production ready. (MV latest production: %s)",
                        feature.name(), latestProduction, MetadataVersion.fromFeatureLevel(dependency.getValue()),
                        MetadataVersion.LATEST_PRODUCTION));
                }
            }
        }

        for (MetadataVersion metadataVersion: MetadataVersion.values()) {
            // Only checking the kraft metadata versions.
            if (metadataVersion.compareTo(MetadataVersion.MINIMUM_KRAFT_VERSION) < 0) {
                continue;
            }

            defaultVersion = feature.defaultVersion(metadataVersion);
            for (Map.Entry<String, Short> dependency: defaultVersion.dependencies().entrySet()) {
                String dependencyFeatureName = dependency.getKey();
                if (!dependencyFeatureName.equals(MetadataVersion.FEATURE_NAME)) {
                    Feature dependencyFeature = featureFromName(dependencyFeatureName);
                    if (dependency.getValue() > dependencyFeature.defaultLevel(metadataVersion)) {
                        throw new IllegalArgumentException(String.format("Feature %s has default FeatureVersion %s " +
                                "when MV=%s with dependency %s that is behind its default version %s.",
                            feature.name(), defaultVersion, metadataVersion,
                            dependencyFeature.fromFeatureLevel(dependency.getValue(), true),
                            dependencyFeature.defaultVersion(metadataVersion)));
                    }
                } else {
                    if (dependency.getValue() > defaultVersion.bootstrapMetadataVersion().featureLevel()) {
                        throw new IllegalArgumentException(String.format("Feature %s has default FeatureVersion %s " +
                                "when MV=%s with MV dependency %s that is behind its bootstrap MV %s.",
                            feature.name(), defaultVersion, metadataVersion,
                            MetadataVersion.fromFeatureLevel(dependency.getValue()),
                            defaultVersion.bootstrapMetadataVersion()));
                    }
                }
            }
        }
    }
}
