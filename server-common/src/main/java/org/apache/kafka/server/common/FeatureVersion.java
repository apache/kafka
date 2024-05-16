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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
public enum FeatureVersion {

    /**
     * Features defined. If a feature is included in this list, and marked to be used in production they will also be specified when
     * formatting a cluster via the StorageTool. MetadataVersion is handled separately, so it is not included here.
     *
     * See {@link TestFeatureVersion} as an example. See {@link FeatureVersionUtils.FeatureVersionImpl} when implementing a new feature.
     */
    TEST_VERSION("test.feature.version", TestFeatureVersion.values(), TestFeatureVersion::fromFeatureLevel, false);

    public static final FeatureVersion[] FEATURES;
    public static final List<FeatureVersion> PRODUCTION_FEATURES;
    private final String name;
    private final FeatureVersionUtils.FeatureVersionImpl[] features;
    private final FeatureVersionUtils.CreateMethod createFeatureVersionMethod;
    private final boolean usedInProduction;

    FeatureVersion(String name,
                   FeatureVersionUtils.FeatureVersionImpl[] features,
                   FeatureVersionUtils.CreateMethod createMethod,
                   boolean usedInProduction) {
        this.name = name;
        this.features = features;
        this.createFeatureVersionMethod = createMethod;
        this.usedInProduction = usedInProduction;
    }

    static {
        FeatureVersion[] enumValues = FeatureVersion.values();
        FEATURES = Arrays.copyOf(enumValues, enumValues.length);

        PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature ->
                feature.usedInProduction).collect(Collectors.toList());
    }

    public String featureName() {
        return name;
    }

    public FeatureVersionUtils.FeatureVersionImpl[] features() {
        return features;
    }

    /**
     * Creates a FeatureVersion from a given name and level with the correct feature object underneath.
     *
     * @param level   the level of the feature
     * @returns       the FeatureVersionUtils.FeatureVersionImpl for the feature the enum is based on.
     * @throws        IllegalArgumentException if the feature name is not valid (not implemented for this method)
     */
    public FeatureVersionUtils.FeatureVersionImpl fromFeatureLevel(short level) {
        return createFeatureVersionMethod.fromFeatureLevel(level);
    }

    /**
     * A method to validate the feature can be set. If a given feature relies on another feature, the dependencies should be
     * captured in {@link FeatureVersionUtils.FeatureVersionImpl#dependencies()}
     * <p>
     * For example, say feature X level x relies on feature Y level y:
     * if feature X >= x then throw an error if feature Y < y.
     *
     * All feature levels above 0 require metadata.version=4 (IBP_3_3_IV0) in order to write the feature records to the cluster.
     *
     * @param feature                   the feature we are validating
     * @param metadataVersion           the metadata version we have (or want to set)
     * @param features                  the feature versions (besides MetadataVersion) we have (or want to set)
     * @throws IllegalArgumentException if the feature is not valid
     */
    public static void validateVersion(FeatureVersionUtils.FeatureVersionImpl feature, MetadataVersion metadataVersion, Map<String, Short> features) {
        if (feature.featureLevel() >= 1 && metadataVersion.isLessThan(MetadataVersion.IBP_3_3_IV0))
            throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                    " because it depends on metadata.version=14 (" + MetadataVersion.IBP_3_3_IV0 + ")");

        for (Map.Entry<String, Short> dependency: feature.dependencies().entrySet()) {
            Short featureLevel = features.get(dependency.getKey());

            if (featureLevel == null || featureLevel < dependency.getValue()) {
                throw new IllegalArgumentException(feature.featureName() + " could not be set to " + feature.featureLevel() +
                        " because it depends on " + dependency.getKey() + " level " + dependency.getValue());
            }
        }
    }

    /**
     * A method to return the default version level of a feature. If metadataVersionOpt is not empty, the default is based on
     * the metadataVersion. If not, use the latest production version for the given feature.
     *
     * Every time a new feature is added, it should create a mapping from metadata version to feature version
     * with {@link FeatureVersionUtils.FeatureVersionImpl#metadataVersionMapping()}
     *
     * @param metadataVersionOpt the metadata version we want to use to set the default, or None if the latest production version is desired
     * @return the default version level for the feature and potential metadata version
     */
    public short defaultValue(Optional<MetadataVersion> metadataVersionOpt) {
        MetadataVersion mv = metadataVersionOpt.orElse(MetadataVersion.LATEST_PRODUCTION);
        short level = 0;

        for (Iterator<FeatureVersionUtils.FeatureVersionImpl> it = Arrays.stream(features).iterator(); it.hasNext(); ) {
            FeatureVersionUtils.FeatureVersionImpl feature = it.next();
            if (feature.metadataVersionMapping().isLessThan(mv) || feature.metadataVersionMapping().equals(mv))
                level = feature.featureLevel();
            else
                return level;
        }
        return level;
    }
}
