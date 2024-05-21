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
    TEST_VERSION("test.feature.version", TestFeatureVersion.values(), TestFeatureVersion::fromFeatureLevel, false);

    public static final Features[] FEATURES;
    public static final List<Features> PRODUCTION_FEATURES;
    private final String name;
    private final FeatureVersion[] featureVersions;
    private final CreateMethod createFeatureVersionMethod;
    private final boolean usedInProduction;

    Features(String name,
             FeatureVersion[] featureVersions,
             CreateMethod createMethod,
             boolean usedInProduction) {
        this.name = name;
        this.featureVersions = featureVersions;
        this.createFeatureVersionMethod = createMethod;
        this.usedInProduction = usedInProduction;
    }

    static {
        Features[] enumValues = Features.values();
        FEATURES = Arrays.copyOf(enumValues, enumValues.length);

        PRODUCTION_FEATURES = Arrays.stream(FEATURES).filter(feature ->
                feature.usedInProduction).collect(Collectors.toList());
    }

    public String featureName() {
        return name;
    }

    public FeatureVersion[] features() {
        return featureVersions;
    }

    /**
     * Creates a FeatureVersion from a given name and level with the correct feature object underneath.
     *
     * @param level   the level of the feature
     * @returns       the FeatureVersionUtils.FeatureVersion for the feature the enum is based on.
     * @throws        IllegalArgumentException if the feature name is not valid (not implemented for this method)
     */
    public FeatureVersion fromFeatureLevel(short level) {
        return createFeatureVersionMethod.fromFeatureLevel(level);
    }

    /**
     * A method to validate the feature can be set. If a given feature relies on another feature, the dependencies should be
     * captured in {@link FeatureVersion#dependencies()}
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
    public static void validateVersion(FeatureVersion feature, MetadataVersion metadataVersion, Map<String, Short> features) {
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
     * A method to return the default version level of a feature based on the metadata version provided
     *
     * Every time a new feature is added, it should create a mapping from metadata version to feature version
     * with {@link FeatureVersion#bootstrapMetadataVersion()}
     *
     * @param metadataVersion the metadata version we want to use to set the default
     * @return the default version level for the feature and potential metadata version
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

    /**
     * Utility method to map a list of FeatureVersion to a map of feature name to feature level
     */
    public static Map<String, Short> featureImplsToMap(List<FeatureVersion> features) {
        return features.stream().collect(Collectors.toMap(FeatureVersion::featureName, FeatureVersion::featureLevel));
    }


    interface CreateMethod {
        /**
         * Creates a FeatureVersion from a given feature and level with the correct feature object underneath.
         *
         * @param level   the level of the feature
         * @throws        IllegalArgumentException if the feature name is not valid (not implemented for this method)
         */
        FeatureVersion fromFeatureLevel(short level);
    }
}
