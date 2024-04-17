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
import java.util.List;
import java.util.Optional;

/**
 * This is an interface for the various features implemented for Kafka clusters.
 * KIP-584: Versioning Scheme for Features introduced the idea of various features, but only added one feature -- MetadataVersion.
 * KIP-1022: Formatting and Updating Features allowed for more features to be added. In order to set and update features,
 * they need to be specified via the StorageTool or FeatureCommand tools.
 * <br>
 * Having a unified interface for the features that will use a shared type in the API used to set and update them
 * makes it easier to process these features.
 */
public interface FeatureVersion {

    /**
     * Features currently used in production. If a feature is included in this list, it will also be specified when
     * formatting a cluster via the StorageTool. MetadataVersion is handled separately, so it is not included here.
     *
     * When a feature is added here, make sure it has a mapping in {@link #defaultValue} and {@link #createFeature}.
     * See {@link TestFeatureVersion} as an example.
     */
    List<String> PRODUCTION_FEATURES = Arrays.asList();

    /**
     * Name of the feature. It should be the same across all feature levels of a given feature.
     */
    String featureName();

    /**
     * Level of the feature. Used to indicate which components of the feature will be enabled.
     */
    short featureLevel();

    /**
     * A method to be implemented by each feature. If a given feature relies on another feature, the dependencies should be
     * captured here.
     *
     * For example, say feature X level x relies on feature Y level y:
     * if feature X >= x then throw an error if feature Y < y.
     *
     * All feature levels above 0 require metadata.version=4 (IBP_3_3_IV0) in order to write the feature records to the cluster.
     *
     * @param metadataVersion the metadata version we want to set
     * @param features        the feature versions (besides MetadataVersion) we want to set
     */
    void validateVersion(MetadataVersion metadataVersion, List<FeatureVersion> features);

    /**
     * A method to return the default version of a feature. If metadataVersionOpt is not empty, the default is based on
     * the metadataVersion. If not, use the latest production version for the given feature.
     *
     * Every time a new feature is added, it should create a mapping from metadata version to feature version and include
     * a case here to specify the default.
     *
     * @param feature            the name of the feature requested
     * @param metadataVersionOpt the metadata version we want to use to set the default, or None if the latest production version is desired
     * @throws                   IllegalArgumentException if the feature name is not valid (not implemented for this method)
     * @return                   the FeatureVersion of the feature that contains the feature name and the default level as determined by this method.
     */
    static FeatureVersion defaultValue(String feature, Optional<MetadataVersion> metadataVersionOpt) {
        switch (feature) {
            case MetadataVersion.FEATURE_NAME:
                return createFeature(MetadataVersion.FEATURE_NAME,
                        metadataVersionOpt.orElse(MetadataVersion.LATEST_PRODUCTION).featureLevel());
            case TestFeatureVersion.FEATURE_NAME:
                return createFeature(TestFeatureVersion.FEATURE_NAME,
                        metadataVersionOpt.map(TestFeatureVersion::metadataVersionMapping).orElse(TestFeatureVersion.PRODUCTION_VERSION).featureLevel());
            default:
                throw new IllegalArgumentException(feature + " is not a valid feature version.");
        }
    }

    /**
     * Creates a FeatureVersion from a given name and level with the correct feature object underneath.
     *
     * @param feature the name of the feature requested
     * @param level   the level of the feature
     * @throws        IllegalArgumentException if the feature name is not valid (not implemented for this method)
     */
    static FeatureVersion createFeature(String feature, short level) {
        switch (feature) {
            case MetadataVersion.FEATURE_NAME:
                return MetadataVersion.fromFeatureLevel(level);
            case TestFeatureVersion.FEATURE_NAME:
                return TestFeatureVersion.fromFeatureLevel(level);
            default:
                throw new IllegalArgumentException(feature + " is not a valid feature version.");
        }
    }

}
