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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface FeatureVersion {

    /**
     * The level of the feature. 0 means the feature is disabled.
     */
    short featureLevel();

    /**
     * The name of the feature.
     */
    String featureName();

    /**
     * The next metadata version to be released when the feature became production ready.
     * (Ie, if the current production MV is 17 when a feature is released, its mapping should be to MV 18)
     */
    MetadataVersion metadataVersionMapping();

    /**
     * A mapping from feature to level for all features that this feature depends on. If this feature doesn't
     * depend on any others, return an empty map.
     * For example, say feature X level x relies on feature Y level y:
     * feature (X level x).dependencies() will return (Y -> y)
     */
    Map<String, Short> dependencies();

    /**
     * Utility method to map a list of FeatureVersion to a map of feature name to feature level
     */
    static Map<String, Short> featureImplsToMap(List<FeatureVersion> features) {
        return features.stream().collect(Collectors.toMap(FeatureVersion::featureName, FeatureVersion::featureLevel));
    }
}
