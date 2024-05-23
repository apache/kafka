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

import java.util.Map;

public interface FeatureVersion {

    /**
     * The level of the feature. 0 means the feature is fully disabled, so this value should be positive.
     */
    short featureLevel();

    /**
     * The name of the feature.
     */
    String featureName();

    /**
     * When bootstrapping using only a metadata version, a reasonable default for all other features is chosen based
     * on the metadata version. This method returns the minimum metadata version that sets this feature version as default.
     * This should be defined as the metadata version released when this feature version becomes production ready.
     * If feature level X is released when metadata version Y is the latest, a new MV should be released and this method should return Y + 1
     * (Ie, if the current production MV is 17 when a feature version is released, its mapping should be to newly released MV 18)
     *
     * NOTE: The feature can be used without setting this metadata version (unless specified by {@link FeatureVersion#dependencies})
     * This method is simply for choosing the default when only metadata version is specified.
     */
    MetadataVersion bootstrapMetadataVersion();

    /**
     * A mapping from feature to level for all features that this feature depends on. If this feature doesn't
     * depend on any others, return an empty map.
     * For example, say feature X level x relies on feature Y level y:
     * feature (X level x).dependencies() will return (Y -> y)
     */
    Map<String, Short> dependencies();
}
