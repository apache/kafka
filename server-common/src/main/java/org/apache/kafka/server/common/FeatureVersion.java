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
     * The MetadataVersion that corresponds to this feature. Setting this MV is not required to set the feature version,
     * but this MV is marked production ready if and only if this feature version is production ready.
     *
     * When bootstrapping, we can find the latest production feature version by finding the highest bootstrapMetadataVersion that is less than or equal
     * to the given MetadataVersion ({@link MetadataVersion#LATEST_PRODUCTION} by default). Setting a feature explicitly
     * will skip this mapping and allow setting the feature independently as long as it is a supported version.
     *
     * If feature level X is created when MetadataVersion Y is the latest production version, create a new MV Y + 1. When the feature version becomes
     * production ready, set MetadataVersion Y + 1 as production ready.
     * (Ie, if the current production MV is 17 when a feature version is created, create MV 18 and mark it as production ready when the feature version is production ready.)
     *
     * NOTE: The feature can be used without setting this metadata version. If we want to mark a dependency, do so in {@link FeatureVersion#dependencies}
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
