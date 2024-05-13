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

public interface FeatureVersionUtils {

    interface FeatureVersionImpl {
        short featureLevel();

        String featureName();

        MetadataVersion metadataVersionMapping();

        Map<String, Short> dependencies();

        static Map<String, Short> featureImplsToMap(List<FeatureVersionImpl> features) {
            return features.stream().collect(Collectors.toMap(FeatureVersionImpl::featureName, FeatureVersionImpl::featureLevel));
        }
    }

    interface CreateMethod {
        /**
         * Creates a FeatureVersion from a given feature and level with the correct feature object underneath.
         *
         * @param level   the level of the feature
         * @throws        IllegalArgumentException if the feature name is not valid (not implemented for this method)
         */
        FeatureVersionImpl fromFeatureLevel(short level);
    }

    interface DefaultValueMethod {
        /**
         * A method to return the default version level of a feature. If metadataVersionOpt is not empty, the default is based on
         * the metadataVersion. If not, use the latest production version for the given feature.
         * <p>
         * Every time a new feature is added, it should create a mapping from metadata version to feature version.
         *
         * @param metadataVersionOpt the metadata version we want to use to set the default, or None if the latest production version is desired
         * @return the default version level for the feature and potential metadata version
         */
        short defaultValue(MetadataVersion metadataVersionOpt);
    }
}
