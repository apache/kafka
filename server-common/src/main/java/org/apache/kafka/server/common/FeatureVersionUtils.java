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

public interface FeatureVersionUtils {

    public interface ValidateMethod {
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
        void validateVersion(short featureLevel, MetadataVersion metadataVersion, Map<String, Short> features);
    }

    public interface DefaultValueMethod {
        /**
         * A method to return the default version level of a feature. If metadataVersionOpt is not empty, the default is based on
         * the metadataVersion. If not, use the latest production version for the given feature.
         * <p>
         * Every time a new feature is added, it should create a mapping from metadata version to feature version and include
         * a case here to specify the default.
         *
         * @param metadataVersionOpt the metadata version we want to use to set the default, or None if the latest production version is desired
         * @return the default version level for the feature and potential metadata version
         * @throws IllegalArgumentException if the feature name is not valid (not implemented for this method)
         */
        short defaultValue(MetadataVersion metadataVersionOpt);
    }
}
