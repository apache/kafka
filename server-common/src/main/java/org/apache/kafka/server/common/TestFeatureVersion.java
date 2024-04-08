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

public enum TestFeatureVersion implements FeatureVersion {

    TEST_0(0),
    TEST_1(1),
    TEST_2(2);

    private short featureLevel;

    public static final String FEATURE_NAME = "test.feature.version";
    public static final TestFeatureVersion PRODUCTION_VERSION = TEST_1;

    TestFeatureVersion(int featureLevel) {
        this.featureLevel = (short) featureLevel;
    }

    public short featureLevel() {
        return featureLevel;
    }

    public String featureName() {
        return FEATURE_NAME;
    }

    public void validateVersion(MetadataVersion metadataVersion, List<FeatureVersion> features) {
        // version 1 depends on metadata.version 3.3-IVO
        if (featureLevel >= 1 && metadataVersion.isLessThan(MetadataVersion.IBP_3_3_IV0))
            throw new IllegalArgumentException(FEATURE_NAME + " could not be set to " + featureLevel +
                    " because it depends on metadata.version=14 (" + MetadataVersion.IBP_3_3_IV0 + ")");
    }

    public static TestFeatureVersion metadataVersionMapping(MetadataVersion metadataVersion) {
        if (metadataVersion.isLessThan(MetadataVersion.IBP_3_8_IV0)) {
            return TEST_0;
        } else {
            return TEST_1;
        }
    }

    public static TestFeatureVersion fromFeatureLevel(short level) {
        for (TestFeatureVersion testFeatureVersion: TestFeatureVersion.values()) {
            if (testFeatureVersion.featureLevel() == level) {
                return testFeatureVersion;
            }
        }
        throw new IllegalArgumentException("No MetadataVersion with feature level " + level);
    }
}
