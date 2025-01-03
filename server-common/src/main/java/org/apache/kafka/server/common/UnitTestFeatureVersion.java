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

import java.util.Collections;
import java.util.Map;

/**
 * Test versions only used for unit test FeatureTest.java.
 */
public class UnitTestFeatureVersion {
    /**
     * The feature is used for testing latest production is not one of the feature versions.
     */
    public enum FV0 implements FeatureVersion {
        UT_FV0_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV0_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap());

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.0";

        public static final FV0 LATEST_PRODUCTION = UT_FV0_1;

        FV0(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test latest production lags behind the default value.
     */
    public enum FV1 implements FeatureVersion {
        UT_FV1_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV1_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap());

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.1";

        public static final FV1 LATEST_PRODUCTION = UT_FV1_0;

        FV1(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test the dependency of the latest production that is not yet production ready.
     */
    public enum FV2 implements FeatureVersion {
        UT_FV2_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV2_1(1, MetadataVersion.IBP_3_7_IV0, Collections.emptyMap());

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.2";

        public static final FV2 LATEST_PRODUCTION = UT_FV2_0;

        FV2(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test the dependency of the latest production that is not yet production ready.
     */
    public enum FV3 implements FeatureVersion {
        UT_FV3_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV3_1(1, MetadataVersion.IBP_3_7_IV0, Collections.singletonMap(FV2.FEATURE_NAME, (short) 1));

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.3";

        public static final FV3 LATEST_PRODUCTION = UT_FV3_1;

        FV3(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test the dependency of the default value that is not yet default ready.
     */
    public enum FV4 implements FeatureVersion {
        UT_FV4_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV4_1(1, MetadataVersion.latestTesting(), Collections.emptyMap());

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.4";

        public static final FV4 LATEST_PRODUCTION = UT_FV4_1;

        FV4(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test the dependency of the default value that is not yet default ready.
     */
    public enum FV5 implements FeatureVersion {
        UT_FV5_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV5_1(1, MetadataVersion.IBP_3_7_IV0, Collections.singletonMap(FV4.FEATURE_NAME, (short) 1));

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.5";

        public static final FV5 LATEST_PRODUCTION = UT_FV5_1;

        FV5(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test the latest production has MV dependency that is not yet production ready.
     */
    public enum FV6 implements FeatureVersion {
        UT_FV6_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.emptyMap()),
        UT_FV6_1(1, MetadataVersion.latestTesting(), Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.latestTesting().featureLevel()));

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.6";

        public static final FV6 LATEST_PRODUCTION = UT_FV6_1;

        FV6(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }

    /**
     * The feature is used to test the default value has MV dependency that is behind the bootstrap MV.
     */
    public enum FV7 implements FeatureVersion {
        UT_FV7_0(0, MetadataVersion.MINIMUM_KRAFT_VERSION, Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_7_IV0.featureLevel())),
        UT_FV7_1(1, MetadataVersion.IBP_3_8_IV0, Collections.singletonMap(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_3_8_IV0.featureLevel()));

        private final short featureLevel;
        private final MetadataVersion bootstrapMetadataVersion;
        private final Map<String, Short> dependencies;

        public static final String FEATURE_NAME = "unit.test.feature.version.7";

        public static final FV7 LATEST_PRODUCTION = UT_FV7_1;

        FV7(int featureLevel, MetadataVersion bootstrapMetadataVersion, Map<String, Short> dependencies) {
            this.featureLevel = (short) featureLevel;
            this.bootstrapMetadataVersion = bootstrapMetadataVersion;
            this.dependencies = dependencies;
        }

        @Override
        public short featureLevel() {
            return featureLevel;
        }

        @Override
        public String featureName() {
            return FEATURE_NAME;
        }

        @Override
        public MetadataVersion bootstrapMetadataVersion() {
            return bootstrapMetadataVersion;
        }

        @Override
        public Map<String, Short> dependencies() {
            return dependencies;
        }
    }
}
