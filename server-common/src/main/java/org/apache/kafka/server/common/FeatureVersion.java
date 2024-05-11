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
     * Features currently used in production. If a feature is included in this list, it will also be specified when
     * formatting a cluster via the StorageTool. MetadataVersion is handled separately, so it is not included here.
     *
     * See {@link TestFeatureVersion} as an example.
     */
    TEST_VERSION("test.feature.version", TestFeatureVersion::defaultValue, TestFeatureVersion::fromFeatureLevel, false);

    public static final FeatureVersion[] FEATURES;
    public static final List<FeatureVersion> PRODUCTION_FEATURES;
    private final String name;
    private final FeatureVersionUtils.DefaultValueMethod defaultValueMethod;
    private final FeatureVersionUtils.CreateMethod createFeatureVersionMethod;
    private final boolean usedInProduction;

    FeatureVersion(String name,
                   FeatureVersionUtils.DefaultValueMethod defaultValueMethod,
                   FeatureVersionUtils.CreateMethod createMethod,
                   boolean usedInProduction) {
        this.name = name;
        this.defaultValueMethod = defaultValueMethod;
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

    public FeatureVersionUtils.FeatureVersionImpl fromFeatureLevel(short level) {
        return createFeatureVersionMethod.fromFeatureLevel(level);
    }

    public short defaultValue(Optional<MetadataVersion> metadataVersionOpt) {
        return defaultValueMethod.defaultValue(metadataVersionOpt.orElse(MetadataVersion.LATEST_PRODUCTION));
    }
}
