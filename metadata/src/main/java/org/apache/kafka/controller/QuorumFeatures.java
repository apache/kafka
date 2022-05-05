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

package org.apache.kafka.controller;

import org.apache.kafka.metadata.VersionRange;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * A holder class of the local node's supported feature flags.
 */
public class QuorumFeatures {
    private final int nodeId;
    private final Map<String, VersionRange> supportedFeatures;

    QuorumFeatures(int nodeId,
                          Map<String, VersionRange> supportedFeatures) {
        this.nodeId = nodeId;
        this.supportedFeatures = Collections.unmodifiableMap(supportedFeatures);
    }

    public static QuorumFeatures create(int nodeId,
                                        Map<String, VersionRange> supportedFeatures) {
        return new QuorumFeatures(nodeId, supportedFeatures);
    }

    public static Map<String, VersionRange> defaultFeatureMap() {
        return Collections.emptyMap();
    }

    Optional<VersionRange> localSupportedFeature(String featureName) {
        return Optional.ofNullable(supportedFeatures.get(featureName));
    }
}
