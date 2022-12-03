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

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.feature.SupportedVersionRange;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A holder class of the local node's supported feature flags as well as the ApiVersions of other nodes.
 */
public class QuorumFeatures {
    private static final VersionRange DISABLED = VersionRange.of(0, 0);

    private static final Logger log = LoggerFactory.getLogger(QuorumFeatures.class);

    private final int nodeId;
    private final ApiVersions apiVersions;
    private final Map<String, VersionRange> localSupportedFeatures;
    private final List<Integer> quorumNodeIds;

    QuorumFeatures(
        int nodeId,
        ApiVersions apiVersions,
        Map<String, VersionRange> localSupportedFeatures,
        List<Integer> quorumNodeIds
    ) {
        this.nodeId = nodeId;
        this.apiVersions = apiVersions;
        this.localSupportedFeatures = Collections.unmodifiableMap(localSupportedFeatures);
        this.quorumNodeIds = Collections.unmodifiableList(quorumNodeIds);
    }

    public static QuorumFeatures create(
        int nodeId,
        ApiVersions apiVersions,
        Map<String, VersionRange> localSupportedFeatures,
        Collection<Node> quorumNodes
    ) {
        List<Integer> nodeIds = quorumNodes.stream().map(Node::id).collect(Collectors.toList());
        return new QuorumFeatures(nodeId, apiVersions, localSupportedFeatures, nodeIds);
    }

    public static Map<String, VersionRange> defaultFeatureMap() {
        Map<String, VersionRange> features = new HashMap<>(1);
        features.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
            MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
            MetadataVersion.latest().featureLevel()));
        return features;
    }

    /**
     * Return the reason a specific feature level is not supported, or Optional.empty if it is supported.
     *
     * @param featureName   The feature name.
     * @param level         The feature level.
     * @return              The reason why the feature level is not supported, or Optional.empty if it is supported.
     */
    public Optional<String> reasonNotSupported(String featureName, short level) {
        VersionRange localRange = localSupportedFeatures.getOrDefault(featureName, DISABLED);
        if (!localRange.contains(level)) {
            if (localRange.equals(DISABLED)) {
                return Optional.of("Local controller " + nodeId + " does not support this feature.");
            } else {
                return Optional.of("Local controller " + nodeId + " only supports versions " + localRange);
            }
        }
        List<String> missing = new ArrayList<>();
        for (int id : quorumNodeIds) {
            if (nodeId == id) {
                continue; // We get the local node's features from localSupportedFeatures.
            }
            NodeApiVersions nodeVersions = apiVersions.get(Integer.toString(id));
            if (nodeVersions == null) {
                missing.add(Integer.toString(id));
                continue;
            }
            SupportedVersionRange supportedRange = nodeVersions.supportedFeatures().get(featureName);
            VersionRange range = supportedRange == null ? DISABLED :
                    VersionRange.of(supportedRange.min(), supportedRange.max());
            if (!range.contains(level)) {
                if (range.equals(DISABLED)) {
                    return Optional.of("Controller " + id + " does not support this feature.");
                } else {
                    return Optional.of("Controller " + id + " only supports versions " + range);
                }
            }
        }
        if (!missing.isEmpty()) {
            log.info("Unable to get feature level information for controller(s): " + String.join(", ", missing));
        }
        return Optional.empty();
    }

    VersionRange localSupportedFeature(String featureName) {
        return localSupportedFeatures.getOrDefault(featureName, DISABLED);
    }

    boolean isControllerId(int nodeId) {
        return quorumNodeIds.contains(nodeId);
    }
}
