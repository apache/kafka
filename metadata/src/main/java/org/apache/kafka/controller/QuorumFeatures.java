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
import org.apache.kafka.common.Node;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A holder class of the local node's supported feature flags as well as the ApiVersions of other nodes.
 */
public class QuorumFeatures {
    private final int nodeId;
    private final ApiVersions apiVersions;
    private final Map<String, VersionRange> supportedFeatures;
    private final List<Integer> quorumNodeIds;

    public QuorumFeatures(int nodeId,
                          ApiVersions apiVersions,
                          Map<String, VersionRange> supportedFeatures,
                          List<Integer> quorumNodeIds) {
        this.nodeId = nodeId;
        this.apiVersions = apiVersions;
        this.supportedFeatures = Collections.unmodifiableMap(supportedFeatures);
        this.quorumNodeIds = Collections.unmodifiableList(quorumNodeIds);
    }

    public static QuorumFeatures create(int nodeId,
                                        ApiVersions apiVersions,
                                        Map<String, VersionRange> supportedFeatures,
                                        Collection<Node> quorumNodes) {
        List<Integer> nodeIds = quorumNodes.stream().map(Node::id).collect(Collectors.toList());
        return new QuorumFeatures(nodeId, apiVersions, supportedFeatures, nodeIds);
    }

    public static Map<String, VersionRange> defaultFeatureMap() {
        Map<String, VersionRange> features = new HashMap<>(1);
        features.put(MetadataVersion.FEATURE_NAME, VersionRange.of(MetadataVersion.IBP_3_0_IV0.featureLevel(), MetadataVersion.latest().featureLevel()));
        return features;
    }

    Optional<VersionRange> quorumSupportedFeature(String featureName) {
        List<VersionRange> supportedVersions = quorumNodeIds.stream()
            .filter(node -> node != nodeId)
            .map(node -> apiVersions.get(Integer.toString(node)))
            .filter(Objects::nonNull)
            .map(apiVersion -> apiVersion.supportedFeatures().get(featureName))
            .filter(Objects::nonNull)
            .map(supportedVersionRange -> VersionRange.of(supportedVersionRange.min(), supportedVersionRange.max()))
            .collect(Collectors.toList());

        localSupportedFeature(featureName).ifPresent(supportedVersions::add);

        if (supportedVersions.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(VersionRange.of(
                (short) supportedVersions.stream().mapToInt(VersionRange::min).max().getAsInt(),
                (short) supportedVersions.stream().mapToInt(VersionRange::max).min().getAsInt()
            ));
        }
    }


    Optional<VersionRange> localSupportedFeature(String featureName) {
        return Optional.ofNullable(supportedFeatures.get(featureName));
    }
}
