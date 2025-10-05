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
package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * Maintains node api versions for access outside of NetworkClient (which is where the information is derived).
 * The pattern is akin to the use of {@link Metadata} for topic metadata.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public class ApiVersions {

    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();

    // The maximum finalized feature epoch of all the node api versions.
    private long maxFinalizedFeaturesEpoch = -1;
    private Map<String, Short> finalizedFeatures;

    public static class FinalizedFeaturesInfo {
        public final long finalizedFeaturesEpoch;
        public final Map<String, Short> finalizedFeatures;
        FinalizedFeaturesInfo(long finalizedFeaturesEpoch, Map<String, Short> finalizedFeatures) {
            this.finalizedFeaturesEpoch = finalizedFeaturesEpoch;
            this.finalizedFeatures = finalizedFeatures;
        }
    }

    public synchronized void update(String nodeId, NodeApiVersions nodeApiVersions) {
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        if (maxFinalizedFeaturesEpoch < nodeApiVersions.finalizedFeaturesEpoch()) {
            this.maxFinalizedFeaturesEpoch = nodeApiVersions.finalizedFeaturesEpoch();
            this.finalizedFeatures = nodeApiVersions.finalizedFeatures();
        }
    }

    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
    }

    public synchronized NodeApiVersions get(String nodeId) {
        return this.nodeApiVersions.get(nodeId);
    }

    public synchronized long getMaxFinalizedFeaturesEpoch() {
        return maxFinalizedFeaturesEpoch;
    }

    public synchronized FinalizedFeaturesInfo getFinalizedFeaturesInfo() {
        return new FinalizedFeaturesInfo(maxFinalizedFeaturesEpoch, finalizedFeatures);
    }

}
