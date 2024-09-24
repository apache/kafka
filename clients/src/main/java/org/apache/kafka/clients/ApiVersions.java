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

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Maintains node api versions for access outside of NetworkClient (which is where the information is derived).
 * The pattern is akin to the use of {@link Metadata} for topic metadata.
 *
 * NOTE: This class is intended for INTERNAL usage only within Kafka.
 */
public class ApiVersions {

    private final Map<String, NodeApiVersions> nodeApiVersions = new HashMap<>();
    private byte maxUsableProduceMagic = RecordBatch.CURRENT_MAGIC_VALUE;
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
        if (maxFinalizedFeaturesEpoch > nodeApiVersions.finalizedFeaturesEpoch()) {
            return;
        }
        this.nodeApiVersions.put(nodeId, nodeApiVersions);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
        this.maxFinalizedFeaturesEpoch = nodeApiVersions.finalizedFeaturesEpoch();
        this.finalizedFeatures = nodeApiVersions.finalizedFeatures();
    }

    public synchronized void remove(String nodeId) {
        this.nodeApiVersions.remove(nodeId);
        this.maxUsableProduceMagic = computeMaxUsableProduceMagic();
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

    private byte computeMaxUsableProduceMagic() {
        // use a magic version which is supported by all brokers to reduce the chance that
        // we will need to convert the messages when they are ready to be sent.
        Optional<Byte> knownBrokerNodesMinRequiredMagicForProduce = this.nodeApiVersions.values().stream()
            .filter(versions -> versions.apiVersion(ApiKeys.PRODUCE) != null) // filter out Raft controller nodes
            .map(versions -> ProduceRequest.requiredMagicForVersion(versions.latestUsableVersion(ApiKeys.PRODUCE)))
            .min(Byte::compare);
        return (byte) Math.min(RecordBatch.CURRENT_MAGIC_VALUE,
            knownBrokerNodesMinRequiredMagicForProduce.orElse(RecordBatch.CURRENT_MAGIC_VALUE));
    }

    public synchronized byte maxUsableProduceMagic() {
        return maxUsableProduceMagic;
    }

}
