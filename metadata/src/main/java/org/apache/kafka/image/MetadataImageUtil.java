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

package org.apache.kafka.image;

public class MetadataImageUtil {
    /**
     * Given a MetadataImage and MetadataDelta, produce a succinct human-readable description.
     */
    public static String toSummaryString(MetadataImage image, MetadataDelta delta) {
        StringBuilder sb = new StringBuilder();
        sb.append("MetadataSummary(");
        sb.append("offset=");
        sb.append(image.provenance().lastContainedOffset());
        sb.append(", epoch=");
        sb.append(image.provenance().lastContainedEpoch());
        if (delta.clusterDelta() != null) {
            sb.append(", numChangedBrokers=");
            sb.append(delta.clusterDelta().changedBrokers().size());
        }
        if (delta.featuresDelta() != null) {
            sb.append(", changedFeatures=");
            sb.append(delta.featuresDelta().changes());
        }
        if (delta.configsDelta() != null) {
            sb.append(", numChangedConfigs=");
            sb.append(delta.configsDelta().changes().size());
        }
        if (delta.producerIdsDelta() != null) {
            sb.append(", changedProducerId=");
            sb.append(delta.producerIdsDelta().nextProducerId());
        }
        if (delta.topicsDelta() != null) {
            sb.append(", numChangedTopics=");
            sb.append(delta.topicsDelta().changedTopics().size());
        }
        sb.append(", numBrokers=");
        sb.append(image.cluster().brokers().size());
        sb.append(", features=");
        sb.append(image.features().finalizedVersions());
        sb.append(", numConfigs=");
        sb.append(image.configs().resourceData().values().stream().mapToInt(configImage -> configImage.data().size()).sum());
        sb.append(", nextProducerId=");
        sb.append(image.producerIds().highestSeenProducerId());
        sb.append(", numTopics=");
        sb.append(image.topics().topicsById().size());
        sb.append(")");
        return sb.toString();
    }
}
