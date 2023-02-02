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

import java.util.function.Function;

public class MetadataImageUtil {
    public static <T> void imageSize(StringBuilder sb, String name, T image, Function<T, Integer> mapper) {
        int count = mapper.apply(image);
        sb.append(count).append(" ").append(name);
        if (count != 1) {
            sb.append("s");
        }
        sb.append(" (");
    }

    public static <T> void deltaSizeOrZero(StringBuilder sb, T delta, Function<T, Integer> mapper) {
        if (delta == null) {
            sb.append("0");
        } else {
            sb.append(mapper.apply(delta));
        }
        sb.append(" changed)");
    }

    /**
     * Given a MetadataImage and MetadataDelta, produce a succinct human-readable description.
     */
    public static String toSummaryString(MetadataImage image, MetadataDelta delta) {
        StringBuilder sb = new StringBuilder();
        sb.append("Metadata at offset ");
        sb.append(image.provenance().lastContainedOffset());
        sb.append(" and epoch ");
        sb.append(image.provenance().lastContainedEpoch());
        sb.append(" includes: ");

        imageSize(sb, "broker", image.cluster(), clusterImage -> clusterImage.brokers().size());
        deltaSizeOrZero(sb, delta.clusterDelta(), clusterDelta -> clusterDelta.changedBrokers().size());
        sb.append(", ");

        imageSize(sb, "topic", image.topics(), topicsImage -> topicsImage.topicsById().size());
        deltaSizeOrZero(sb, delta.topicsDelta(), topicsDelta -> topicsDelta.changedTopics().size());
        sb.append(", ");

        imageSize(sb, "feature", image.features(), featuresImage -> featuresImage.finalizedVersions().size());
        deltaSizeOrZero(sb, delta.featuresDelta(), featuresDelta -> featuresDelta.changes().size());
        sb.append(", ");

        imageSize(sb, "config", image.configs(),
            configsImage -> configsImage.resourceData().values().stream().mapToInt(ConfigurationImage::size).sum());
        deltaSizeOrZero(sb, delta.configsDelta(),
            configsDelta -> configsDelta.changes().values().stream().mapToInt(ConfigurationDelta::size).sum());

        if (!image.producerIds().isEmpty()) {
            sb.append(", ");
            sb.append("1 producer id (");
            if (delta.producerIdsDelta() != null) {
                sb.append("1");
            } else {
                sb.append("0");
            }
            sb.append(" changed)");
        }
        return sb.toString();
    }
}
