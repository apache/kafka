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

public class MetadataSummary {
    public static String toString(MetadataImage image) {
        StringBuilder sb = new StringBuilder();
        sb.append("MetadataSummary(");
        sb.append("offset=");
        sb.append(image.provenance().lastContainedOffset());
        sb.append(", epoch=");
        sb.append(image.provenance().lastContainedEpoch());
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
