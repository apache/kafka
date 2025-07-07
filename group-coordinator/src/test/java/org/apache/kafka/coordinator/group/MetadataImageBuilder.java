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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import java.util.Arrays;

public class MetadataImageBuilder {
    private final MetadataDelta delta;

    public MetadataImageBuilder() {
        this(MetadataImage.EMPTY);
    }

    public MetadataImageBuilder(MetadataImage image) {
        this.delta = new MetadataDelta(image);
    }

    public MetadataImageBuilder addTopic(
        Uuid topicId,
        String topicName,
        int numPartitions
    ) {
        // For testing purposes, the following criteria are used:
        // - Number of replicas for each partition: 2
        // - Number of brokers available in the cluster: 4
        delta.replay(new TopicRecord().setTopicId(topicId).setName(topicName));
        for (int i = 0; i < numPartitions; i++) {
            delta.replay(new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(i)
                .setReplicas(Arrays.asList(i % 4, (i + 1) % 4)));
        }
        return this;
    }

    /**
     * Add rack Ids for 4 broker Ids.
     * <p>
     * For testing purposes, each broker is mapped
     * to a rack Id with the same broker Id as a suffix.
     */
    public MetadataImageBuilder addRacks() {
        for (int i = 0; i < 4; i++) {
            delta.replay(new RegisterBrokerRecord().setBrokerId(i).setRack("rack" + i));
        }
        return this;
    }

    public MetadataImage build() {
        return build(0);
    }

    public MetadataImage build(long version) {
        return delta.apply(new MetadataProvenance(version, 0, 0L, true));
    }
}
