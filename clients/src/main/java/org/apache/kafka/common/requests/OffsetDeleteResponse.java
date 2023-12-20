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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - {@link Errors#GROUP_SUBSCRIBED_TO_TOPIC}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *
 * - Group or coordinator errors:
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 *   - {@link Errors#INVALID_GROUP_ID}
 *   - {@link Errors#GROUP_ID_NOT_FOUND}
 *   - {@link Errors#NON_EMPTY_GROUP}
 */
public class OffsetDeleteResponse extends AbstractResponse {

    public static class Builder {
        OffsetDeleteResponseData data = new OffsetDeleteResponseData();

        private OffsetDeleteResponseTopic getOrCreateTopic(
            String topicName
        ) {
            OffsetDeleteResponseTopic topic = data.topics().find(topicName);
            if (topic == null) {
                topic = new OffsetDeleteResponseTopic().setName(topicName);
                data.topics().add(topic);
            }
            return topic;
        }

        public Builder addPartition(
            String topicName,
            int partitionIndex,
            Errors error
        ) {
            final OffsetDeleteResponseTopic topicResponse = getOrCreateTopic(topicName);

            topicResponse.partitions().add(new OffsetDeleteResponsePartition()
                .setPartitionIndex(partitionIndex)
                .setErrorCode(error.code()));

            return this;
        }

        public <P> Builder addPartitions(
            String topicName,
            List<P> partitions,
            Function<P, Integer> partitionIndex,
            Errors error
        ) {
            final OffsetDeleteResponseTopic topicResponse = getOrCreateTopic(topicName);

            partitions.forEach(partition -> {
                topicResponse.partitions().add(new OffsetDeleteResponsePartition()
                    .setPartitionIndex(partitionIndex.apply(partition))
                    .setErrorCode(error.code()));
            });

            return this;
        }

        public Builder merge(
            OffsetDeleteResponseData newData
        ) {
            if (newData.errorCode() != Errors.NONE.code()) {
                // If the top-level error exists, we can discard it and use the new data.
                data = newData;
            } else if (data.topics().isEmpty()) {
                // If the current data is empty, we can discard it and use the new data.
                data = newData;
            } else {
                // Otherwise, we have to merge them together.
                newData.topics().forEach(newTopic -> {
                    OffsetDeleteResponseTopic existingTopic = data.topics().find(newTopic.name());
                    if (existingTopic == null) {
                        // If no topic exists, we can directly copy the new topic data.
                        data.topics().add(newTopic.duplicate());
                    } else {
                        // Otherwise, we add the partitions to the existing one. Note we
                        // expect non-overlapping partitions here as we don't verify
                        // if the partition is already in the list before adding it.
                        newTopic.partitions().forEach(partition -> {
                            existingTopic.partitions().add(partition.duplicate());
                        });
                    }
                });
            }

            return this;
        }

        public OffsetDeleteResponse build() {
            return new OffsetDeleteResponse(data);
        }
    }

    private final OffsetDeleteResponseData data;

    public OffsetDeleteResponse(OffsetDeleteResponseData data) {
        super(ApiKeys.OFFSET_DELETE);
        this.data = data;
    }

    @Override
    public OffsetDeleteResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        updateErrorCounts(counts, Errors.forCode(data.errorCode()));
        data.topics().forEach(topic ->
            topic.partitions().forEach(partition ->
                updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
            )
        );
        return counts;
    }

    public static OffsetDeleteResponse parse(ByteBuffer buffer, short version) {
        return new OffsetDeleteResponse(new OffsetDeleteResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 0;
    }
}
