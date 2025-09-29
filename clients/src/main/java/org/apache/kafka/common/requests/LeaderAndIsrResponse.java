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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LeaderAndIsrResponse {

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     * STALE_BROKER_EPOCH (77)
     */
    private final Errors error;
    private final LinkedHashMap<Uuid, List<PartitionError>> topicErrors;

    public LeaderAndIsrResponse(Errors error, LinkedHashMap<Uuid, List<PartitionError>> topicErrors) {
        this.error = error;
        this.topicErrors = topicErrors;
    }

    public LinkedHashMap<Uuid, List<PartitionError>> topics() {
        return topicErrors;
    }

    public Errors error() {
        return error;
    }

    public Map<Errors, Integer> errorCounts() {
        Errors error = error();
        if (error != Errors.NONE) {
            // Minor optimization since the top-level error applies to all partitions
            return Collections.singletonMap(error, topics().values().stream().mapToInt(partitionErrors ->
                    partitionErrors.size()).sum() + 1);
        }
        Map<Errors, Integer> errors = AbstractResponse.errorCounts(topics().values().stream().flatMap(partitionErrors ->
                partitionErrors.stream()).map(p -> Errors.forCode(p.errorCode)));
        AbstractResponse.updateErrorCounts(errors, Errors.NONE);
        return errors;
    }

    public Map<TopicPartition, Errors> partitionErrors(Map<Uuid, String> topicNames) {
        Map<TopicPartition, Errors> errors = new HashMap<>();
        topics().forEach((topicId, partitionErrors) -> {
            String topicName = topicNames.get(topicId);
            if (topicName != null) {
                partitionErrors.forEach(partition ->
                    errors.put(new TopicPartition(topicName, partition.partitionIndex), Errors.forCode(partition.errorCode)));
            }
        });
        return errors;
    }

    @Override
    public String toString() {
        return "LeaderAndIsrResponse{" +
                "error=" + error +
                ", topicErrors=" + topicErrors +
                '}';
    }

    public static class PartitionError {
        public final int partitionIndex;
        public final short errorCode;

        public PartitionError(int partitionIndex, short errorCode) {
            this.partitionIndex = partitionIndex;
            this.errorCode = errorCode;
        }
    }
}
