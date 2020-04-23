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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ElectLeadersResponseData;
import org.apache.kafka.common.message.ElectLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class ElectLeadersResponse extends AbstractResponse {

    private final short version;
    private final ElectLeadersResponseData data;

    public ElectLeadersResponse(Struct struct) {
        this(struct, ApiKeys.ELECT_LEADERS.latestVersion());
    }

    public ElectLeadersResponse(Struct struct, short version) {
        this.version = version;
        this.data = new ElectLeadersResponseData(struct, version);
    }

    public ElectLeadersResponse(
            int throttleTimeMs,
            short errorCode,
            List<ReplicaElectionResult> electionResults) {
        this(throttleTimeMs, errorCode, electionResults, ApiKeys.ELECT_LEADERS.latestVersion());
    }

    public ElectLeadersResponse(
            int throttleTimeMs,
            short errorCode,
            List<ReplicaElectionResult> electionResults,
            short version) {

        this.version = version;
        this.data = new ElectLeadersResponseData();

        data.setThrottleTimeMs(throttleTimeMs);

        if (version >= 1) {
            data.setErrorCode(errorCode);
        }

        data.setReplicaElectionResults(electionResults);
    }

    public ElectLeadersResponseData data() {
        return data;
    }

    public short version() {
        return version;
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        data.replicaElectionResults().forEach(result ->
            result.partitionResult().forEach(partitionResult ->
                updateErrorCounts(counts, Errors.forCode(partitionResult.errorCode()))
            )
        );
        return counts;
    }

    public static ElectLeadersResponse parse(ByteBuffer buffer, short version) {
        return new ElectLeadersResponse(ApiKeys.ELECT_LEADERS.responseSchema(version).read(buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return true;
    }

    public static Map<TopicPartition, Optional<Throwable>> electLeadersResult(ElectLeadersResponseData data) {
        Map<TopicPartition, Optional<Throwable>> map = new HashMap<>();

        for (ElectLeadersResponseData.ReplicaElectionResult topicResults : data.replicaElectionResults()) {
            for (ElectLeadersResponseData.PartitionResult partitionResult : topicResults.partitionResult()) {
                Optional<Throwable> value = Optional.empty();
                Errors error = Errors.forCode(partitionResult.errorCode());
                if (error != Errors.NONE) {
                    value = Optional.of(error.exception(partitionResult.errorMessage()));
                }

                map.put(new TopicPartition(topicResults.topic(), partitionResult.partitionId()),
                        value);
            }
        }

        return map;
    }
}
