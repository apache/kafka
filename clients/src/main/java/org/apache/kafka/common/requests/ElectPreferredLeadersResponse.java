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

import org.apache.kafka.common.message.ElectPreferredLeadersResponseData;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.PartitionResult;
import org.apache.kafka.common.message.ElectPreferredLeadersResponseData.ReplicaElectionResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ElectPreferredLeadersResponse extends AbstractResponse {

    private final ElectPreferredLeadersResponseData data;

    public ElectPreferredLeadersResponse(ElectPreferredLeadersResponseData data) {
        this.data = data;
    }

    public ElectPreferredLeadersResponse(Struct struct, short version) {
        this.data = new ElectPreferredLeadersResponseData(struct, version);
    }

    public ElectPreferredLeadersResponse(Struct struct) {
        short latestVersion = (short) (ElectPreferredLeadersResponseData.SCHEMAS.length - 1);
        this.data = new ElectPreferredLeadersResponseData(struct, latestVersion);
    }

    public ElectPreferredLeadersResponseData data() {
        return data;
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
        for (ReplicaElectionResult result : data.replicaElectionResults()) {
            for (PartitionResult partitionResult : result.partitionResult()) {
                Errors error = Errors.forCode(partitionResult.errorCode());
                counts.put(error, counts.getOrDefault(error, 0) + 1);
            }
        }
        return counts;
    }

    public static ElectPreferredLeadersResponse parse(ByteBuffer buffer, short version) {
        return new ElectPreferredLeadersResponse(
                ApiKeys.ELECT_PREFERRED_LEADERS.responseSchema(version).read(buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 3;
    }
}