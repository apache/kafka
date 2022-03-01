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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.common.message.LiCombinedControlResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class LiCombinedControlResponse extends AbstractResponse {
    private final LiCombinedControlResponseData data;
    private final short version;
    public LiCombinedControlResponse(LiCombinedControlResponseData data, short version) {
        super(ApiKeys.LI_COMBINED_CONTROL);
        this.data = data;
        this.version = version;
    }

    public List<LiCombinedControlResponseData.LeaderAndIsrPartitionError> leaderAndIsrPartitionErrors() {
        return data.leaderAndIsrPartitionErrors();
    }

    public List<LiCombinedControlResponseData.StopReplicaPartitionError> stopReplicaPartitionErrors() {
        return data.stopReplicaPartitionErrors();
    }

    public short leaderAndIsrErrorCode() {
        return data.leaderAndIsrErrorCode();
    }

    private Errors leaderAndIsrError() {
        return Errors.forCode(data.leaderAndIsrErrorCode());
    }

    public short updateMetadataErrorCode() {
        return data.updateMetadataErrorCode();
    }

    private Errors updateMetadataError() {
        return Errors.forCode(data.updateMetadataErrorCode());
    }

    public short stopReplicaErrorCode() {
        return data.stopReplicaErrorCode();
    }

    private Errors stopReplicaError() {
        return Errors.forCode(data.stopReplicaErrorCode());
    }

    public Errors error() {
        // To be backward compatible with the existing API, which can only return one error,
        // we give the following priorities LeaderAndIsr error > Stop Replica error > UpdateMetadata error
        Errors leaderAndIsrError = leaderAndIsrError();
        if (leaderAndIsrError != Errors.NONE) {
            return leaderAndIsrError;
        }

        Errors stopReplicaError = stopReplicaError();
        if (stopReplicaError != Errors.NONE) {
            return stopReplicaError;
        }

        Errors updateMetadataError = updateMetadataError();
        if (updateMetadataError != Errors.NONE) {
            return updateMetadataError;
        }

        return Errors.NONE;
    }

    public static LiCombinedControlResponse parse(ByteBuffer buffer, short version) {
        return new LiCombinedControlResponse(new LiCombinedControlResponseData(
                new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Errors leaderAndIsrError = leaderAndIsrError();
        Map<Errors, Integer> leaderAndIsrErrorCount;
        if (leaderAndIsrError != Errors.NONE) {
            // Minor optimization since the top-level error applies to all partitions
            leaderAndIsrErrorCount = Collections.singletonMap(leaderAndIsrError, data.leaderAndIsrPartitionErrors().size());
        } else {
            leaderAndIsrErrorCount = errorCounts(data.leaderAndIsrPartitionErrors().stream().map(l -> Errors.forCode(l.errorCode())).collect(Collectors.toList()));
        }

        Map<Errors, Integer> updateMetadataErrorCount = errorCounts(updateMetadataError());

        Map<Errors, Integer> stopReplicaErrorCount;
        if (data.stopReplicaErrorCode() != Errors.NONE.code()) {
            // Minor optimization since the top-level error applies to all partitions
            stopReplicaErrorCount = Collections.singletonMap(error(), data.stopReplicaPartitionErrors().size());
        } else {
            stopReplicaErrorCount = errorCounts(data.stopReplicaPartitionErrors().stream().map(p -> Errors.forCode(p.errorCode())).collect(Collectors.toList()));
        }

        // merge the several count maps into one result
        Map<Errors, Integer> combinedErrorCount = mergeMaps(leaderAndIsrErrorCount, updateMetadataErrorCount);
        combinedErrorCount = mergeMaps(combinedErrorCount, stopReplicaErrorCount);

        return combinedErrorCount;
    }

    @Override
    public int throttleTimeMs() {
        return 0;
    }

    private static Map<Errors, Integer> mergeMaps(Map<Errors, Integer> m1, Map<Errors, Integer> m2) {
        Map<Errors, Integer> result = new HashMap<>(m1);
        m2.forEach((k, v) -> result.merge(k, v, (v1, v2) -> v1 + v2));
        return result;
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public LiCombinedControlResponseData data() {
        return data;
    }

    public short version() {
        return version;
    }
}
