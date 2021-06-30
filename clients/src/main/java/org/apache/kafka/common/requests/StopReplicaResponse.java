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

import org.apache.kafka.common.message.StopReplicaResponseData;
import org.apache.kafka.common.message.StopReplicaResponseData.StopReplicaPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class StopReplicaResponse extends AbstractResponse {

    /**
     * Possible error code:
     *  - {@link Errors#STALE_CONTROLLER_EPOCH}
     *  - {@link Errors#STALE_BROKER_EPOCH}
     *  - {@link Errors#FENCED_LEADER_EPOCH}
     *  - {@link Errors#KAFKA_STORAGE_ERROR}
     */
    private final StopReplicaResponseData data;

    public StopReplicaResponse(StopReplicaResponseData data) {
        super(ApiKeys.STOP_REPLICA);
        this.data = data;
    }

    public List<StopReplicaPartitionError> partitionErrors() {
        return data.partitionErrors();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (data.errorCode() != Errors.NONE.code())
            // Minor optimization since the top-level error applies to all partitions
            return Collections.singletonMap(error(), data.partitionErrors().size() + 1);
        Map<Errors, Integer> errors = errorCounts(data.partitionErrors().stream().map(p -> Errors.forCode(p.errorCode())));
        updateErrorCounts(errors, Errors.forCode(data.errorCode())); // top level error
        return errors;
    }

    public static StopReplicaResponse parse(ByteBuffer buffer, short version) {
        return new StopReplicaResponse(new StopReplicaResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public StopReplicaResponseData data() {
        return data;
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
