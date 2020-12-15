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

import org.apache.kafka.common.message.LeaderAndIsrResponseData;
import org.apache.kafka.common.message.LeaderAndIsrResponseData.LeaderAndIsrPartitionError;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LeaderAndIsrResponse extends AbstractResponse {

    /**
     * Possible error code:
     *
     * STALE_CONTROLLER_EPOCH (11)
     * STALE_BROKER_EPOCH (77)
     */
    private final LeaderAndIsrResponseData data;

    public LeaderAndIsrResponse(LeaderAndIsrResponseData data) {
        super(ApiKeys.LEADER_AND_ISR);
        this.data = data;
    }

    public List<LeaderAndIsrPartitionError> partitions() {
        return data.partitionErrors();
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Errors error = error();
        if (error != Errors.NONE)
            // Minor optimization since the top-level error applies to all partitions
            return Collections.singletonMap(error, data.partitionErrors().size() + 1);
        Map<Errors, Integer> errors = errorCounts(data.partitionErrors().stream().map(l -> Errors.forCode(l.errorCode())));
        // Top level error
        updateErrorCounts(errors, Errors.NONE);
        return errors;
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    public static LeaderAndIsrResponse parse(ByteBuffer buffer, short version) {
        return new LeaderAndIsrResponse(new LeaderAndIsrResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public LeaderAndIsrResponseData data() {
        return data;
    }

    @Override
    public String toString() {
        return data.toString();
    }

}
