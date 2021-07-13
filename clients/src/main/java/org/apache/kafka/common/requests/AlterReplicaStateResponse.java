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

import org.apache.kafka.common.message.AlterReplicaStateResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class AlterReplicaStateResponse extends AbstractResponse {
    private final AlterReplicaStateResponseData data;

    public AlterReplicaStateResponse(AlterReplicaStateResponseData data) {
        super(ApiKeys.ALTER_REPLICA_STATE);
        this.data = data;
    }

    public AlterReplicaStateResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        updateErrorCounts(counts, Errors.forCode(data.errorCode()));
        data.topics().forEach(topicResponse -> topicResponse.partitions().forEach(partitionResponse -> {
            updateErrorCounts(counts, Errors.forCode(partitionResponse.errorCode()));
        }));
        return counts;
    }

    public static AlterReplicaStateResponse parse(ByteBuffer buffer, short version) {
        return new AlterReplicaStateResponse(new AlterReplicaStateResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }
}
