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

import org.apache.kafka.common.message.AssignReplicasToDirsRequestData;
import org.apache.kafka.common.message.AssignReplicasToDirsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;

public class AssignReplicasToDirsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AssignReplicasToDirsRequest> {

        private final AssignReplicasToDirsRequestData data;

        public Builder(AssignReplicasToDirsRequestData data) {
            super(ApiKeys.ASSIGN_REPLICAS_TO_DIRS);
            this.data = data;
        }

        @Override
        public AssignReplicasToDirsRequest build(short version) {
            return new AssignReplicasToDirsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AssignReplicasToDirsRequestData data;

    public AssignReplicasToDirsRequest(AssignReplicasToDirsRequestData data, short version) {
        super(ApiKeys.ASSIGN_REPLICAS_TO_DIRS, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AssignReplicasToDirsResponse(
                new AssignReplicasToDirsResponseData()
                        .setThrottleTimeMs(throttleTimeMs)
                        .setErrorCode(Errors.forException(e).code())
        );
    }

    @Override
    public AssignReplicasToDirsRequestData data() {
        return data;
    }

    public static AssignReplicasToDirsRequest parse(ByteBuffer buffer, short version) {
        return new AssignReplicasToDirsRequest(new AssignReplicasToDirsRequestData(
                new ByteBufferAccessor(buffer), version), version);
    }
}
