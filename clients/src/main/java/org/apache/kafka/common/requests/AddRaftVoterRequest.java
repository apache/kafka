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

import org.apache.kafka.common.message.AddRaftVoterRequestData;
import org.apache.kafka.common.message.AddRaftVoterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class AddRaftVoterRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<AddRaftVoterRequest> {
        private final AddRaftVoterRequestData data;

        public Builder(AddRaftVoterRequestData data) {
            super(ApiKeys.ADD_RAFT_VOTER);
            this.data = data;
        }

        @Override
        public AddRaftVoterRequest build(short version) {
            return new AddRaftVoterRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

    }

    private final AddRaftVoterRequestData data;

    public AddRaftVoterRequest(AddRaftVoterRequestData data, short version) {
        super(ApiKeys.ADD_RAFT_VOTER, version);
        this.data = data;
    }

    @Override
    public AddRaftVoterRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new AddRaftVoterResponse(new AddRaftVoterResponseData().
            setErrorCode(error.code()).
            setErrorMessage(error.message()).
            setThrottleTimeMs(throttleTimeMs));
    }

    public static AddRaftVoterRequest parse(Readable readable, short version) {
        return new AddRaftVoterRequest(
            new AddRaftVoterRequestData(readable, version),
            version);
    }
}
