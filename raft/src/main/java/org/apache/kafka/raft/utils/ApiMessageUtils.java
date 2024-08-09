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
package org.apache.kafka.raft.utils;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import org.apache.kafka.common.message.EndQuorumEpochResponseData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchSnapshotResponseData;
import org.apache.kafka.common.message.UpdateRaftVoterResponseData;
import org.apache.kafka.common.message.VoteResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;

public class ApiMessageUtils {
    public static ApiMessage parseErrorResponse(ApiKeys apiKey, Errors error) {
        switch (apiKey) {
            case VOTE:
                return new VoteResponseData().setErrorCode(error.code());
            case BEGIN_QUORUM_EPOCH:
                return new BeginQuorumEpochResponseData().setErrorCode(error.code());
            case END_QUORUM_EPOCH:
                return new EndQuorumEpochResponseData().setErrorCode(error.code());
            case FETCH:
                return new FetchResponseData().setErrorCode(error.code());
            case FETCH_SNAPSHOT:
                return new FetchSnapshotResponseData().setErrorCode(error.code());
            case API_VERSIONS:
                return new ApiVersionsResponseData().setErrorCode(error.code());
            case UPDATE_RAFT_VOTER:
                return new UpdateRaftVoterResponseData().setErrorCode(error.code());
            default:
                throw new IllegalArgumentException("Received response for unexpected request type: " + apiKey);
        }
    }
}
