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

import org.apache.kafka.common.message.AlterUserScramCredentialsRequestData;
import org.apache.kafka.common.message.AlterUserScramCredentialsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AlterUserScramCredentialsRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<AlterUserScramCredentialsRequest> {
        private final AlterUserScramCredentialsRequestData data;

        public Builder(AlterUserScramCredentialsRequestData data) {
            super(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS);
            this.data = data;
        }

        @Override
        public AlterUserScramCredentialsRequest build(short version) {
            return new AlterUserScramCredentialsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final AlterUserScramCredentialsRequestData data;

    private AlterUserScramCredentialsRequest(AlterUserScramCredentialsRequestData data, short version) {
        super(ApiKeys.ALTER_USER_SCRAM_CREDENTIALS, version);
        this.data = data;
    }

    public static AlterUserScramCredentialsRequest parse(ByteBuffer buffer, short version) {
        return new AlterUserScramCredentialsRequest(new AlterUserScramCredentialsRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public AlterUserScramCredentialsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        ApiError apiError = ApiError.fromThrowable(e);
        short errorCode = apiError.error().code();
        String errorMessage = apiError.message();
        Set<String> users = Stream.concat(
                this.data.deletions().stream().map(deletion -> deletion.name()),
                this.data.upsertions().stream().map(upsertion -> upsertion.name()))
                .collect(Collectors.toSet());
        List<AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult> results =
                users.stream().sorted().map(user ->
                        new AlterUserScramCredentialsResponseData.AlterUserScramCredentialsResult()
                                .setUser(user)
                                .setErrorCode(errorCode)
                                .setErrorMessage(errorMessage))
                        .collect(Collectors.toList());
        return new AlterUserScramCredentialsResponse(new AlterUserScramCredentialsResponseData().setResults(results));
    }
}
