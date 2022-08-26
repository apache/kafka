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

import org.apache.kafka.common.message.DescribeDelegationTokenRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class DescribeDelegationTokenRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<DescribeDelegationTokenRequest> {
        private final DescribeDelegationTokenRequestData data;

        public Builder(List<KafkaPrincipal> owners) {
            super(ApiKeys.DESCRIBE_DELEGATION_TOKEN);
            this.data = new DescribeDelegationTokenRequestData()
                .setOwners(owners == null ? null : owners
                    .stream()
                    .map(owner -> new DescribeDelegationTokenRequestData.DescribeDelegationTokenOwner()
                            .setPrincipalName(owner.getName())
                            .setPrincipalType(owner.getPrincipalType()))
                    .collect(Collectors.toList()));
        }

        @Override
        public DescribeDelegationTokenRequest build(short version) {
            return new DescribeDelegationTokenRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final DescribeDelegationTokenRequestData data;

    public DescribeDelegationTokenRequest(DescribeDelegationTokenRequestData data, short version) {
        super(ApiKeys.DESCRIBE_DELEGATION_TOKEN, version);
        this.data = data;
    }

    @Override
    public DescribeDelegationTokenRequestData data() {
        return data;
    }

    public boolean ownersListEmpty() {
        return data.owners() != null && data.owners().isEmpty();
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeDelegationTokenResponse(version(), throttleTimeMs, Errors.forException(e));
    }

    public static DescribeDelegationTokenRequest parse(ByteBuffer buffer, short version) {
        return new DescribeDelegationTokenRequest(new DescribeDelegationTokenRequestData(
            new ByteBufferAccessor(buffer), version), version);
    }
}
