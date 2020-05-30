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

import org.apache.kafka.common.message.DescribeQuorumRequestData;
import org.apache.kafka.common.message.FindQuorumResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

public class DescribeQuorumRequest extends AbstractRequest {
    public static class Builder extends AbstractRequest.Builder<DescribeQuorumRequest> {
        private final DescribeQuorumRequestData data;

        public Builder(DescribeQuorumRequestData data) {
            super(ApiKeys.DESCRIBE_QUORUM);
            this.data = data;
        }

        @Override
        public DescribeQuorumRequest build(short version) {
            return new DescribeQuorumRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public final DescribeQuorumRequestData data;

    private DescribeQuorumRequest(DescribeQuorumRequestData data, short version) {
        super(ApiKeys.DESCRIBE_QUORUM, version);
        this.data = data;
    }

    public DescribeQuorumRequest(Struct struct, short version) {
        super(ApiKeys.DESCRIBE_QUORUM, version);
        this.data = new DescribeQuorumRequestData(struct, version);
    }

    @Override
    protected Struct toStruct() {
        return data.toStruct(version());
    }

    @Override
    public FindQuorumResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        FindQuorumResponseData data = new FindQuorumResponseData();
        data.setErrorCode(Errors.forException(e).code());
        return new FindQuorumResponse(data);
    }
}
