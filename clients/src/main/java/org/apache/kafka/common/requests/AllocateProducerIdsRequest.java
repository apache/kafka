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

import org.apache.kafka.common.message.AllocateProducerIdsRequestData;
import org.apache.kafka.common.message.AllocateProducerIdsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

public class AllocateProducerIdsRequest extends AbstractRequest {
    private static final short LAST_VERSION_BEFORE_REQUEST_HEADER_V3 = 0;

    private final AllocateProducerIdsRequestData data;

    public AllocateProducerIdsRequest(AllocateProducerIdsRequestData data, short version) {
        super(ApiKeys.ALLOCATE_PRODUCER_IDS, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AllocateProducerIdsResponse(new AllocateProducerIdsResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(Errors.forException(e).code()));
    }

    @Override
    public AllocateProducerIdsRequestData data() {
        return data;
    }

    public static class Builder extends AbstractRequest.Builder<AllocateProducerIdsRequest> {

        private final AllocateProducerIdsRequestData data;

        /**
         * @param data               the request data
         * @param useRequestHeaderV3 whether the versions which use request header v3 may be used
         */
        public Builder(AllocateProducerIdsRequestData data, boolean useRequestHeaderV3) {
            super(ApiKeys.ALLOCATE_PRODUCER_IDS,
                ApiKeys.ALLOCATE_PRODUCER_IDS.oldestVersion(),
                useRequestHeaderV3 ? ApiKeys.ALLOCATE_PRODUCER_IDS.latestVersion() : LAST_VERSION_BEFORE_REQUEST_HEADER_V3);
            this.data = data;
        }

        @Override
        public AllocateProducerIdsRequest build(short version) {
            return new AllocateProducerIdsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public static AllocateProducerIdsRequest parse(Readable readable, short version) {
        return new AllocateProducerIdsRequest(new AllocateProducerIdsRequestData(
                readable, version), version);
    }
}
