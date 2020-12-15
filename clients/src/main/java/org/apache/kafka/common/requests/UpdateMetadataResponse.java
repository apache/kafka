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

import org.apache.kafka.common.message.UpdateMetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Map;

public class UpdateMetadataResponse extends AbstractResponse {

    private final UpdateMetadataResponseData data;

    public UpdateMetadataResponse(UpdateMetadataResponseData data) {
        super(ApiKeys.UPDATE_METADATA);
        this.data = data;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    public static UpdateMetadataResponse parse(ByteBuffer buffer, short version) {
        return new UpdateMetadataResponse(new UpdateMetadataResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public UpdateMetadataResponseData data() {
        return data;
    }

}
