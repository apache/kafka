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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.message.LiDeleteFederatedTopicZnodesResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;


public class LiDeleteFederatedTopicZnodesResponse extends AbstractResponse {
    private final LiDeleteFederatedTopicZnodesResponseData data;
    private final short version;

    public LiDeleteFederatedTopicZnodesResponse(LiDeleteFederatedTopicZnodesResponseData data, short version) {
        super(ApiKeys.LI_DELETE_FEDERATED_TOPIC_ZNODES);
        this.data = data;
        this.version = version;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return Collections.singletonMap(error(), 1);
    }

    @Override
    public LiDeleteFederatedTopicZnodesResponseData data() {
        return data;
    }

    public static LiDeleteFederatedTopicZnodesResponse prepareResponse(Errors error, int throttleTimeMs, short version) {
        LiDeleteFederatedTopicZnodesResponseData data = new LiDeleteFederatedTopicZnodesResponseData();
        data.setErrorCode(error.code());
        data.setThrottleTimeMs(throttleTimeMs);
        return new LiDeleteFederatedTopicZnodesResponse(data, version);
    }

    public static LiDeleteFederatedTopicZnodesResponse parse(ByteBuffer buffer, short version) {
        return new LiDeleteFederatedTopicZnodesResponse(new LiDeleteFederatedTopicZnodesResponseData(new ByteBufferAccessor(buffer), version), version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public short version() {
        return version;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
