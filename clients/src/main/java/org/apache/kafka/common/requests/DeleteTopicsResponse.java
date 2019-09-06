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

import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


public class DeleteTopicsResponse extends AbstractResponse {

    /**
     * Possible error codes:
     *
     * REQUEST_TIMED_OUT(7)
     * INVALID_TOPIC_EXCEPTION(17)
     * TOPIC_AUTHORIZATION_FAILED(29)
     * NOT_CONTROLLER(41)
     * INVALID_REQUEST(42)
     * TOPIC_DELETION_DISABLED(73)
     */
    private DeleteTopicsResponseData data;

    public DeleteTopicsResponse(DeleteTopicsResponseData data) {
        this.data = data;
    }

    public DeleteTopicsResponse(Struct struct, short version) {
        this.data = new DeleteTopicsResponseData(struct, version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public DeleteTopicsResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        for (DeletableTopicResult result : data.responses()) {
            Errors error = Errors.forCode(result.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    public static DeleteTopicsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteTopicsResponse(ApiKeys.DELETE_TOPICS.parseResponse(version, buffer), version);
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }
}
