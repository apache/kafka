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

import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class DeleteRecordsResponse extends AbstractResponse {

    public static final long INVALID_LOW_WATERMARK = -1L;
    private final DeleteRecordsResponseData data;

    /**
     * Possible error code:
     *
     * OFFSET_OUT_OF_RANGE (1)
     * UNKNOWN_TOPIC_OR_PARTITION (3)
     * NOT_LEADER_OR_FOLLOWER (6)
     * REQUEST_TIMED_OUT (7)
     * UNKNOWN (-1)
     */

    public DeleteRecordsResponse(DeleteRecordsResponseData data) {
        super(ApiKeys.DELETE_RECORDS);
        this.data = data;
    }

    @Override
    public DeleteRecordsResponseData data() {
        return data;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> errorCounts = new HashMap<>();
        data.topics().forEach(topicResponses ->
            topicResponses.partitions().forEach(response ->
                updateErrorCounts(errorCounts, Errors.forCode(response.errorCode()))
            )
        );
        return errorCounts;
    }

    public static DeleteRecordsResponse parse(ByteBuffer buffer, short version) {
        return new DeleteRecordsResponse(new DeleteRecordsResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
