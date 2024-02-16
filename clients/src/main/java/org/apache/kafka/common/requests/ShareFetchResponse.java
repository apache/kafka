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

import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes.
 * - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 * - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 * - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 * - {@link Errors#NOT_LEADER_OR_FOLLOWER}
 * - {@link Errors#UNKNOWN_TOPIC_ID}
 * - {@link Errors#INVALID_RECORD_STATE}
 * - {@link Errors#KAFKA_STORAGE_ERROR}
 * - {@link Errors#CORRUPT_MESSAGE}
 * - {@link Errors#INVALID_REQUEST}
 * - {@link Errors#UNKNOWN_SERVER_ERROR}
 */
public class ShareFetchResponse extends AbstractResponse {

    private final ShareFetchResponseData data;

    public ShareFetchResponse(ShareFetchResponseData data) {
        super(ApiKeys.SHARE_FETCH);
        this.data = data;
    }

    @Override
    public ShareFetchResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        HashMap<Errors, Integer> counts = new HashMap<>();
        updateErrorCounts(counts, Errors.forCode(data.errorCode()));
        data.responses().forEach(
                topic -> topic.partitions().forEach(
                        partition -> updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                )
        );
        return counts;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static ShareFetchResponse parse(ByteBuffer buffer, short version) {
        return new ShareFetchResponse(
                new ShareFetchResponseData(new ByteBufferAccessor(buffer), version)
        );
    }
}
