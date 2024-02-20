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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.message.ShareFetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

public class ShareFetchRequest extends AbstractRequest {

    public static class Builder extends AbstractRequest.Builder<ShareFetchRequest> {

        private final ShareFetchRequestData data;

        public Builder(ShareFetchRequestData data) {
            this(data, false);
        }

        public Builder(ShareFetchRequestData data, boolean enableUnstableLastVersion) {
            super(ApiKeys.SHARE_FETCH, enableUnstableLastVersion);
            this.data = data;
        }

        @Override
        public ShareFetchRequest build(short version) {
            return new ShareFetchRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    private final ShareFetchRequestData data;

    public ShareFetchRequest(ShareFetchRequestData data, short version) {
        super(ApiKeys.SHARE_FETCH, version);
        this.data = data;
    }

    @Override
    public ShareFetchRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        Errors error = Errors.forException(e);
        return new ShareFetchResponse(new ShareFetchResponseData()
                .setThrottleTimeMs(throttleTimeMs)
                .setErrorCode(error.code()));
    }

    public static ShareFetchRequest parse(ByteBuffer buffer, short version) {
        return new ShareFetchRequest(
            new ShareFetchRequestData(new ByteBufferAccessor(buffer), version),
            version
        );
    }

    public static final class SharePartitionData {
        public final Uuid topicId;
        public final int maxBytes;
        public final Optional<Integer> currentLeaderEpoch;

        public SharePartitionData(
                Uuid topicId,
                int maxBytes,
                Optional<Integer> currentLeaderEpoch
        ) {
            this.topicId = topicId;
            this.maxBytes = maxBytes;
            this.currentLeaderEpoch = currentLeaderEpoch;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShareFetchRequest.SharePartitionData that = (ShareFetchRequest.SharePartitionData) o;
            return Objects.equals(topicId, that.topicId) &&
                    maxBytes == that.maxBytes &&
                    Objects.equals(currentLeaderEpoch, that.currentLeaderEpoch);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topicId, maxBytes, currentLeaderEpoch);
        }

        @Override
        public String toString() {
            return "SharePartitionData(" +
                    "topicId=" + topicId +
                    ", maxBytes=" + maxBytes +
                    ", currentLeaderEpoch=" + currentLeaderEpoch +
                    ')';
        }
    }
}