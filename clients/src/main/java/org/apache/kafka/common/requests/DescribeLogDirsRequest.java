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

import org.apache.kafka.common.message.DescribeLogDirsRequestData;
import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;

import java.nio.ByteBuffer;

public class DescribeLogDirsRequest extends AbstractRequest {

    private final DescribeLogDirsRequestData data;

    public static class Builder extends AbstractRequest.Builder<DescribeLogDirsRequest> {
        private final DescribeLogDirsRequestData data;

        public Builder(DescribeLogDirsRequestData data) {
            super(ApiKeys.DESCRIBE_LOG_DIRS);
            this.data = data;
        }

        @Override
        public DescribeLogDirsRequest build(short version) {
            return new DescribeLogDirsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public DescribeLogDirsRequest(DescribeLogDirsRequestData data, short version) {
        super(ApiKeys.DESCRIBE_LOG_DIRS, version);
        this.data = data;
    }

    @Override
    public DescribeLogDirsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new DescribeLogDirsResponse(new DescribeLogDirsResponseData().setThrottleTimeMs(throttleTimeMs));
    }

    public boolean isAllTopicPartitions() {
        return data.topics() == null;
    }

    public static DescribeLogDirsRequest parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsRequest(new DescribeLogDirsRequestData(new ByteBufferAccessor(buffer), version), version);
    }
}
