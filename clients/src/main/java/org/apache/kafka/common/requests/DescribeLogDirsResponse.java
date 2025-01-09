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

import org.apache.kafka.common.message.DescribeLogDirsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


public class DescribeLogDirsResponse extends AbstractResponse {

    public static final long INVALID_OFFSET_LAG = -1L;
    public static final long UNKNOWN_VOLUME_BYTES = -1L;

    private final DescribeLogDirsResponseData data;

    public DescribeLogDirsResponse(DescribeLogDirsResponseData data) {
        super(ApiKeys.DESCRIBE_LOG_DIRS);
        this.data = data;
    }

    @Override
    public DescribeLogDirsResponseData data() {
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
        errorCounts.put(Errors.forCode(data.errorCode()), 1);
        data.results().forEach(result ->
            updateErrorCounts(errorCounts, Errors.forCode(result.errorCode()))
        );
        return errorCounts;
    }

    public static DescribeLogDirsResponse parse(ByteBuffer buffer, short version) {
        return new DescribeLogDirsResponse(new DescribeLogDirsResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }
}
