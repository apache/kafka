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

import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class CreatePartitionsResponse extends AbstractResponse {

    private final CreatePartitionsResponseData data;

    public CreatePartitionsResponse(CreatePartitionsResponseData data) {
        this.data = data;
    }

    public CreatePartitionsResponseData data() {
        return data;
    }

    //FIXME Check this
    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        for (CreatePartitionsTopicResult result : data.results()) {
            Errors error = Errors.forCode(result.errorCode());
            counts.put(error, counts.getOrDefault(error, 0) + 1);
        }
        return counts;
    }

    public static CreatePartitionsResponse parse(ByteBuffer buffer, short version) {
        return new CreatePartitionsResponse(new CreatePartitionsResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 1;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }
}
