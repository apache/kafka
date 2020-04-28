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

import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteResponseData.OffsetDeleteResponseTopic;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Message;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - {@link Errors#GROUP_SUBSCRIBED_TO_TOPIC}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *
 * - Group or coordinator errors:
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 *   - {@link Errors#INVALID_GROUP_ID}
 *   - {@link Errors#GROUP_ID_NOT_FOUND}
 *   - {@link Errors#NON_EMPTY_GROUP}
 */
public class OffsetDeleteResponse extends AbstractResponse {

    public final OffsetDeleteResponseData data;

    public OffsetDeleteResponse(OffsetDeleteResponseData data) {
        this.data = data;
    }

    @Override
    protected Message data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new HashMap<>();
        counts.put(Errors.forCode(data.errorCode()), 1);
        for (OffsetDeleteResponseTopic topic : data.topics()) {
            for (OffsetDeleteResponsePartition partition : topic.partitions()) {
                Errors error = Errors.forCode(partition.errorCode());
                counts.put(error, counts.getOrDefault(error, 0) + 1);
            }
        }
        return counts;
    }

    public static OffsetDeleteResponse parse(ByteBuffer buffer, short version) {
        return new OffsetDeleteResponse(new OffsetDeleteResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 0;
    }
}
