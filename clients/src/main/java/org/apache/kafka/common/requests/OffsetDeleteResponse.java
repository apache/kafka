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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.Struct;

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

    public OffsetDeleteResponse(Struct struct) {
        short latestVersion = (short) (OffsetDeleteResponseData.SCHEMAS.length - 1);
        this.data = new OffsetDeleteResponseData(struct, latestVersion);
    }

    public OffsetDeleteResponse(Struct struct, short version) {
        this.data = new OffsetDeleteResponseData(struct, version);
    }

    @Override
    protected Struct toStruct(short version) {
        return data.toStruct(version);
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
        return new OffsetDeleteResponse(ApiKeys.OFFSET_DELETE.parseResponse(version, buffer));
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
