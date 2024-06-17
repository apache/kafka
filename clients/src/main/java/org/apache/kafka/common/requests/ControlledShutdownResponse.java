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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ControlledShutdownResponseData;
import org.apache.kafka.common.message.ControlledShutdownResponseData.RemainingPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;


public class ControlledShutdownResponse extends AbstractResponse {

    /**
     * Possible error codes:
     *
     * UNKNOWN(-1) (this is because IllegalStateException may be thrown in `KafkaController.shutdownBroker`, it would be good to improve this)
     * BROKER_NOT_AVAILABLE(8)
     * STALE_CONTROLLER_EPOCH(11)
     */
    private final ControlledShutdownResponseData data;

    public ControlledShutdownResponse(ControlledShutdownResponseData data) {
        super(ApiKeys.CONTROLLED_SHUTDOWN);
        this.data = data;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    @Override
    public int throttleTimeMs() {
        return DEFAULT_THROTTLE_TIME;
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        // Not supported by the response schema
    }

    public static ControlledShutdownResponse parse(ByteBuffer buffer, short version) {
        return new ControlledShutdownResponse(new ControlledShutdownResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public ControlledShutdownResponseData data() {
        return data;
    }

    public static ControlledShutdownResponse prepareResponse(Errors error, Set<TopicPartition> tps) {
        ControlledShutdownResponseData data = new ControlledShutdownResponseData();
        data.setErrorCode(error.code());
        ControlledShutdownResponseData.RemainingPartitionCollection pSet = new ControlledShutdownResponseData.RemainingPartitionCollection();
        tps.forEach(tp -> {
            pSet.add(new RemainingPartition()
                    .setTopicName(tp.topic())
                    .setPartitionIndex(tp.partition()));
        });
        data.setRemainingPartitions(pSet);
        return new ControlledShutdownResponse(data);
    }

}
