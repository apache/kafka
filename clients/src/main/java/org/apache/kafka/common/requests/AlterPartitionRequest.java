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

import org.apache.kafka.common.message.AlterPartitionRequestData;
import org.apache.kafka.common.message.AlterPartitionRequestData.BrokerState;
import org.apache.kafka.common.message.AlterPartitionResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class AlterPartitionRequest extends AbstractRequest {

    private final AlterPartitionRequestData data;

    public AlterPartitionRequest(AlterPartitionRequestData data, short apiVersion) {
        super(ApiKeys.ALTER_PARTITION, apiVersion);
        this.data = data;
    }

    @Override
    public AlterPartitionRequestData data() {
        return data;
    }

    /**
     * Get an error response for a request with specified throttle time in the response if applicable
     */
    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        return new AlterPartitionResponse(new AlterPartitionResponseData()
            .setThrottleTimeMs(throttleTimeMs)
            .setErrorCode(Errors.forException(e).code()));
    }

    public static AlterPartitionRequest parse(ByteBuffer buffer, short version) {
        return new AlterPartitionRequest(new AlterPartitionRequestData(new ByteBufferAccessor(buffer), version), version);
    }

    public static class Builder extends AbstractRequest.Builder<AlterPartitionRequest> {

        private final AlterPartitionRequestData data;

        /**
         * Constructs a builder for AlterPartitionRequest.
         *
         * @param data The data to be sent. Note that because the version of the
         *             request is not known at this time, it is expected that all
         *             topics have a topic id and a topic name set.
         * @param canUseTopicIds True if version 2 and above can be used.
         */
        public Builder(AlterPartitionRequestData data, boolean canUseTopicIds) {
            super(
                ApiKeys.ALTER_PARTITION,
                ApiKeys.ALTER_PARTITION.oldestVersion(),
                // Version 1 is the maximum version that can be used without topic ids.
                canUseTopicIds ? ApiKeys.ALTER_PARTITION.latestVersion() : 1
            );
            this.data = data;
        }

        @Override
        public AlterPartitionRequest build(short version) {
            if (version < 3) {
                data.topics().forEach(topicData -> {
                    topicData.partitions().forEach(partitionData -> {
                        // The newIsrWithEpochs will be empty after build. Then we can skip the conversion if the build
                        // is called again.
                        if (partitionData.newIsrWithEpochs().size() > 0) {
                            List<Integer> newIsr = new ArrayList<>(partitionData.newIsrWithEpochs().size());
                            partitionData.newIsrWithEpochs().forEach(brokerState -> {
                                newIsr.add(brokerState.brokerId());
                            });
                            partitionData.setNewIsr(newIsr);
                            partitionData.setNewIsrWithEpochs(Collections.emptyList());
                        }
                    });
                });
            }
            return new AlterPartitionRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    public static List<BrokerState> newIsrToSimpleNewIsrWithBrokerEpochs(List<Integer> newIsr) {
        return newIsr.stream().map(brokerId -> new BrokerState().setBrokerId(brokerId)).collect(Collectors.toList());
    }
}
