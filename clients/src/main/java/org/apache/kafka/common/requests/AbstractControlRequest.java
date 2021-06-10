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

import org.apache.kafka.common.protocol.ApiKeys;

// Abstract class for all control requests including UpdateMetadataRequest, LeaderAndIsrRequest and StopReplicaRequest

/**
 * 所有控制类请求的抽象类，目前包含三种控制类请求，分别是
 * {@link UpdateMetadataRequest}
 * {@link LeaderAndIsrRequest}
 * {@link StopReplicaRequest}
 */
public abstract class AbstractControlRequest extends AbstractRequest {

    public static final long UNKNOWN_BROKER_EPOCH = -1L;

    public static abstract class Builder<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
        /**
         * Controller所在的BrokerID
         */
        protected final int controllerId;

        /**
         * Controller版本号，用于保证Controller在集群间的一致性
         */
        protected final int controllerEpoch;

        /**
         * Controller所在的Broker的版本号，用于保证Broker在集群间的一致性
         */
        protected final long brokerEpoch;

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
            super(api, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
        }

    }

    protected AbstractControlRequest(ApiKeys api, short version) {
        super(api, version);
    }

    public abstract int controllerId();

    public abstract int controllerEpoch();

    public abstract long brokerEpoch();

}
