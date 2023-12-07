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
public abstract class AbstractControlRequest extends AbstractRequest {

    /**
     * Indicates if a controller request is incremental, full, or unknown.
     * Used by LeaderAndIsrRequest.Type and UpdateMetadataRequest.Type fields.
     */
    public enum Type {
        UNKNOWN(0),
        INCREMENTAL(1),
        FULL(2);

        private final byte type;
        private Type(int type) {
            this.type = (byte) type;
        }

        public byte toByte() {
            return type;
        }

        public static Type fromByte(byte type) {
            for (Type t : Type.values()) {
                if (t.type == type) {
                    return t;
                }
            }
            return UNKNOWN;
        }
    }

    public static final long UNKNOWN_BROKER_EPOCH = -1L;

    public static abstract class Builder<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
        protected final int controllerId;
        protected final int controllerEpoch;
        protected final long brokerEpoch;
        protected final boolean kraftController;

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch, long brokerEpoch) {
            this(api, version, controllerId, controllerEpoch, brokerEpoch, false);
        }

        protected Builder(ApiKeys api, short version, int controllerId, int controllerEpoch,
                          long brokerEpoch, boolean kraftController) {
            super(api, version);
            this.controllerId = controllerId;
            this.controllerEpoch = controllerEpoch;
            this.brokerEpoch = brokerEpoch;
            this.kraftController = kraftController;
        }

        public int controllerId() {
            return controllerId;
        }

        public int controllerEpoch() {
            return controllerEpoch;
        }

        public long brokerEpoch() {
            return brokerEpoch;
        }
    }

    protected AbstractControlRequest(ApiKeys api, short version) {
        super(api, version);
    }

    public abstract int controllerId();

    public abstract boolean isKRaftController();

    public abstract int controllerEpoch();

    public abstract long brokerEpoch();

}
