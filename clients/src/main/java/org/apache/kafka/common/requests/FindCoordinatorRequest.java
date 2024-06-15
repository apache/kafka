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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Collections;

public class FindCoordinatorRequest extends AbstractRequest {

    public static final short MIN_BATCHED_VERSION = 4;

    public static class Builder extends AbstractRequest.Builder<FindCoordinatorRequest> {
        private final FindCoordinatorRequestData data;

        public Builder(FindCoordinatorRequestData data) {
            super(ApiKeys.FIND_COORDINATOR);
            this.data = data;
        }

        @Override
        public FindCoordinatorRequest build(short version) {
            if (version < 1 && data.keyType() == CoordinatorType.TRANSACTION.id()) {
                throw new UnsupportedVersionException("Cannot create a v" + version + " FindCoordinator request " +
                        "because we require features supported only in 2 or later.");
            }
            int batchedKeys = data.coordinatorKeys().size();
            if (version < MIN_BATCHED_VERSION) {
                if (batchedKeys > 1)
                    throw new NoBatchedFindCoordinatorsException("Cannot create a v" + version + " FindCoordinator request " +
                        "because we require features supported only in " + MIN_BATCHED_VERSION + " or later.");
                if (batchedKeys == 1) {
                    data.setKey(data.coordinatorKeys().get(0));
                    data.setCoordinatorKeys(Collections.emptyList());
                }
            } else if (batchedKeys == 0 && data.key() != null) {
                data.setCoordinatorKeys(Collections.singletonList(data.key()));
                data.setKey(""); // default value
            }
            return new FindCoordinatorRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }

        public FindCoordinatorRequestData data() {
            return data;
        }
    }

    /**
     * Indicates that it is not possible to lookup coordinators in batches with FindCoordinator. Instead
     * coordinators must be looked up one by one.
     */
    public static class NoBatchedFindCoordinatorsException extends UnsupportedVersionException {
        private static final long serialVersionUID = 1L;

        public NoBatchedFindCoordinatorsException(String message) {
            super(message);
        }
    }

    private final FindCoordinatorRequestData data;

    private FindCoordinatorRequest(FindCoordinatorRequestData data, short version) {
        super(ApiKeys.FIND_COORDINATOR, version);
        this.data = data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        FindCoordinatorResponseData response = new FindCoordinatorResponseData();
        if (version() >= 2) {
            response.setThrottleTimeMs(throttleTimeMs);
        }
        Errors error = Errors.forException(e);
        if (version() < MIN_BATCHED_VERSION) {
            return FindCoordinatorResponse.prepareOldResponse(error, Node.noNode());
        } else {
            return FindCoordinatorResponse.prepareErrorResponse(error, data.coordinatorKeys());
        }
    }

    public static FindCoordinatorRequest parse(ByteBuffer buffer, short version) {
        return new FindCoordinatorRequest(new FindCoordinatorRequestData(new ByteBufferAccessor(buffer), version),
            version);
    }

    @Override
    public FindCoordinatorRequestData data() {
        return data;
    }

    public enum CoordinatorType {
        GROUP((byte) 0), TRANSACTION((byte) 1);

        final byte id;

        CoordinatorType(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static CoordinatorType forId(byte id) {
            switch (id) {
                case 0:
                    return GROUP;
                case 1:
                    return TRANSACTION;
                default:
                    throw new InvalidRequestException("Unknown coordinator type received: " + id);
            }
        }
    }

}
