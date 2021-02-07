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
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

import java.nio.ByteBuffer;
import java.util.Map;


public class FindCoordinatorResponse extends AbstractResponse {

    /**
     * Possible error codes:
     *
     * COORDINATOR_LOAD_IN_PROGRESS (14)
     * COORDINATOR_NOT_AVAILABLE (15)
     * GROUP_AUTHORIZATION_FAILED (30)
     * INVALID_REQUEST (42)
     * TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53)
     */

    private final FindCoordinatorResponseData data;

    public FindCoordinatorResponse(FindCoordinatorResponseData data) {
        super(ApiKeys.FIND_COORDINATOR);
        this.data = data;
    }

    @Override
    public FindCoordinatorResponseData data() {
        return data;
    }

    public Node node() {
        return new Node(data.nodeId(), data.host(), data.port());
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    public boolean hasError() {
        return error() != Errors.NONE;
    }

    public Errors error() {
        return Errors.forCode(data.errorCode());
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(error());
    }

    public static FindCoordinatorResponse parse(ByteBuffer buffer, short version) {
        return new FindCoordinatorResponse(new FindCoordinatorResponseData(new ByteBufferAccessor(buffer), version));
    }

    @Override
    public String toString() {
        return data.toString();
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 2;
    }

    public static FindCoordinatorResponse prepareResponse(Errors error, Node node) {
        FindCoordinatorResponseData data = new FindCoordinatorResponseData();
        data.setErrorCode(error.code())
            .setErrorMessage(error.message())
            .setNodeId(node.id())
            .setHost(node.host())
            .setPort(node.port());
        return new FindCoordinatorResponse(data);
    }
}
