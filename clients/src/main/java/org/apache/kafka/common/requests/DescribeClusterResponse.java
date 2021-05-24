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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.Errors;

public class DescribeClusterResponse extends AbstractResponse {

    private final DescribeClusterResponseData data;

    public DescribeClusterResponse(DescribeClusterResponseData data) {
        super(ApiKeys.DESCRIBE_CLUSTER);
        this.data = data;
    }

    public Map<Integer, Node> nodes() {
        return data.brokers().valuesList().stream()
            .map(b -> new Node(b.brokerId(), b.host(), b.port(), b.rack()))
            .collect(Collectors.toMap(Node::id, Function.identity()));
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        return errorCounts(Errors.forCode(data.errorCode()));
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public DescribeClusterResponseData data() {
        return data;
    }

    public static DescribeClusterResponse parse(ByteBuffer buffer, short version) {
        return new DescribeClusterResponse(new DescribeClusterResponseData(new ByteBufferAccessor(buffer), version));
    }
}
