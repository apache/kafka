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

package org.apache.kafka.soak.cloud;

import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Arrays;
import java.util.List;

/**
 * The response to a mock command invocation.
 */
public final class MockCommandResponse {
    private final KafkaFutureImpl<String> response;
    private final List<String> request;
    private final KafkaFutureImpl<Void> complete = new KafkaFutureImpl<>();

    public MockCommandResponse(KafkaFutureImpl<String> response, String... request) {
        this.response = response;
        this.request = Arrays.asList(request);
    }

    public MockCommandResponse(String response, String... request) {
        this.response = new KafkaFutureImpl<>();
        this.request = Arrays.asList(request);
        this.response.complete(response);
    }

    public MockCommandResponse(int returnCode, String... request) {
        this.request = Arrays.asList(request);
        this.response = new KafkaFutureImpl<>();
        if (returnCode != 0) {
            this.response.completeExceptionally(
                new RemoteCommandResultException(this.request, returnCode));
        } else {
            this.response.complete("");
        }
    }

    public List<String> request() {
        return request;
    }

    public KafkaFutureImpl<String> response() {
        return response;
    };

    public KafkaFutureImpl<Void> complete() {
        return complete;
    };

    @Override
    public int hashCode() {
        return request.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MockCommandResponse other = (MockCommandResponse) o;
        return request.equals(other.request);
    }
}
