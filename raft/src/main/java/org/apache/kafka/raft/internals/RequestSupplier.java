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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import java.util.function.Supplier;

/**
 * This class is used to generate requests from a supplier for KRaft RPCs. If the
 * api key of the supplier's generated request does not match the expected api key,
 * an IllegalStateException is thrown.
 */
public class RequestSupplier {
    private final Supplier<ApiMessage> supplier;
    private final ApiKeys apiKey;

    private RequestSupplier(Supplier<ApiMessage> supplier, ApiKeys apiKey) {
        this.supplier = supplier;
        this.apiKey = apiKey;
    }

    public static RequestSupplier of(
        Supplier<ApiMessage> supplier,
        ApiKeys apiKey
    ) {
        return new RequestSupplier(supplier, apiKey);
    }

    public ApiMessage request() {
        ApiMessage request = supplier.get();
        if (request.apiKey() != apiKey.id) {
            throw new IllegalStateException("Request type mismatch: expected " + apiKey +
                " but got " + request.apiKey());
        }
        return request;
    }

    public ApiKeys apiKey() {
        return apiKey;
    }
}
