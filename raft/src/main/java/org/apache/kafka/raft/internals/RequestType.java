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

/**
 * This class is used to wrap the ApiKeys enum for KRaft RPCs so the KRaft request
 * manager can treat the FETCH and FETCH_SNAPSHOT requests as the same type when
 * managing in-flight requests. This is useful for satisfying the invariant
 * that at most one FETCH or FETCH_SNAPSHOT request is pending at any time.
 */
public class RequestType {
    public static final RequestType FETCH_AND_FETCH_SNAPSHOT = new RequestType(ApiKeys.FETCH);
    private final ApiKeys apiKey;

    private RequestType(ApiKeys apiKey) {
        this.apiKey = apiKey;
    }

    public static RequestType of(ApiKeys apiKey) {
        if (apiKey == ApiKeys.FETCH_SNAPSHOT) {
            return FETCH_AND_FETCH_SNAPSHOT;
        }
        return new RequestType(apiKey);
    }

    public ApiKeys apiKey() {
        return apiKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RequestType)) {
            return false;
        }
        RequestType other = (RequestType) obj;
        return apiKey == other.apiKey;
    }

    @Override
    public int hashCode() {
        return apiKey.hashCode();
    }
}
