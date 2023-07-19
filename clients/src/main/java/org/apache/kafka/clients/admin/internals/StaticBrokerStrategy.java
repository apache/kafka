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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;

import java.util.OptionalInt;
import java.util.Set;

/**
 * This lookup strategy is used when we already know the destination broker ID
 * and we have no need for an explicit lookup. By setting {@link ApiRequestScope#destinationBrokerId()}
 * in the returned value for {@link #lookupScope(Object)}, the driver will
 * skip the lookup.
 */
public class StaticBrokerStrategy<K> implements AdminApiLookupStrategy<K> {
    private final SingleBrokerScope scope;

    public StaticBrokerStrategy(int brokerId) {
        this.scope = new SingleBrokerScope(brokerId);
    }

    @Override
    public ApiRequestScope lookupScope(K key) {
        return scope;
    }

    @Override
    public AbstractRequest.Builder<?> buildRequest(Set<K> keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LookupResult<K> handleResponse(Set<K> keys, AbstractResponse response) {
        throw new UnsupportedOperationException();
    }

    private static class SingleBrokerScope implements ApiRequestScope {
        private final int brokerId;

        private SingleBrokerScope(int brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public OptionalInt destinationBrokerId() {
            return OptionalInt.of(brokerId);
        }
    }
}
