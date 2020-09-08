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

import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.Set;

/**
 * This class is used for use cases which require requests to be sent to all
 * brokers in the cluster.
 */
public abstract class AllBrokerApiDriver<V> extends ApiDriver<AllBrokerApiDriver.BrokerKey, V> {
    private static final BrokerKey ALL_BROKERS = new BrokerKey(OptionalInt.empty());
    private static final RequestScope SINGLE_REQUEST_SCOPE = new RequestScope() {
    };

    private final Logger log;
    private final KafkaFutureImpl<Map<Integer, KafkaFutureImpl<V>>> lookupFuture;

    public AllBrokerApiDriver(
        long deadlineMs,
        long retryBackoffMs,
        LogContext logContext
    ) {
        super(Utils.mkSet(ALL_BROKERS), deadlineMs, retryBackoffMs, logContext);

        this.lookupFuture = new KafkaFutureImpl<>();
        this.log = logContext.logger(AllBrokerApiDriver.class);

        super.futures().get(ALL_BROKERS).whenComplete((nil, exception) -> {
            if (exception != null) {
                this.lookupFuture.completeExceptionally(exception);
            } else {
                this.lookupFuture.complete(collectBrokerFutures());
            }
        });
    }

    public KafkaFutureImpl<Map<Integer, KafkaFutureImpl<V>>> lookupFuture() {
        return lookupFuture;
    }

    @Override
    RequestScope lookupScope(BrokerKey key) {
        return SINGLE_REQUEST_SCOPE;
    }

    @Override
    AbstractRequest.Builder<?> buildLookupRequest(Set<BrokerKey> keys) {
        // Send empty `Metadata` request. We are only interested in the brokers from the response
        return new MetadataRequest.Builder(new MetadataRequestData());
    }

    @Override
    void handleLookupResponse(Set<BrokerKey> keys, AbstractResponse abstractResponse) {
        MetadataResponse response = (MetadataResponse) abstractResponse;
        MetadataResponseData.MetadataResponseBrokerCollection brokers = response.data.brokers();

        if (brokers.isEmpty()) {
            log.debug("Metadata response contained no brokers. Will backoff and retry");
            return;
        }

        for (MetadataResponseData.MetadataResponseBroker broker : brokers) {
            int brokerId = broker.nodeId();
            super.map(new BrokerKey(OptionalInt.of(brokerId)), brokerId);
        }

        super.complete(ALL_BROKERS, null);
    }

    private Map<Integer, KafkaFutureImpl<V>> collectBrokerFutures() {
        Map<Integer, KafkaFutureImpl<V>> brokerFutures = new HashMap<>();
        for (Map.Entry<BrokerKey, KafkaFutureImpl<V>> entry : super.futures().entrySet()) {
            BrokerKey key = entry.getKey();
            KafkaFutureImpl<V> future = entry.getValue();
            if (key.brokerId.isPresent()) {
                brokerFutures.put(key.brokerId.getAsInt(), future);
            }
        }
        return brokerFutures;
    }

    abstract AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId);

    @Override
    AbstractRequest.Builder<?> buildFulfillmentRequest(Integer brokerId, Set<BrokerKey> keys) {
        return buildFulfillmentRequest(brokerId);
    }

    abstract void handleFulfillmentResponse(Integer brokerId, AbstractResponse response);

    @Override
    void handleFulfillmentResponse(Integer brokerId, Set<BrokerKey> keys, AbstractResponse response) {
        handleFulfillmentResponse(brokerId, response);
    }

    void completeExceptionally(Integer brokerId, Throwable t) {
        super.completeExceptionally(new BrokerKey(OptionalInt.of(brokerId)), t);
    }

    void complete(Integer brokerId, V value) {
        super.complete(new BrokerKey(OptionalInt.of(brokerId)), value);
    }

    public static class BrokerKey {
        private final OptionalInt brokerId;

        public BrokerKey(OptionalInt brokerId) {
            this.brokerId = brokerId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BrokerKey that = (BrokerKey) o;
            return Objects.equals(brokerId, that.brokerId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(brokerId);
        }
    }

}
