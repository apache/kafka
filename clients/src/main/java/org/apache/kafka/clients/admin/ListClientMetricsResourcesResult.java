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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.internals.KafkaFutureImpl;

import java.util.Collection;

/**
 * The result of the {@link Admin#listClientMetricsResources()} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListClientMetricsResourcesResult {
    private final KafkaFuture<Collection<ClientMetricsResourceListing>> future;

    ListClientMetricsResourcesResult(KafkaFuture<Collection<ClientMetricsResourceListing>> future) {
        this.future = future;
    }

    /**
     * Returns a future that yields either an exception, or the full set of client metrics
     * listings.
     *
     * In the event of a failure, the future yields nothing but the first exception which
     * occurred.
     */
    public KafkaFuture<Collection<ClientMetricsResourceListing>> all() {
        final KafkaFutureImpl<Collection<ClientMetricsResourceListing>> result = new KafkaFutureImpl<>();
        future.whenComplete((listings, throwable) -> {
            if (throwable != null) {
                result.completeExceptionally(throwable);
            } else {
                result.complete(listings);
            }
        });
        return result;
    }
}
