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

import java.util.ArrayList;
import java.util.Collection;

/**
 * The result of the {@link Admin#listShareGroups(ListShareGroupsOptions)} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListShareGroupsResult {

    private final KafkaFutureImpl<Collection<ShareGroupListing>> all;
    private final KafkaFutureImpl<Collection<ShareGroupListing>> valid;
    private final KafkaFutureImpl<Collection<Throwable>> errors;

    ListShareGroupsResult(KafkaFuture<Collection<Object>> future) {
        this.all = new KafkaFutureImpl<>();
        this.valid = new KafkaFutureImpl<>();
        this.errors = new KafkaFutureImpl<>();
        future.thenApply((KafkaFuture.BaseFunction<Collection<Object>, Void>) results -> {
            ArrayList<Throwable> curErrors = new ArrayList<>();
            ArrayList<ShareGroupListing> curValid = new ArrayList<>();
            for (Object resultObject : results) {
                if (resultObject instanceof Throwable) {
                    curErrors.add((Throwable) resultObject);
                } else {
                    curValid.add((ShareGroupListing) resultObject);
                }
            }
            if (!curErrors.isEmpty()) {
                all.completeExceptionally(curErrors.get(0));
            } else {
                all.complete(curValid);
            }
            valid.complete(curValid);
            errors.complete(curErrors);
            return null;
        });
    }

    /**
     * Returns a future that yields either an exception, or the full set of share group listings.
     */
    public KafkaFuture<Collection<ShareGroupListing>> all() {
        return all;
    }

    /**
     * Returns a future which yields just the valid listings.
     */
    public KafkaFuture<Collection<ShareGroupListing>> valid() {
        return valid;
    }

    /**
     * Returns a future which yields just the errors which occurred.
     */
    public KafkaFuture<Collection<Throwable>> errors() {
        return errors;
    }
}