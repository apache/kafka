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
 * The result of the {@link AdminClient#listConsumerGroups()} call.
 * <p>
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class ListConsumerGroupsResult {
    private final KafkaFutureImpl<Collection<ConsumerGroupListing>> all;
    private final KafkaFutureImpl<Collection<ConsumerGroupListing>> valid;
    private final KafkaFutureImpl<Collection<Throwable>> errors;

    ListConsumerGroupsResult(KafkaFutureImpl<Collection<Object>> future) {
        this.all = new KafkaFutureImpl<>();
        this.valid = new KafkaFutureImpl<>();
        this.errors = new KafkaFutureImpl<>();
        future.thenApply(new KafkaFuture.BaseFunction<Collection<Object>, Void>() {
            @Override
            public Void apply(Collection<Object> results) {
                ArrayList<Throwable> curErrors = new ArrayList<>();
                ArrayList<ConsumerGroupListing> curValid = new ArrayList<>();
                for (Object resultObject : results) {
                    if (resultObject instanceof Throwable) {
                        curErrors.add((Throwable) resultObject);
                    } else {
                        curValid.add((ConsumerGroupListing) resultObject);
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
            }
        });
    }

    /**
     * Returns a future that yields either an exception, or the full set of consumer group
     * listings.
     *
     * In the event of a failure, the future yields nothing but the first exception which
     * occurred.
     */
    public KafkaFutureImpl<Collection<ConsumerGroupListing>> all() {
        return all;
    }

    /**
     * Returns a future which yields just the valid listings.
     *
     * This future never fails with an error, no matter what happens.  Errors are completely
     * ignored.  If nothing can be fetched, an empty collection is yielded.
     * If there is an error, but some results can be returned, this future will yield
     * those partial results.  When using this future, it is a good idea to also check
     * the errors future so that errors can be displayed and handled.
     */
    public KafkaFutureImpl<Collection<ConsumerGroupListing>> valid() {
        return valid;
    }

    /**
     * Returns a future which yields just the errors which occurred.
     *
     * If this future yields a non-empty collection, it is very likely that elements are
     * missing from the valid() set.
     *
     * This future itself never fails with an error.  In the event of an error, this future
     * will successfully yield a collection containing at least one exception.
     */
    public KafkaFutureImpl<Collection<Throwable>> errors() {
        return errors;
    }
}
