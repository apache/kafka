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
import java.util.Collections;
import java.util.List;

/**
 * The result of the {@link Admin#listGroups()} call.
 * <p>
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class ListGroupsResult {
    private final KafkaFutureImpl<Collection<GroupListing>> all;
    private final KafkaFutureImpl<Collection<GroupListing>> valid;
    private final KafkaFutureImpl<Collection<Throwable>> errors;

    ListGroupsResult(KafkaFuture<Collection<Object>> future) {
        this.all = new KafkaFutureImpl<>();
        this.valid = new KafkaFutureImpl<>();
        this.errors = new KafkaFutureImpl<>();
        future.thenApply(results -> {
            ArrayList<Throwable> curErrors = new ArrayList<>();
            ArrayList<GroupListing> curValid = new ArrayList<>();
            for (Object resultObject : results) {
                if (resultObject instanceof Throwable) {
                    curErrors.add((Throwable) resultObject);
                } else {
                    curValid.add((GroupListing) resultObject);
                }
            }
            List<GroupListing> validResult = Collections.unmodifiableList(curValid);
            List<Throwable> errorsResult = Collections.unmodifiableList(curErrors);
            if (!errorsResult.isEmpty()) {
                all.completeExceptionally(errorsResult.get(0));
            } else {
                all.complete(validResult);
            }
            valid.complete(validResult);
            errors.complete(errorsResult);
            return null;
        });
    }

    /**
     * Returns a future that yields either an exception, or the full set of group listings.
     * <p>
     * In the event of a failure, the future yields nothing but the first exception which
     * occurred.
     */
    public KafkaFuture<Collection<GroupListing>> all() {
        return all;
    }

    /**
     * Returns a future which yields just the valid listings.
     * <p>
     * This future never fails with an error, no matter what happens.  Errors are completely
     * ignored.  If nothing can be fetched, an empty collection is yielded.
     * If there is an error, but some results can be returned, this future will yield
     * those partial results.  When using this future, it is a good idea to also check
     * the errors future so that errors can be displayed and handled.
     */
    public KafkaFuture<Collection<GroupListing>> valid() {
        return valid;
    }

    /**
     * Returns a future which yields just the errors which occurred.
     * <p>
     * If this future yields a non-empty collection, it is very likely that elements are
     * missing from the valid() set.
     * <p>
     * This future itself never fails with an error.  In the event of an error, this future
     * will successfully yield a collection containing at least one exception.
     */
    public KafkaFuture<Collection<Throwable>> errors() {
        return errors;
    }
}
