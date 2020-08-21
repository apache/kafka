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
import org.apache.kafka.common.errors.ResourceNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The result of the {@link Admin#describeUserScramCredentials()} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeUserScramCredentialsResult {
    private final KafkaFuture<List<String>> usersFuture;
    private final Map<String, KafkaFuture<UserScramCredentialsDescription>> perUserFutures;

    /**
     *
     * @param usersFuture the future indicating the users described by the call
     * @param perUserFutures the required map of user names to futures representing the results of describing each
     *                       user's SCRAM credentials.
     */
    public DescribeUserScramCredentialsResult(KafkaFuture<List<String>> usersFuture,
                                              Map<String, KafkaFuture<UserScramCredentialsDescription>> perUserFutures) {
        this.usersFuture = Objects.requireNonNull(usersFuture);
        this.perUserFutures = Objects.requireNonNull(perUserFutures);
    }

    /**
     *
     * @return a future for the results of all requested (either explicitly or implicitly via describe-all) users.
     * The future will complete successfully only if the users future first completes successfully and then all the
     * futures for the user descriptions complete successfully.
     */
    public KafkaFuture<Map<String, UserScramCredentialsDescription>> all() {
        KafkaFuture<Void> succeedsOnlyIfUsersFutureSucceeds = KafkaFuture.allOf(users());
        return succeedsOnlyIfUsersFutureSucceeds.thenApply(void1 -> {
            KafkaFuture<Void> succeedsOnlyIfAllDescriptionsSucceed = KafkaFuture.allOf(perUserFutures.values().toArray(
                    new KafkaFuture[perUserFutures.size()]));
            KafkaFuture<Map<String, UserScramCredentialsDescription>> mapFuture = succeedsOnlyIfAllDescriptionsSucceed.thenApply(void2 ->
                perUserFutures.entrySet().stream().collect(Collectors.toMap(
                    e -> e.getKey(),
                    e -> valueFromFutureGuaranteedToSucceedAtThisPoint(e.getValue()))));
            /* At this point it is only the users future that is guaranteed to have succeeded.
             * We want to return the future to the map, but we have to return a map at this point.
             * We need to dereference the future while propagating any exception.
             */
            return valueFromFuturePropagatingExceptionsAsUnchecked(mapFuture);
        });
    }

    /**
     *
     * @return a future indicating the distinct users that were requested (either explicitly or implicitly via
     * describe-all).  The future will not complete successfully if the user is not authorized to perform the describe
     * operation; otherwise, it will complete successfully as long as the list of users with credentials can be
     * successfully determined within some hard-coded timeout period.
     */
    public KafkaFuture<List<String>> users() {
        return usersFuture;
    }

    /**
     *
     * @param userName the name of the user description being requested
     * @return a future indicating the description results for the given user. The future will complete exceptionally if
     * the future returned by {@link #users()} completes exceptionally.  If the given user does not exist in the list
     * of requested users then the future will complete exceptionally with
     * {@link org.apache.kafka.common.errors.ResourceNotFoundException}.
     */
    public KafkaFuture<UserScramCredentialsDescription> description(String userName) {
        KafkaFuture<Void> succeedsOnlyIfUsersFutureSucceeds = KafkaFuture.allOf(usersFuture);
        return succeedsOnlyIfUsersFutureSucceeds.thenApply(void1 -> {
            // it is possible that there is no future for this user (for example, the original describe request was for
            // users 1, 2, and 3 but this is looking for user 4), so explicitly take care of that case
            KafkaFuture<UserScramCredentialsDescription> requestedUserFuture = perUserFutures.get(userName);
            if (requestedUserFuture == null) {
                throw new ResourceNotFoundException("No such user: " + userName);
            }
            KafkaFuture<Void> succeedsOnlyIfRequestedUserFutureSucceeds = KafkaFuture.allOf(requestedUserFuture);
            KafkaFuture<UserScramCredentialsDescription> descriptionFuture = succeedsOnlyIfRequestedUserFutureSucceeds.thenApply(void2 ->
                valueFromFutureGuaranteedToSucceedAtThisPoint(requestedUserFuture));
            /* At this point it is only the users future that is guaranteed to have succeeded.
             * We want to return the future to the description, but we have to return a description at this point.
             * We need to dereference the future while propagating any exception.
             */
            return valueFromFuturePropagatingExceptionsAsUnchecked(descriptionFuture);
        });
    }

    private static <T> T valueFromFutureGuaranteedToSucceedAtThisPoint(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            // could happen; convert it to an unchecked exception
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // will never happen since it is assumed the future will succeed
            throw new RuntimeException(e);
        }
    }

    private static <T> T valueFromFuturePropagatingExceptionsAsUnchecked(KafkaFuture<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            // could happen; convert it to an unchecked exception
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            // could happen; convert it to an unchecked exception
            throw new RuntimeException(e);
        }
    }
}
