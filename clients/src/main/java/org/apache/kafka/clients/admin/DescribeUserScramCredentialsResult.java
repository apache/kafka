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
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.protocol.Errors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * The result of the {@link Admin#describeUserScramCredentials()} call.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeUserScramCredentialsResult {
    private final KafkaFuture<DescribeUserScramCredentialsResponseData> dataFuture;

    /**
     *
     * @param dataFuture the future indicating response data from the call
     */
    public DescribeUserScramCredentialsResult(KafkaFuture<DescribeUserScramCredentialsResponseData> dataFuture) {
        this.dataFuture = Objects.requireNonNull(dataFuture);
    }

    /**
     *
     * @return a future for the results of all requested (either explicitly or implicitly via describe-all) users.
     * The future will complete successfully only if all user descriptions complete successfully.
     */
    public KafkaFuture<Map<String, UserScramCredentialsDescription>> all() {
        return KafkaFuture.allOf(dataFuture).thenApply(v -> {
            DescribeUserScramCredentialsResponseData data = valueFromFutureGuaranteedToSucceedAtThisPoint(dataFuture);
            // check to make sure every individual user succeeded
            Optional<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult> optionalFirstFailure =
                    data.results().stream().filter(result -> result.errorCode() != Errors.NONE.code()).findFirst();
            if (optionalFirstFailure.isPresent()) {
                throw Errors.forCode(optionalFirstFailure.get().errorCode()).exception(optionalFirstFailure.get().errorMessage());
            }
            Map<String, UserScramCredentialsDescription> retval = new HashMap<>();
            data.results().stream().forEach(userResult ->
                    retval.put(userResult.user(), new UserScramCredentialsDescription(userResult.user(),
                            getScramCredentialInfosFor(userResult))));
            return retval;
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
        return KafkaFuture.allOf(dataFuture).thenApply(v -> {
            DescribeUserScramCredentialsResponseData data = valueFromFutureGuaranteedToSucceedAtThisPoint(dataFuture);
            return data.results().stream().map(result -> result.user()).collect(Collectors.toList());
        });
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
        return KafkaFuture.allOf(dataFuture).thenApply(v -> {
            DescribeUserScramCredentialsResponseData data = valueFromFutureGuaranteedToSucceedAtThisPoint(dataFuture);
            Optional<DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult> optionalUserResult =
                    data.results().stream().filter(result -> result.user().equals(userName)).findFirst();
            // it is possible that there is no future for this user (for example, the original describe request was for
            // users 1, 2, and 3 but this is looking for user 4), so explicitly take care of that case
            if (!optionalUserResult.isPresent()) {
                throw new ResourceNotFoundException("No such user: " + userName);
            }
            DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult userResult = optionalUserResult.get();
            if (userResult.errorCode() != Errors.NONE.code()) {
                throw Errors.forCode(userResult.errorCode()).exception(userResult.errorMessage());
            }
            return new UserScramCredentialsDescription(userResult.user(), getScramCredentialInfosFor(userResult));
        });
    }

    private static List<ScramCredentialInfo> getScramCredentialInfosFor(
            DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult userResult) {
        return userResult.credentialInfos().stream().map(c ->
                new ScramCredentialInfo(ScramMechanism.fromType(c.mechanism()), c.iterations()))
                .collect(Collectors.toList());
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
}
