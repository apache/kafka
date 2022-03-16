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
package org.apache.kafka.common.requests;


import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.NotCoordinatorException;
import org.apache.kafka.common.errors.NotEnoughReplicasException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ApiErrorTest {

    @ParameterizedTest
    @MethodSource("parameters")
    public void fromThrowableShouldReturnCorrectError(Throwable t, Errors expectedErrors, String expectedMsg) {
        ApiError apiError = ApiError.fromThrowable(t);
        assertEquals(apiError.error(), expectedErrors);
        assertEquals(apiError.message(), expectedMsg);
    }

    private static Collection<Arguments> parameters() {
        List<Arguments> arguments = new ArrayList<>();

        arguments.add(Arguments.of(
            new UnknownServerException("Don't leak sensitive information "), Errors.UNKNOWN_SERVER_ERROR, null));

        arguments.add(Arguments.of(
            new NotEnoughReplicasException(), Errors.NOT_ENOUGH_REPLICAS, null));

        // avoid populating the error message if it's a generic one
        arguments.add(Arguments.of(
            new UnknownTopicOrPartitionException(Errors.UNKNOWN_TOPIC_OR_PARTITION.message()), Errors.UNKNOWN_TOPIC_OR_PARTITION, null));

        String notCoordinatorErrorMsg = "Not coordinator";
        arguments.add(Arguments.of(
            new NotCoordinatorException(notCoordinatorErrorMsg), Errors.NOT_COORDINATOR, notCoordinatorErrorMsg));

        String notControllerErrorMsg = "Not controller";
        // test the NotControllerException is wrapped in the CompletionException, should return correct error
        arguments.add(Arguments.of(
            new CompletionException(new NotControllerException(notControllerErrorMsg)), Errors.NOT_CONTROLLER, notControllerErrorMsg));

        String requestTimeoutErrorMsg = "request time out";
        // test the TimeoutException is wrapped in the ExecutionException, should return correct error
        arguments.add(Arguments.of(
            new ExecutionException(new TimeoutException(requestTimeoutErrorMsg)), Errors.REQUEST_TIMED_OUT, requestTimeoutErrorMsg));

        // test the exception not in the Errors list, should return UNKNOWN_SERVER_ERROR
        arguments.add(Arguments.of(new IOException(), Errors.UNKNOWN_SERVER_ERROR, null));

        return arguments;
    }
}
