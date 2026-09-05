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

import org.apache.kafka.common.errors.DuplicateResourceException;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.message.DescribeUserScramCredentialsResponseData;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DescribeUserScramCredentialsResultTest {
    @Test
    public void testTopLevelError() {
        KafkaFutureImpl<DescribeUserScramCredentialsResponseData> dataFuture = new KafkaFutureImpl<>();
        RuntimeException topLevelError = new RuntimeException();
        dataFuture.completeExceptionally(topLevelError);
        DescribeUserScramCredentialsResult results = new DescribeUserScramCredentialsResult(dataFuture);

        ExecutionException e = assertThrows(ExecutionException.class, () -> results.all().get());
        assertEquals(topLevelError, e.getCause());

        e = assertThrows(ExecutionException.class, () -> results.users().get());
        assertEquals(topLevelError, e.getCause());

        e = assertThrows(ExecutionException.class, () -> results.description("whatever").get());
        assertEquals(topLevelError, e.getCause());
    }

    @Test
    public void testUserLevelErrors() throws Exception {
        String goodUser = "goodUser";
        String unknownUser = "unknownUser";
        String failedUser = "failedUser";
        KafkaFutureImpl<DescribeUserScramCredentialsResponseData> dataFuture = new KafkaFutureImpl<>();
        ScramMechanism scramSha256 = ScramMechanism.SCRAM_SHA_256;
        int iterations = 4096;
        dataFuture.complete(new DescribeUserScramCredentialsResponseData().setErrorCode(Errors.NONE.code()).setResults(Arrays.asList(
                new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser(goodUser).setCredentialInfos(
                        Collections.singletonList(new DescribeUserScramCredentialsResponseData.CredentialInfo().setMechanism(scramSha256.type()).setIterations(iterations))),
                new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser(unknownUser).setErrorCode(Errors.RESOURCE_NOT_FOUND.code()),
                new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser(failedUser).setErrorCode(Errors.DUPLICATE_RESOURCE.code()))));
        DescribeUserScramCredentialsResult results = new DescribeUserScramCredentialsResult(dataFuture);

        ExecutionException e = assertThrows(ExecutionException.class, () -> results.all().get());
        assertInstanceOf(DuplicateResourceException.class, e.getCause());

        assertEquals(Arrays.asList(goodUser, failedUser), results.users().get(), "Expected 2 users with credentials");
        UserScramCredentialsDescription goodUserDescription = results.description(goodUser).get();
        assertEquals(new UserScramCredentialsDescription(goodUser, Collections.singletonList(new ScramCredentialInfo(scramSha256, iterations))), goodUserDescription);

        e = assertThrows(ExecutionException.class, () -> results.description(failedUser).get());
        assertInstanceOf(DuplicateResourceException.class, e.getCause());

        e = assertThrows(ExecutionException.class, () -> results.description(unknownUser).get());
        assertInstanceOf(ResourceNotFoundException.class, e.getCause());
    }

    @Test
    public void testSuccessfulDescription() throws Exception {
        String goodUser = "goodUser";
        String unknownUser = "unknownUser";
        KafkaFutureImpl<DescribeUserScramCredentialsResponseData> dataFuture = new KafkaFutureImpl<>();
        ScramMechanism scramSha256 = ScramMechanism.SCRAM_SHA_256;
        int iterations = 4096;
        dataFuture.complete(new DescribeUserScramCredentialsResponseData().setErrorCode(Errors.NONE.code()).setResults(Collections.singletonList(
                new DescribeUserScramCredentialsResponseData.DescribeUserScramCredentialsResult().setUser(goodUser).setCredentialInfos(
                        Collections.singletonList(new DescribeUserScramCredentialsResponseData.CredentialInfo().setMechanism(scramSha256.type()).setIterations(iterations))))));
        DescribeUserScramCredentialsResult results = new DescribeUserScramCredentialsResult(dataFuture);
        assertEquals(Collections.singletonList(goodUser), results.users().get(), "Expected 1 user with credentials");
        Map<String, UserScramCredentialsDescription> allResults = results.all().get();
        assertEquals(1, allResults.size());
        UserScramCredentialsDescription goodUserDescriptionViaAll = allResults.get(goodUser);
        assertEquals(new UserScramCredentialsDescription(goodUser, Collections.singletonList(new ScramCredentialInfo(scramSha256, iterations))), goodUserDescriptionViaAll);
        assertEquals(goodUserDescriptionViaAll, results.description(goodUser).get(), "Expected same thing via all() and description()");

        ExecutionException e = assertThrows(ExecutionException.class, () -> results.description(unknownUser).get());
        assertInstanceOf(ResourceNotFoundException.class, e.getCause());
    }
}
