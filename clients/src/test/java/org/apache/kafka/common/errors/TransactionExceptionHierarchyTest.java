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
package org.apache.kafka.common.errors;

import org.apache.kafka.common.InvalidRecordException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransactionExceptionHierarchyTest {

    /**
     * Verifies that the given exception class extends `RetriableException`
     * and does **not** extend `RefreshRetriableException`.
     *
     * Using `RefreshRetriableException` changes the exception handling behavior,
     * so only exceptions extending `RetriableException` directly are considered valid here.
     *
     * @param exceptionClass the exception class to check
     */
    @ParameterizedTest
    @ValueSource(classes = {
        TimeoutException.class,
        NotEnoughReplicasException.class,
        CoordinatorLoadInProgressException.class,
        CorruptRecordException.class,
        NotEnoughReplicasAfterAppendException.class,
        ConcurrentTransactionsException.class
    })
    void testRetriableExceptionHierarchy(Class<? extends Exception> exceptionClass) {
        assertTrue(RetriableException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should extend RetriableException");
        assertFalse(RefreshRetriableException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should NOT extend RefreshRetriableException");
    }

    /**
     * Verifies that RefreshRetriableException extends RetriableException.
     */
    @Test
    void testRefreshRetriableException() {
        assertTrue(RetriableException.class.isAssignableFrom(RefreshRetriableException.class),
                "RefreshRetriableException should extend RetriableException");
    }

    /**
     * Verifies that the given exception class extends `RefreshRetriableException`
     *
     * @param exceptionClass the exception class to check
     */
    @ParameterizedTest
    @ValueSource(classes = {
        UnknownTopicOrPartitionException.class,
        NotLeaderOrFollowerException.class,
        NotCoordinatorException.class,
        CoordinatorNotAvailableException.class
    })
    void testRefreshRetriableExceptionHierarchy(Class<? extends Exception> exceptionClass) {
        assertTrue(RefreshRetriableException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should extend RefreshRetriableException");
    }

    /**
     * Verifies that the given exception class extends `ApplicationRecoverableException`
     *
     * @param exceptionClass the exception class to check
     */
    @ParameterizedTest
    @ValueSource(classes = {
        FencedInstanceIdException.class,
        IllegalGenerationException.class,
        InvalidPidMappingException.class,
        InvalidProducerEpochException.class,
        ProducerFencedException.class,
        UnknownMemberIdException.class
    })
    void testApplicationRecoverableExceptionHierarchy(Class<? extends Exception> exceptionClass) {
        assertTrue(ApplicationRecoverableException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should extend ApplicationRecoverableException");
    }

    /**
     * Verifies that the given exception class extends `InvalidConfigurationException`
     *
     * @param exceptionClass the exception class to check
     */
    @ParameterizedTest
    @ValueSource(classes = {
        AuthenticationException.class,
        AuthorizationException.class,
        ClusterAuthorizationException.class,
        TransactionalIdAuthorizationException.class,
        UnsupportedVersionException.class,
        UnsupportedForMessageFormatException.class,
        InvalidRecordException.class,
        InvalidRequiredAcksException.class,
        RecordBatchTooLargeException.class,
        InvalidTopicException.class,
        TopicAuthorizationException.class,
        GroupAuthorizationException.class
    })
    void testInvalidConfigurationExceptionHierarchy(Class<? extends Exception> exceptionClass) {
        assertTrue(InvalidConfigurationException.class.isAssignableFrom(exceptionClass),
                exceptionClass.getSimpleName() + " should extend InvalidConfigurationException");
    }
}
