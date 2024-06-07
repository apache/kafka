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

package org.apache.kafka.controller.errors;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NotControllerException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.raft.errors.UnexpectedBaseOffsetException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class EventHandlerExceptionInfoTest {
    private static final EventHandlerExceptionInfo TOPIC_EXISTS =
        EventHandlerExceptionInfo.fromInternal(
            new TopicExistsException("Topic exists."),
            () -> OptionalInt.empty());

    private static final EventHandlerExceptionInfo REJECTED_EXECUTION =
        EventHandlerExceptionInfo.fromInternal(
            new RejectedExecutionException(),
            () -> OptionalInt.empty());

    private static final EventHandlerExceptionInfo INTERRUPTED =
        EventHandlerExceptionInfo.fromInternal(
            new InterruptedException(),
            () -> OptionalInt.of(1));

    private static final EventHandlerExceptionInfo NULL_POINTER =
        EventHandlerExceptionInfo.fromInternal(
            new NullPointerException(),
            () -> OptionalInt.of(1));

    private static final EventHandlerExceptionInfo NOT_LEADER =
        EventHandlerExceptionInfo.fromInternal(
            new NotLeaderException("Append failed"),
            () -> OptionalInt.of(2));

    private static final EventHandlerExceptionInfo UNEXPECTED_END_OFFSET =
        EventHandlerExceptionInfo.fromInternal(
            new UnexpectedBaseOffsetException("Wanted base offset 3, but the next offset was 4"),
            () -> OptionalInt.of(1));

    @Test
    public void testTopicExistsExceptionInfo() {
        assertEquals(new EventHandlerExceptionInfo(false, false,
            new TopicExistsException("Topic exists.")),
                TOPIC_EXISTS);
    }

    @Test
    public void testTopicExistsExceptionFailureMessage() {
        assertEquals("event failed with TopicExistsException in 234 microseconds. Exception message: Topic exists.",
            TOPIC_EXISTS.failureMessage(123, OptionalLong.of(234L), true, 456L));
    }

    @Test
    public void testRejectedExecutionExceptionInfo() {
        assertEquals(new EventHandlerExceptionInfo(false, false,
            new RejectedExecutionException(),
            new TimeoutException("The controller is shutting down.", new RejectedExecutionException())),
                REJECTED_EXECUTION);
    }

    @Test
    public void testRejectedExecutionExceptionFailureMessage() {
        assertEquals("event unable to start processing because of RejectedExecutionException (treated " +
            "as TimeoutException).",
            REJECTED_EXECUTION.failureMessage(123, OptionalLong.empty(), true, 456L));
    }

    @Test
    public void testInterruptedExceptionInfo() {
        assertEquals(new EventHandlerExceptionInfo(true, true,
            new InterruptedException(),
            new UnknownServerException("The controller was interrupted.")),
                INTERRUPTED);
    }

    @Test
    public void testInterruptedExceptionFailureMessageWhenActive() {
        assertEquals("event unable to start processing because of InterruptedException (treated as " +
            "UnknownServerException) at epoch 123. Renouncing leadership and reverting to the " +
            "last committed offset 456.",
            INTERRUPTED.failureMessage(123, OptionalLong.empty(), true, 456L));
    }

    @Test
    public void testInterruptedExceptionFailureMessageWhenInactive() {
        assertEquals("event unable to start processing because of InterruptedException (treated as " +
            "UnknownServerException) at epoch 123. The controller is already in standby mode.",
                INTERRUPTED.failureMessage(123, OptionalLong.empty(), false, 456L));
    }

    @Test
    public void testNullPointerExceptionInfo() {
        assertEquals(new EventHandlerExceptionInfo(true, true,
            new NullPointerException(),
            new UnknownServerException(new NullPointerException())),
                NULL_POINTER);
    }

    @Test
    public void testNullPointerExceptionFailureMessageWhenActive() {
        assertEquals("event failed with NullPointerException (treated as UnknownServerException) " +
            "at epoch 123 in 40 microseconds. Renouncing leadership and reverting to the last " +
            "committed offset 456.",
                NULL_POINTER.failureMessage(123, OptionalLong.of(40L), true, 456L));
    }

    @Test
    public void testNullPointerExceptionFailureMessageWhenInactive() {
        assertEquals("event failed with NullPointerException (treated as UnknownServerException) " +
            "at epoch 123 in 40 microseconds. The controller is already in standby mode.",
                NULL_POINTER.failureMessage(123, OptionalLong.of(40L), false, 456L));
    }

    @Test
    public void testNotLeaderExceptionInfo() {
        assertEquals(new EventHandlerExceptionInfo(false, true,
            new NotLeaderException("Append failed"),
            new NotControllerException("The active controller appears to be node 2.")),
                NOT_LEADER);
    }

    @Test
    public void testNotLeaderExceptionFailureMessage() {
        assertEquals("event unable to start processing because of NotLeaderException (treated as " +
            "NotControllerException) at epoch 123. Renouncing leadership and reverting to the " +
            "last committed offset 456. Exception message: Append failed",
            NOT_LEADER.failureMessage(123, OptionalLong.empty(), true, 456L));
    }

    @Test
    public void testUnexpectedBaseOffsetExceptionInfo() {
        assertEquals(new EventHandlerExceptionInfo(false, true,
            new UnexpectedBaseOffsetException("Wanted base offset 3, but the next offset was 4"),
            new NotControllerException("Unexpected end offset. Controller will resign.")),
                UNEXPECTED_END_OFFSET);
    }

    @Test
    public void testUnexpectedBaseOffsetFailureMessage() {
        assertEquals("event failed with UnexpectedBaseOffsetException (treated as " +
            "NotControllerException) at epoch 123 in 90 microseconds. Renouncing leadership " +
            "and reverting to the last committed offset 456. Exception message: Wanted base offset 3, but the next offset was 4",
                UNEXPECTED_END_OFFSET.failureMessage(123, OptionalLong.of(90L), true, 456L));
    }

    @Test
    public void testFaultExceptionFailureMessage() {
        EventHandlerExceptionInfo faultExceptionInfo = EventHandlerExceptionInfo.fromInternal(
                new KafkaException("Custom kafka exception message"),
                () -> OptionalInt.of(1));
        String failureMessage = faultExceptionInfo.failureMessage(123, OptionalLong.of(90L), true, 456L);
        assertEquals("event failed with KafkaException (treated as UnknownServerException) " +
                "at epoch 123 in 90 microseconds. Renouncing leadership and reverting " +
                "to the last committed offset 456.", failureMessage);
    }

    @Test
    public void testIsNotTimeoutException() {
        assertFalse(TOPIC_EXISTS.isTimeoutException());
        assertFalse(REJECTED_EXECUTION.isTimeoutException());
        assertFalse(INTERRUPTED.isTimeoutException());
        assertFalse(NULL_POINTER.isTimeoutException());
        assertFalse(NOT_LEADER.isTimeoutException());
        assertFalse(UNEXPECTED_END_OFFSET.isTimeoutException());
    }

    @Test
    public void testIsTimeoutException() {
        EventHandlerExceptionInfo timeoutExceptionInfo = EventHandlerExceptionInfo.fromInternal(
                new TimeoutException(),
                () -> OptionalInt.of(1));
        assertTrue(timeoutExceptionInfo.isTimeoutException());
    }
}
