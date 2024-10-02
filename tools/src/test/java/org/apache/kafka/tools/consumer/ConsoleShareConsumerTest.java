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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.MessageFormatter;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.MockTime;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConsoleShareConsumerTest {

    @BeforeEach
    public void setup() {
        ConsoleShareConsumer.messageCount = 0;
    }

    @Test
    public void shouldThrowTimeoutExceptionWhenTimeoutIsReached() {
        String topic = "test";
        final Time time = new MockTime();
        final int timeoutMs = 1000;

        @SuppressWarnings("unchecked")
        ShareConsumer<byte[], byte[]> mockConsumer = mock(ShareConsumer.class);

        when(mockConsumer.poll(Duration.ofMillis(timeoutMs))).thenAnswer(invocation -> {
            time.sleep(timeoutMs / 2 + 1);
            return ConsumerRecords.EMPTY;
        });

        ConsoleShareConsumer.ConsumerWrapper consumer = new ConsoleShareConsumer.ConsumerWrapper(
                topic,
                mockConsumer,
                timeoutMs
        );

        assertThrows(TimeoutException.class, consumer::receive);
    }

    @Test
    public void shouldLimitReadsToMaxMessageLimit() {
        ConsoleShareConsumer.ConsumerWrapper consumer = mock(ConsoleShareConsumer.ConsumerWrapper.class);
        MessageFormatter formatter = mock(MessageFormatter.class);
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("foo", 1, 1, new byte[0], new byte[0]);

        int messageLimit = 10;
        when(consumer.receive()).thenReturn(record);

        ConsoleShareConsumer.process(messageLimit, formatter, consumer, System.out, true, AcknowledgeType.ACCEPT);

        verify(consumer, times(messageLimit)).receive();
        verify(formatter, times(messageLimit)).writeTo(any(), any());

        consumer.cleanup();
    }

    @Test
    public void shouldStopWhenOutputCheckErrorFails() {
        ConsoleShareConsumer.ConsumerWrapper consumer = mock(ConsoleShareConsumer.ConsumerWrapper.class);
        MessageFormatter formatter = mock(MessageFormatter.class);
        PrintStream printStream = mock(PrintStream.class);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("foo", 1, 1, new byte[0], new byte[0]);

        when(consumer.receive()).thenReturn(record);
        //Simulate an error on System.out after the first record has been printed
        when(printStream.checkError()).thenReturn(true);

        ConsoleShareConsumer.process(-1, formatter, consumer, printStream, true, AcknowledgeType.ACCEPT);

        verify(formatter).writeTo(any(), eq(printStream));
        verify(consumer).receive();
        verify(printStream).checkError();

        consumer.cleanup();
    }

    @Test
    public void testRejectMessageOnError() {
        ConsoleShareConsumer.ConsumerWrapper consumer = mock(ConsoleShareConsumer.ConsumerWrapper.class);
        MessageFormatter formatter = mock(MessageFormatter.class);
        PrintStream printStream = mock(PrintStream.class);

        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>("foo", 1, 1, new byte[0], new byte[0]);

        when(consumer.receive()).thenReturn(record);

        //Simulate an error on formatter.writeTo() call
        doThrow(new RuntimeException())
            .when(formatter)
            .writeTo(any(), any());

        ConsoleShareConsumer.process(1, formatter, consumer, printStream, true, AcknowledgeType.ACCEPT);

        verify(formatter).writeTo(any(), eq(printStream));
        verify(consumer).receive();
        verify(consumer).acknowledge(record, AcknowledgeType.REJECT);

        consumer.cleanup();
    }
}
