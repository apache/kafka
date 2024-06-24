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
package org.apache.kafka.streams.kstream;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class WindowedSerdesTest {

    private final String topic = "sample";

    @Test
    public void shouldWrapForTimeWindowedSerde() {
        final Serde<Windowed<String>> serde = WindowedSerdes.timeWindowedSerdeFrom(String.class, Long.MAX_VALUE);
        assertInstanceOf(TimeWindowedSerializer.class, serde.serializer());
        assertInstanceOf(TimeWindowedDeserializer.class, serde.deserializer());
        assertInstanceOf(StringSerializer.class, ((TimeWindowedSerializer) serde.serializer()).innerSerializer());
        assertInstanceOf(StringDeserializer.class, ((TimeWindowedDeserializer) serde.deserializer()).innerDeserializer());
    }

    @Test
    public void shouldWrapForSessionWindowedSerde() {
        final Serde<Windowed<String>> serde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        assertInstanceOf(SessionWindowedSerializer.class, serde.serializer());
        assertInstanceOf(SessionWindowedDeserializer.class, serde.deserializer());
        assertInstanceOf(StringSerializer.class, ((SessionWindowedSerializer) serde.serializer()).innerSerializer());
        assertInstanceOf(StringDeserializer.class, ((SessionWindowedDeserializer) serde.deserializer()).innerDeserializer());
    }

    @Test
    public void testTimeWindowSerdeFrom() {
        final Windowed<Integer> timeWindowed = new Windowed<>(10, new TimeWindow(0, Long.MAX_VALUE));
        final Serde<Windowed<Integer>> timeWindowedSerde = WindowedSerdes.timeWindowedSerdeFrom(Integer.class, Long.MAX_VALUE);
        final byte[] bytes = timeWindowedSerde.serializer().serialize(topic, timeWindowed);
        final Windowed<Integer> windowed = timeWindowedSerde.deserializer().deserialize(topic, bytes);
        assertEquals(timeWindowed, windowed);
    }

    @Test
    public void testSessionWindowedSerdeFrom() {
        final Windowed<Integer> sessionWindowed = new Windowed<>(10, new SessionWindow(0, 1));
        final Serde<Windowed<Integer>> sessionWindowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(Integer.class);
        final byte[] bytes = sessionWindowedSerde.serializer().serialize(topic, sessionWindowed);
        final Windowed<Integer> windowed = sessionWindowedSerde.deserializer().deserialize(topic, bytes);
        assertEquals(sessionWindowed, windowed);
    }

    @Test
    public void timeWindowedSerializerShouldThrowNpeIfNotInitializedProperly() {
        final TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> serializer.serialize("topic", new Windowed<>(new byte[0], new TimeWindow(0, 1))));
        assertThat(
            exception.getMessage(),
            equalTo("Inner serializer is `null`. User code must use constructor " +
                "`TimeWindowedSerializer(final Serializer<T> inner)` instead of the no-arg constructor."));
    }

    @Test
    public void timeWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly() {
        final TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> serializer.serializeBaseKey("topic", new Windowed<>(new byte[0], new TimeWindow(0, 1))));
        assertThat(
            exception.getMessage(),
            equalTo("Inner serializer is `null`. User code must use constructor " +
                "`TimeWindowedSerializer(final Serializer<T> inner)` instead of the no-arg constructor."));
    }

    @Test
    public void timeWindowedDeserializerShouldThrowNpeIfNotInitializedProperly() {
        final TimeWindowedDeserializer<byte[]> deserializer = new TimeWindowedDeserializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> deserializer.deserialize("topic", new byte[0]));
        assertThat(
            exception.getMessage(),
            equalTo("Inner deserializer is `null`. User code must use constructor " +
                "`TimeWindowedDeserializer(final Deserializer<T> inner)` instead of the no-arg constructor."));
    }

    @Test
    public void sessionWindowedSerializerShouldThrowNpeIfNotInitializedProperly() {
        final SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> serializer.serialize("topic", new Windowed<>(new byte[0], new SessionWindow(0, 0))));
        assertThat(
            exception.getMessage(),
            equalTo("Inner serializer is `null`. User code must use constructor " +
                "`SessionWindowedSerializer(final Serializer<T> inner)` instead of the no-arg constructor."));
    }

    @Test
    public void sessionWindowedSerializerShouldThrowNpeOnSerializingBaseKeyIfNotInitializedProperly() {
        final SessionWindowedSerializer<byte[]> serializer = new SessionWindowedSerializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> serializer.serializeBaseKey("topic", new Windowed<>(new byte[0], new SessionWindow(0, 0))));
        assertThat(
            exception.getMessage(),
            equalTo("Inner serializer is `null`. User code must use constructor " +
                "`SessionWindowedSerializer(final Serializer<T> inner)` instead of the no-arg constructor."));
    }

    @Test
    public void sessionWindowedDeserializerShouldThrowNpeIfNotInitializedProperly() {
        final SessionWindowedDeserializer<byte[]> deserializer = new SessionWindowedDeserializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> deserializer.deserialize("topic", new byte[0]));
        assertThat(
            exception.getMessage(),
            equalTo("Inner deserializer is `null`. User code must use constructor " +
                "`SessionWindowedDeserializer(final Deserializer<T> inner)` instead of the no-arg constructor."));
    }

    @Test
    public void timeWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly() {
        new TimeWindowedSerializer<>().close();
    }

    @Test
    public void timeWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly() {
        new TimeWindowedDeserializer<>().close();
    }

    @Test
    public void sessionWindowedSerializerShouldNotThrowOnCloseIfNotInitializedProperly() {
        new SessionWindowedSerializer<>().close();
    }

    @Test
    public void sessionWindowedDeserializerShouldNotThrowOnCloseIfNotInitializedProperly() {
        new SessionWindowedDeserializer<>().close();
    }

}
