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
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class WindowedSerdesTest {

    private final String topic = "sample";

    @Test
    public void shouldWrapForTimeWindowedSerde() {
        final Serde<Windowed<String>> serde = WindowedSerdes.timeWindowedSerdeFrom(String.class);
        assertTrue(serde.serializer() instanceof TimeWindowedSerializer);
        assertTrue(serde.deserializer() instanceof TimeWindowedDeserializer);
        assertTrue(((TimeWindowedSerializer) serde.serializer()).innerSerializer() instanceof StringSerializer);
        assertTrue(((TimeWindowedDeserializer) serde.deserializer()).innerDeserializer() instanceof StringDeserializer);
    }

    @Test
    public void shouldWrapForSessionWindowedSerde() {
        final Serde<Windowed<String>> serde = WindowedSerdes.sessionWindowedSerdeFrom(String.class);
        assertTrue(serde.serializer() instanceof SessionWindowedSerializer);
        assertTrue(serde.deserializer() instanceof SessionWindowedDeserializer);
        assertTrue(((SessionWindowedSerializer) serde.serializer()).innerSerializer() instanceof StringSerializer);
        assertTrue(((SessionWindowedDeserializer) serde.deserializer()).innerDeserializer() instanceof StringDeserializer);
    }

    @Test
    public void testTimeWindowSerdeFrom() {
        final Windowed<Integer> timeWindowed = new Windowed<>(10, Window.withBounds(0, Long.MAX_VALUE));
        final Serde<Windowed<Integer>> timeWindowedSerde = WindowedSerdes.timeWindowedSerdeFrom(Integer.class);
        final byte[] bytes = timeWindowedSerde.serializer().serialize(topic, timeWindowed);
        final Windowed<Integer> windowed = timeWindowedSerde.deserializer().deserialize(topic, bytes);
        Assert.assertEquals(timeWindowed, windowed);
    }

    @Test
    public void testSessionWindowedSerdeFrom() {
        final Windowed<Integer> sessionWindowed = new Windowed<>(10, Window.withBounds(0, 1));
        final Serde<Windowed<Integer>> sessionWindowedSerde = WindowedSerdes.sessionWindowedSerdeFrom(Integer.class);
        final byte[] bytes = sessionWindowedSerde.serializer().serialize(topic, sessionWindowed);
        final Windowed<Integer> windowed = sessionWindowedSerde.deserializer().deserialize(topic, bytes);
        Assert.assertEquals(sessionWindowed, windowed);
    }

    @Test
    public void timeWindowedSerializerShouldThrowNpeIfNotInitializedProperly() {
        final TimeWindowedSerializer<byte[]> serializer = new TimeWindowedSerializer<>();
        final NullPointerException exception = assertThrows(
            NullPointerException.class,
            () -> serializer.serialize("topic", new Windowed<>(new byte[0], Window.withBounds(0, 1))));
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
            () -> serializer.serializeBaseKey("topic", new Windowed<>(new byte[0], Window.withBounds(0, 1))));
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
            () -> serializer.serialize("topic", new Windowed<>(new byte[0], Window.withBounds(0, 0))));
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
            () -> serializer.serializeBaseKey("topic", new Windowed<>(new byte[0], Window.withBounds(0, 0))));
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
