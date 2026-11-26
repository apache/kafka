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
package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.internals.SerdeGetter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class LateBoundWindowedSerdeTest {
    private static final String TOPIC = "topic";
    private static final long WINDOW_SIZE = 100L;
    private static final Windowed<String> TIME_WINDOWED_KEY = new Windowed<>("key", new TimeWindow(200L, 300L));
    private static final Windowed<String> SESSION_WINDOWED_KEY = new Windowed<>("key", new SessionWindow(200L, 250L));

    private static Stream<Arguments> windowedSerdes() {
        final Function<Serde<String>, LateBoundWindowedSerde<String>> time = inner -> new FullTimeWindowedSerde<>(inner, WINDOW_SIZE);
        final Function<Serde<String>, LateBoundWindowedSerde<String>> session = FullSessionWindowedSerde::new;
        return Stream.of(
            Arguments.of(time, TIME_WINDOWED_KEY),
            Arguments.of(session, SESSION_WINDOWED_KEY)
        );
    }

    @SuppressWarnings("unchecked")
    private static SerdeGetter getterWithDefaultKeySerde(final Serde<?> defaultKeySerde) {
        final ProcessorContext<?, ?> context = mock(ProcessorContext.class);
        doReturn(defaultKeySerde).when(context).keySerde();
        return new SerdeGetter(context);
    }

    // Scenario: the user supplied the inner key serde. Expected: windowed keys round-trip without any default serde.
    @ParameterizedTest
    @MethodSource("windowedSerdes")
    public void shouldRoundTripWithExplicitInnerSerde(final Function<Serde<String>, LateBoundWindowedSerde<String>> factory,
                                                      final Windowed<String> key) {
        final LateBoundWindowedSerde<String> serde = factory.apply(Serdes.String());

        final byte[] bytes = serde.serializer().serialize(TOPIC, key);

        assertEquals(key, serde.deserializer().deserialize(TOPIC, bytes));
    }

    // Scenario: no inner serde at build time (defaults only), completed from the default key serde at runtime.
    // Expected: windowed keys round-trip using the default serde.
    @ParameterizedTest
    @MethodSource("windowedSerdes")
    public void shouldResolveInnerSerdeFromDefaultKeySerde(final Function<Serde<String>, LateBoundWindowedSerde<String>> factory,
                                                           final Windowed<String> key) {
        final LateBoundWindowedSerde<String> serde = factory.apply(null);

        serde.setIfUnset(getterWithDefaultKeySerde(Serdes.String()));

        final byte[] bytes = serde.serializer().serialize(TOPIC, key);
        assertEquals(key, serde.deserializer().deserialize(TOPIC, bytes));
    }

    // Scenario: the inner serde was supplied by the user and a different default exists.
    // Expected: setIfUnset does not replace the user's serde (a String serde would fail on a Long key).
    @Test
    public void shouldNotOverrideExplicitInnerSerde() {
        final SerdeGetter stringDefault = getterWithDefaultKeySerde(Serdes.String());
        final LateBoundWindowedSerde<Long> timeSerde = new FullTimeWindowedSerde<>(Serdes.Long(), WINDOW_SIZE);
        final LateBoundWindowedSerde<Long> sessionSerde = new FullSessionWindowedSerde<>(Serdes.Long());
        final Windowed<Long> timeKey = new Windowed<>(42L, new TimeWindow(200L, 300L));
        final Windowed<Long> sessionKey = new Windowed<>(42L, new SessionWindow(200L, 250L));

        timeSerde.setIfUnset(stringDefault);
        sessionSerde.setIfUnset(stringDefault);

        assertEquals(timeKey, timeSerde.deserializer().deserialize(TOPIC, timeSerde.serializer().serialize(TOPIC, timeKey)));
        assertEquals(sessionKey, sessionSerde.deserializer().deserialize(TOPIC, sessionSerde.serializer().serialize(TOPIC, sessionKey)));
    }

    // Scenario: the inner serde is still unset. Expected: a clear IllegalStateException, never a NullPointerException.
    @ParameterizedTest
    @MethodSource("windowedSerdes")
    public void shouldFailClearlyWhileInnerSerdeIsUnset(final Function<Serde<String>, LateBoundWindowedSerde<String>> factory,
                                                        final Windowed<String> key) {
        final LateBoundWindowedSerde<String> serde = factory.apply(null);

        assertThrows(IllegalStateException.class, () -> serde.serializer().serialize(TOPIC, key));
        assertThrows(IllegalStateException.class, () -> serde.deserializer().deserialize(TOPIC, new byte[0]));
    }

    // Scenario: configure/close are called on a serde whose inner serde is still unset.
    // Expected: they are no-ops instead of failing for the missing inner serde.
    @ParameterizedTest
    @MethodSource("windowedSerdes")
    public void shouldAllowConfigureAndCloseWhileInnerSerdeIsUnset(final Function<Serde<String>, LateBoundWindowedSerde<String>> factory) {
        final LateBoundWindowedSerde<String> serde = factory.apply(null);

        assertDoesNotThrow(() -> serde.configure(Collections.emptyMap(), true));
        assertDoesNotThrow(serde::close);
    }

    // Scenario: sinks pick a WindowedStreamPartitioner only for WindowedSerializers.
    // Expected: the serializer is one and exposes the base (non-windowed) key, so records partition by their base key.
    @ParameterizedTest
    @MethodSource("windowedSerdes")
    @SuppressWarnings("unchecked")
    public void shouldBeWindowedSerializerThatExposesBaseKey(final Function<Serde<String>, LateBoundWindowedSerde<String>> factory,
                                                             final Windowed<String> key) {
        final LateBoundWindowedSerde<String> serde = factory.apply(null);
        serde.setIfUnset(getterWithDefaultKeySerde(Serdes.String()));

        final WindowedSerializer<String> serializer = assertInstanceOf(WindowedSerializer.class, serde.serializer());

        assertArrayEquals(Serdes.String().serializer().serialize(TOPIC, key.key()), serializer.serializeBaseKey(TOPIC, key));
    }
}
