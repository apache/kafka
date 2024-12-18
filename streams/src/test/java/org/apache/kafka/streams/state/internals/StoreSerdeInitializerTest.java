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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.internals.WrappingNullableUtils;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.test.MockInternalNewProcessorContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;

public class StoreSerdeInitializerTest {

    private MockedStatic<WrappingNullableUtils> utilsMock;

    @BeforeEach
    public void setup() {
        utilsMock = Mockito.mockStatic(WrappingNullableUtils.class);
    }

    @AfterEach
    public void cleanup() {
        utilsMock.close();
    }

    @Test
    public void shouldPrepareStoreSerdeForProcessorContext() {
        final Serde<String> keySerde = new Serdes.StringSerde();
        final Serde<String> valueSerde = new Serdes.StringSerde();

        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareKeySerde(any(), any())).thenReturn(keySerde);
        utilsMock.when(() -> WrappingNullableUtils.prepareValueSerde(any(), any())).thenReturn(valueSerde);

        final StateSerdes<String, String> result = StoreSerdeInitializer.prepareStoreSerde(
            context, "myStore", "topic", keySerde, valueSerde, WrappingNullableUtils::prepareValueSerde);

        assertThat(result.keySerde(), equalTo(keySerde));
        assertThat(result.valueSerde(), equalTo(valueSerde));
        assertThat(result.topic(), equalTo("topic"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedKeySerdeForProcessorContext() {
        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareKeySerde(any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class,
            () -> StoreSerdeInitializer.prepareStoreSerde(context, "myStore", "topic",
                new Serdes.StringSerde(), new Serdes.StringSerde(), WrappingNullableUtils::prepareValueSerde));

        assertThat(exception.getMessage(), equalTo("Failed to initialize key serdes for store myStore"));
        assertThat(exception.getCause().getMessage(), equalTo("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedValueSerdeForProcessorContext() {
        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareValueSerde(any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class,
            () -> StoreSerdeInitializer.prepareStoreSerde(context, "myStore", "topic",
                new Serdes.StringSerde(), new Serdes.StringSerde(), WrappingNullableUtils::prepareValueSerde));

        assertThat(exception.getMessage(), equalTo("Failed to initialize value serdes for store myStore"));
        assertThat(exception.getCause().getMessage(), equalTo("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedKeySerdeForStateStoreContext() {
        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareKeySerde(any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class,
            () -> StoreSerdeInitializer.prepareStoreSerde(context, "myStore", "topic",
                new Serdes.StringSerde(), new Serdes.StringSerde(), WrappingNullableUtils::prepareValueSerde));

        assertThat(exception.getMessage(), equalTo("Failed to initialize key serdes for store myStore"));
        assertThat(exception.getCause().getMessage(), equalTo("Please set StreamsConfig#DEFAULT_KEY_SERDE_CLASS_CONFIG"));
    }

    @Test
    public void shouldThrowStreamsExceptionOnUndefinedValueSerdeForStateStoreContext() {
        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareValueSerde(any(), any()))
            .thenThrow(new ConfigException("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG"));

        final Throwable exception = assertThrows(StreamsException.class,
            () -> StoreSerdeInitializer.prepareStoreSerde(context, "myStore", "topic",
                new Serdes.StringSerde(), new Serdes.StringSerde(), WrappingNullableUtils::prepareValueSerde));

        assertThat(exception.getMessage(), equalTo("Failed to initialize value serdes for store myStore"));
        assertThat(exception.getCause().getMessage(), equalTo("Please set StreamsConfig#DEFAULT_VALUE_SERDE_CLASS_CONFIG"));
    }

    @Test
    public void shouldThrowStreamsExceptionWithExplicitErrorMessageForProcessorContext() {
        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareKeySerde(any(), any())).thenThrow(new StreamsException(""));

        final Throwable exception = assertThrows(StreamsException.class,
            () -> StoreSerdeInitializer.prepareStoreSerde(context, "myStore", "topic",
                new Serdes.StringSerde(), new Serdes.StringSerde(), WrappingNullableUtils::prepareValueSerde));

        assertThat(exception.getMessage(), equalTo("Failed to initialize key serdes for store myStore"));
    }

    @Test
    public void shouldThrowStreamsExceptionWithExplicitErrorMessageForStateStoreContext() {
        final MockInternalNewProcessorContext<String, String> context = new MockInternalNewProcessorContext<>();

        utilsMock.when(() -> WrappingNullableUtils.prepareValueSerde(any(), any())).thenThrow(new StreamsException(""));

        final Throwable exception = assertThrows(StreamsException.class,
            () -> StoreSerdeInitializer.prepareStoreSerde(context, "myStore", "topic",
                new Serdes.StringSerde(), new Serdes.StringSerde(), WrappingNullableUtils::prepareValueSerde));

        assertThat(exception.getMessage(), equalTo("Failed to initialize value serdes for store myStore"));
    }
}
