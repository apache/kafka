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
package org.apache.kafka.test;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;

import org.junit.jupiter.params.provider.Arguments;

import java.util.Properties;
import java.util.stream.Stream;

/**
 * Utility methods for testing with different store formats (default/timestamped and headers-aware).
 */
public class StoreFormatTestUtils {

    /**
     * Provides test parameters for different store formats.
     * Returns a stream of arguments containing "default" and "headers" format strings.
     *
     * @return Stream of Arguments for parametrized tests
     */
    public static Stream<Arguments> storeFormats() {
        return Stream.of(
            Arguments.of("default"),
            Arguments.of("headers")
        );
    }

    /**
     * Creates test properties configured for the specified store format.
     *
     * @param storeFormat the store format ("default" or "headers")
     * @param keySerde the key serde
     * @param valueSerde the value serde
     * @return Properties configured with the specified store format and caching disabled
     */
    public static Properties getProps(final String storeFormat, final Serde<?> keySerde, final Serde<?> valueSerde) {
        final Properties properties = StreamsTestUtils.getStreamsConfig(keySerde, valueSerde);
        properties.setProperty(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0");
        properties.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, storeFormat);
        return properties;
    }

    /**
     * Creates test headers with a single key-value pair.
     *
     * @param key the header key
     * @param value the header value
     * @return Headers containing the specified key-value pair
     */
    public static Headers makeHeaders(final String key, final String value) {
        final RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(key, value.getBytes()));
        return headers;
    }
}
