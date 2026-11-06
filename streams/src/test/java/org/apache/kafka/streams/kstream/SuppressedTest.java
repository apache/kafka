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

import org.apache.kafka.streams.kstream.internals.suppress.EagerBufferConfigImpl;
import org.apache.kafka.streams.kstream.internals.suppress.FinalResultsSuppressionBuilder;
import org.apache.kafka.streams.kstream.internals.suppress.StrictBufferConfigImpl;
import org.apache.kafka.streams.kstream.internals.suppress.SuppressedInternal;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.apache.kafka.streams.kstream.internals.suppress.BufferFullStrategy.SHUT_DOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SuppressedTest {

    @Test
    public void bufferBuilderShouldBeConsistent() {
        assertEquals(
            unbounded(),
            maxBytes(2L).withMaxRecords(4L).withNoBound(),
            "noBound should remove bounds"
        );

        assertEquals(
            new EagerBufferConfigImpl(2L, MAX_VALUE, Map.of()),
            maxRecords(2L),
            "keys alone should be set"
        );

        assertEquals(
            new EagerBufferConfigImpl(MAX_VALUE, 2L, Map.of()),
            maxBytes(2L),
            "size alone should be set"
        );

        assertEquals(
            new EagerBufferConfigImpl(2L, 4L, Map.of("myConfigKey", "myConfigValue")),
            maxRecords(2L).withMaxBytes(4L).withLoggingEnabled(Map.of("myConfigKey", "myConfigValue")),
            "config should be set even after max records"
        );
    }

    @Test
    public void intermediateEventsShouldAcceptAnyBufferAndSetBounds() {
        assertEquals(
            new SuppressedInternal<>("myname", ofMillis(2), unbounded(), null, false),
            untilTimeLimit(ofMillis(2), unbounded()).withName("myname"),
            "name should be set"
        );

        assertEquals(
            new SuppressedInternal<>(null, ofMillis(2), unbounded(), null, false),
            untilTimeLimit(ofMillis(2), unbounded()),
            "time alone should be set"
        );

        assertEquals(
            new SuppressedInternal<>(null, ofMillis(2), unbounded(), null, false),
            untilTimeLimit(ofMillis(2), unbounded()),
            "time and unbounded buffer should be set"
        );

        assertEquals(
            new SuppressedInternal<>(null, ofMillis(2), maxRecords(2), null, false),
            untilTimeLimit(ofMillis(2), maxRecords(2)),
            "time and keys buffer should be set"
        );

        assertEquals(
            new SuppressedInternal<>(null, ofMillis(2), maxBytes(2), null, false),
            untilTimeLimit(ofMillis(2), maxBytes(2)),
            "time and size buffer should be set"
        );

        assertEquals(
            new SuppressedInternal<>(null, ofMillis(2), new EagerBufferConfigImpl(3L, 2L, Map.of()), null, false),
            untilTimeLimit(ofMillis(2L), maxRecords(3L).withMaxBytes(2L)),
            "all constraints should be set"
        );

        assertEquals(
            new SuppressedInternal<>(null, ofMillis(2), new EagerBufferConfigImpl(2L, MAX_VALUE, Map.of("myConfigKey", "myConfigValue")), null, false),
            untilTimeLimit(ofMillis(2), maxRecords(2L).withLoggingEnabled(Map.of("myConfigKey", "myConfigValue")).emitEarlyWhenFull()),
            "config is not lost early emit is set"
        );
    }

    @Test
    public void finalEventsShouldAcceptStrictBuffersAndSetBounds() {

        assertEquals(
            new FinalResultsSuppressionBuilder<>(null, unbounded()),
            untilWindowCloses(unbounded())
        );

        assertEquals(
            new FinalResultsSuppressionBuilder<>(null, new StrictBufferConfigImpl(2L, MAX_VALUE, SHUT_DOWN, Map.of())),
            untilWindowCloses(maxRecords(2L).shutDownWhenFull())
        );

        assertEquals(
            new FinalResultsSuppressionBuilder<>(null, new StrictBufferConfigImpl(MAX_VALUE, 2L, SHUT_DOWN, Map.of())),
            untilWindowCloses(maxBytes(2L).shutDownWhenFull())
        );

        assertEquals(
            new FinalResultsSuppressionBuilder<>("name", unbounded()),
            untilWindowCloses(unbounded()).withName("name")
        );

        assertEquals(
            new FinalResultsSuppressionBuilder<>("name", new StrictBufferConfigImpl(2L, MAX_VALUE, SHUT_DOWN, Map.of())),
            untilWindowCloses(maxRecords(2L).shutDownWhenFull()).withName("name")
        );

        assertEquals(
            new FinalResultsSuppressionBuilder<>("name", new StrictBufferConfigImpl(MAX_VALUE, 2L, SHUT_DOWN, Map.of())),
            untilWindowCloses(maxBytes(2L).shutDownWhenFull()).withName("name")
        );

        assertEquals(
            new FinalResultsSuppressionBuilder<>(null, new StrictBufferConfigImpl(MAX_VALUE, 2L, SHUT_DOWN, Map.of("myConfigKey", "myConfigValue"))),
            untilWindowCloses(maxBytes(2L).withLoggingEnabled(Map.of("myConfigKey", "myConfigValue")).shutDownWhenFull()),
            "config is not lost when shutdown when full is set"
        );
    }

    @Test
    public void supportLongChainOfMethods() {
        final Suppressed.BufferConfig<Suppressed.EagerBufferConfig> bufferConfig = unbounded()
            .emitEarlyWhenFull()
            .withMaxRecords(3L)
            .withMaxBytes(4L)
            .withMaxRecords(5L)
            .withMaxBytes(6L);

        assertEquals(
            new EagerBufferConfigImpl(5L, 6L, Map.of()),
            bufferConfig,
            "long chain of eager buffer config sets attributes properly"
        );
        assertEquals(
            new StrictBufferConfigImpl(5L, 6L, SHUT_DOWN, Map.of()),
            bufferConfig.shutDownWhenFull(),
            "long chain of strict buffer config sets attributes properly"
        );

        final Suppressed.BufferConfig<Suppressed.EagerBufferConfig> bufferConfigWithLogging = unbounded()
            .withLoggingEnabled(Map.of("myConfigKey", "myConfigValue"))
            .emitEarlyWhenFull()
            .withMaxRecords(3L)
            .withMaxBytes(4L)
            .withMaxRecords(5L)
            .withMaxBytes(6L);

        assertEquals(
            new EagerBufferConfigImpl(5L, 6L, Map.of("myConfigKey", "myConfigValue")),
            bufferConfigWithLogging,
            "long chain of eager buffer config sets attributes properly with logging enabled"
        );
        assertEquals(
            new StrictBufferConfigImpl(5L, 6L, SHUT_DOWN, Map.of("myConfigKey", "myConfigValue")),
            bufferConfigWithLogging.shutDownWhenFull(),
            "long chain of strict buffer config sets attributes properly with logging enabled"
        );

        final Suppressed.BufferConfig<Suppressed.EagerBufferConfig> bufferConfigWithLoggingCalledAtTheEnd = unbounded()
            .emitEarlyWhenFull()
            .withMaxRecords(3L)
            .withMaxBytes(4L)
            .withMaxRecords(5L)
            .withMaxBytes(6L)
            .withLoggingEnabled(Map.of("myConfigKey", "myConfigValue"));

        assertEquals(
            new EagerBufferConfigImpl(5L, 6L, Map.of("myConfigKey", "myConfigValue")),
            bufferConfigWithLoggingCalledAtTheEnd,
            "long chain of eager buffer config sets logging even after other setters"
        );
        assertEquals(
            new StrictBufferConfigImpl(5L, 6L, SHUT_DOWN, Map.of("myConfigKey", "myConfigValue")),
            bufferConfigWithLoggingCalledAtTheEnd.shutDownWhenFull(),
            "long chain of strict buffer config sets logging even after other setters"
        );
    }
}
