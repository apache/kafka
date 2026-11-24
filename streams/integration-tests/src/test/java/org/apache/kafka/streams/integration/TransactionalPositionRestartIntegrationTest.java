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
package org.apache.kafka.streams.integration;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.provider.Arguments;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Exercises the transactional (KIP-892) Position-across-restart path, reusing all of
 * {@link PositionRestartIntegrationTest}'s machinery (topology building, produce, restart, IQ position
 * verification) by overriding two hooks: {@link #transactional()} (turns on
 * {@code enable.transactional.statestores} + {@code exactly_once_v2}) and {@link #data()} (supplies a
 * transactional-only matrix).
 *
 * <p>Keeping this separate from the parent keeps the (large) non-transactional matrix and the transactional
 * matrix in independent test targets, each with its own time budget, rather than multiplying one big
 * cross-product.
 */
@Tag("integration")
@Timeout(600)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TransactionalPositionRestartIntegrationTest extends PositionRestartIntegrationTest {

    @Override
    protected boolean transactional() {
        return true;
    }

    @Override
    protected Stream<Arguments> data() {
        // Every persistent and in-memory store family, crossed with caching on/off. log=true throughout so
        // in-memory stores survive the restart via changelog restore; kind is rotated (not crossed) since it
        // is store-level-independent for the Position API.
        final List<StoresToTest> stores = List.of(
            StoresToTest.ROCKS_KV,
            StoresToTest.TIME_ROCKS_KV,
            StoresToTest.ROCKS_WINDOW,
            StoresToTest.TIME_ROCKS_WINDOW,
            StoresToTest.ROCKS_SESSION,
            StoresToTest.IN_MEMORY_KV,
            StoresToTest.IN_MEMORY_WINDOW,
            StoresToTest.IN_MEMORY_SESSION
        );
        final List<Arguments> values = new ArrayList<>();
        int i = 0;
        for (final StoresToTest store : stores) {
            for (final boolean cache : List.of(false, true)) {
                final String kind = (i++ % 2 == 0) ? "DSL" : "PAPI";
                values.add(Arguments.of(cache, true, store, kind));
            }
        }
        return values.stream();
    }
}
