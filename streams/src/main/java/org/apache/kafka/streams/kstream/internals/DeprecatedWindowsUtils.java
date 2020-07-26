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

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.EnumerableWindowDefinition;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.internals.RocksDbWindowBytesStoreSupplier;

final class DeprecatedWindowsUtils {

    private DeprecatedWindowsUtils() {}

    @SuppressWarnings("deprecation")
    static <K, VR, W extends Window> RocksDbWindowBytesStoreSupplier supplierFromDeprecatedWindows(
        final String name,
        final EnumerableWindowDefinition<W> windows,
        final MaterializedInternal<K, VR, WindowStore<Bytes, byte[]>> materialized) {

        // old style retention: use deprecated Windows retention/segmentInterval.

        // NOTE: in the future, when we remove Windows#maintainMs(), we should set the default retention
        // to be (windows.size() + windows.grace()). This will yield the same default behavior.

        final long maintainMs = ((org.apache.kafka.streams.kstream.Windows<W>) windows).maintainMs();
        final long size = ((org.apache.kafka.streams.kstream.Windows<W>) windows).size();
        if ((size + windows.gracePeriodMs()) > maintainMs) {
            throw new IllegalArgumentException(
                "The retention period of the window store "
                    + name + " must be no smaller than its window size plus the grace period."
                    + " Got size=[" + size + "],"
                    + " grace=[" + windows.gracePeriodMs() + "],"
                    + " retention=[" + maintainMs + "]");
        }
        return new RocksDbWindowBytesStoreSupplier(
            materialized.storeName(),
            maintainMs,
            Math.max(
                maintainMs / (((org.apache.kafka.streams.kstream.Windows<W>) windows).segments - 1),
                60_000L
            ),
            size,
            false,
            true);
    }

    @SuppressWarnings("deprecation")
    static <W extends Window> boolean isDeprecatedWindows(final EnumerableWindowDefinition<W> windows) {
        return windows instanceof org.apache.kafka.streams.kstream.Windows;
    }
}
