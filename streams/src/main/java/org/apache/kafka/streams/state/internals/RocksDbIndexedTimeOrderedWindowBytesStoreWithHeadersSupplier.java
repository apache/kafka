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

import org.apache.kafka.streams.state.HeadersBytesStoreSupplier;

import java.time.Duration;
import java.util.Objects;

import static org.apache.kafka.streams.internals.ApiUtils.prepareMillisCheckFailMsgPrefix;
import static org.apache.kafka.streams.internals.ApiUtils.validateMillisecondDuration;

public class RocksDbIndexedTimeOrderedWindowBytesStoreWithHeadersSupplier
    extends RocksDbIndexedTimeOrderedWindowBytesStoreSupplier
    implements HeadersBytesStoreSupplier {

    public static RocksDbIndexedTimeOrderedWindowBytesStoreWithHeadersSupplier create(
        final String name,
        final Duration retentionPeriod,
        final Duration windowSize,
        final boolean retainDuplicates,
        final boolean hasIndex
    ) {
        Objects.requireNonNull(name, "name cannot be null");
        final String rpMsgPrefix = prepareMillisCheckFailMsgPrefix(retentionPeriod, "retentionPeriod");
        final long retentionMs = validateMillisecondDuration(retentionPeriod, rpMsgPrefix);
        final String wsMsgPrefix = prepareMillisCheckFailMsgPrefix(windowSize, "windowSize");
        final long windowSizeMs = validateMillisecondDuration(windowSize, wsMsgPrefix);

        final long defaultSegmentInterval = Math.max(retentionMs / 2, 60_000L);

        if (retentionMs < 0L) {
            throw new IllegalArgumentException("retentionPeriod cannot be negative");
        }
        if (windowSizeMs < 0L) {
            throw new IllegalArgumentException("windowSize cannot be negative");
        }
        if (windowSizeMs > retentionMs) {
            throw new IllegalArgumentException("The retention period of the window store "
                + name + " must be no smaller than its window size. Got size=["
                + windowSizeMs + "], retention=[" + retentionMs + "]");
        }

        return new RocksDbIndexedTimeOrderedWindowBytesStoreWithHeadersSupplier(
            name,
            retentionMs,
            defaultSegmentInterval,
            windowSizeMs,
            retainDuplicates,
            hasIndex,
            true
        );
    }

    private RocksDbIndexedTimeOrderedWindowBytesStoreWithHeadersSupplier(
        final String name,
        final long retentionPeriod,
        final long segmentInterval,
        final long windowSize,
        final boolean retainDuplicates,
        final boolean withIndex,
        final boolean withHeaders
    ) {
        super(
            name,
            retentionPeriod,
            segmentInterval,
            windowSize,
            retainDuplicates,
            withIndex,
            withHeaders
        );
    }

    @Override
    public String toString() {
        return "RocksDbIndexedTimeOrderedWindowBytesStoreWithHeadersSupplier{" +
                   "name='" + name + '\'' +
                   ", retentionPeriod=" + retentionPeriod +
                   ", segmentInterval=" + segmentInterval +
                   ", windowSize=" + windowSize +
                   ", retainDuplicates=" + retainDuplicates +
                   ", windowStoreType=" + windowStoreType +
                   '}';
    }
}
