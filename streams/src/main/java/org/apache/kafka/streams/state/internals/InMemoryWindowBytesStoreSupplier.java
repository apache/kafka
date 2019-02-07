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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;

public class InMemoryWindowBytesStoreSupplier implements WindowBytesStoreSupplier {
    private final String name;
    private final long retentionPeriod;
    private final long windowSize;
    private final long gracePeriod;
    private final long segmentInterval;

    public InMemoryWindowBytesStoreSupplier(final String name,
                                            final long retentionPeriod,
                                            final long windowSize,
                                            final long gracePeriod) {
        this.name = name;
        this.retentionPeriod = retentionPeriod;
        this.windowSize = windowSize;
        this.gracePeriod = gracePeriod;
        this.segmentInterval = 1;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public WindowStore<Bytes, byte[]> get() {
        return new InMemoryWindowStore<>(name,
                                         Serdes.Bytes(),
                                         Serdes.ByteArray(),
                                         retentionPeriod,
                                         windowSize,
                                         gracePeriod,
                                         metricsScope());
    }

    @Override
    public String metricsScope() {
        return "in-memory-window-state";
    }

    @Deprecated
    @Override
    public int segments() {
        return (int) (retentionPeriod / segmentInterval);
    }

    @Override
    public long retentionPeriod() {
        return retentionPeriod;
    }


    @Override
    public long windowSize() {
        return windowSize;
    }

    public long gracePeriod() {
        return gracePeriod;
    }

    @Override
    public long segmentIntervalMs() {
        return segmentInterval;
    }

    @Override
    public boolean retainDuplicates() {
        return false;
    }
}
