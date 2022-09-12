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

import java.io.File;
import java.util.Map;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

public class TransactionalTimestampedSegment extends AbstractTransactionalSegment {
    TransactionalTimestampedSegment(final String segmentName,
                                    final String windowName,
                                    final long segmentId,
                                    final RocksDBMetricsRecorder metricsRecorder) {
        super(segmentName, windowName, segmentId, metricsRecorder);
    }

    @Override
    Segment createMainStore(final String segmentName, final String windowName, final long segmentId,
        final RocksDBMetricsRecorder metricsRecorder) {
        return new TimestampedSegment(segmentName, windowName, segmentId, metricsRecorder);
    }

    @Override
    public void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void openDB(final Map<String, Object> configs, final File stateDir) {
        doInit(configs, stateDir);
        ((TimestampedSegment) mainStore).openDB(configs, stateDir);
    }

    @Override
    public String toString() {
        return "TransactionalTimestampedSegment(id=" + mainStore.id() + ", name=" + name() + ")";
    }
}
