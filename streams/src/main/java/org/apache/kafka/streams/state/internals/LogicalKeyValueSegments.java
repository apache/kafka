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

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

public class LogicalKeyValueSegments extends AbstractSegments<LogicalKeyValueSegment> {

    private final RocksDBMetricsRecorder metricsRecorder;
    private final RocksDBStore physicalStore;

    LogicalKeyValueSegments(final String name,
                            final String parentDir,
                            final long retentionPeriod,
                            final long segmentInterval,
                            final RocksDBMetricsRecorder metricsRecorder) {
        super(name, retentionPeriod, segmentInterval);
        this.metricsRecorder = metricsRecorder;
        this.physicalStore = new RocksDBStore(name, parentDir, metricsRecorder, false);
    }

    @Override
    public LogicalKeyValueSegment getOrCreateSegment(final long segmentId,
                                                     final ProcessorContext context) {
        if (segments.containsKey(segmentId)) {
            return segments.get(segmentId);
        } else {
            final LogicalKeyValueSegment newSegment = new LogicalKeyValueSegment(segmentId, segmentName(segmentId), physicalStore);

            if (segments.put(segmentId, newSegment) != null) {
                throw new IllegalStateException("LogicalKeyValueSegment already exists. Possible concurrent access.");
            }

            return newSegment;
        }
    }

    @Override
    public void openExisting(final ProcessorContext context, final long streamTime) {
        metricsRecorder.init(ProcessorContextUtils.getMetricsImpl(context), context.taskId());
        physicalStore.openDB(context.appConfigs(), context.stateDir());
    }

    @Override
    public void flush() {
        physicalStore.flush();
    }

    @Override
    public void close() {
        // close the logical segments first to close any open iterators
        super.close();
        physicalStore.close();
    }
}