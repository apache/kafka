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
package org.apache.kafka.jmh.coordinator;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.util.Scheduler;
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache;
import org.apache.kafka.storage.internals.log.FetchDataInfo;
import org.apache.kafka.storage.internals.log.LocalLog;
import org.apache.kafka.storage.internals.log.LogOffsetsListener;
import org.apache.kafka.storage.internals.log.LogSegments;
import org.apache.kafka.storage.internals.log.ProducerStateManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.IOException;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class MockLog extends UnifiedLog {

    public MockLog(TopicPartition tp) throws IOException {
        super(
            0,
            createMockLocalLog(tp),
            mock(BrokerTopicStats.class),
            Integer.MAX_VALUE,
            mock(LeaderEpochFileCache.class),
            mock(ProducerStateManager.class),
            Optional.empty(),
            false,
            LogOffsetsListener.NO_OP_OFFSETS_LISTENER
        );
    }

    @Override
    public abstract long logStartOffset();

    @Override
    public abstract long logEndOffset();

    @Override
    public long highWatermark() {
        return logEndOffset();
    }

    @Override
    public abstract FetchDataInfo read(long startOffset, int maxLength, FetchIsolation isolation, boolean minOneMessage);

    private static LocalLog createMockLocalLog(TopicPartition tp) {
        LocalLog localLog = mock(LocalLog.class);
        when(localLog.scheduler()).thenReturn(mock(Scheduler.class));
        when(localLog.segments()).thenReturn(mock(LogSegments.class));
        when(localLog.topicPartition()).thenReturn(tp);
        return localLog;
    }
}
