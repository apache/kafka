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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.data.Schema;

import java.util.Map;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.time.Duration;

/** Emits heartbeats. */
public class MirrorHeartbeatTask extends SourceTask {
    private String sourceClusterAlias;
    private String targetClusterAlias;
    private String heartbeatsTopic;
    private Duration interval;
    private CountDownLatch stopped;

    @Override
    public void start(Map<String, String> props) {
        stopped = new CountDownLatch(1);
        MirrorTaskConfig config = new MirrorTaskConfig(props);
        sourceClusterAlias = config.sourceClusterAlias();
        targetClusterAlias = config.targetClusterAlias();
        heartbeatsTopic = config.heartbeatsTopic();
        interval = config.emitHeartbeatsInterval();
    }

    @Override
    public void commit() throws InterruptedException {
        // nop
    }

    @Override
    public void stop() {
        stopped.countDown();
    }

    @Override
    public String version() {
        return "1";
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        // pause to throttle, unless we've stopped
        if (stopped.await(interval.toMillis(), TimeUnit.MILLISECONDS)) {
            // SourceWorkerTask expects non-zero batches or null
            return null;
        }
        long timestamp = System.currentTimeMillis();
        Heartbeat heartbeat = new Heartbeat(sourceClusterAlias, targetClusterAlias, timestamp);
        SourceRecord record = new SourceRecord(
            heartbeat.connectPartition(), MirrorUtils.wrapOffset(0),
            heartbeatsTopic, 0,
            Schema.BYTES_SCHEMA, heartbeat.recordKey(),
            Schema.BYTES_SCHEMA, heartbeat.recordValue(),
            timestamp);
        return Collections.singletonList(record);
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
    }
}
