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
package org.apache.kafka.connect.tools;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class MockSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MockSinkTask.class);

    private String mockMode;
    private long startTimeMs;
    private long failureDelayMs;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> config) {
        this.mockMode = config.get(MockConnector.MOCK_MODE_KEY);

        if (MockConnector.TASK_FAILURE.equals(mockMode)) {
            this.startTimeMs = System.currentTimeMillis();

            String delayMsString = config.get(MockConnector.DELAY_MS_KEY);
            this.failureDelayMs = MockConnector.DEFAULT_FAILURE_DELAY_MS;
            if (delayMsString != null)
                failureDelayMs = Long.parseLong(delayMsString);

            log.debug("Started MockSinkTask at {} with failure scheduled in {} ms", startTimeMs, failureDelayMs);
            setTimeout();
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (MockConnector.TASK_FAILURE.equals(mockMode)) {
            long now = System.currentTimeMillis();
            if (now - startTimeMs > failureDelayMs) {
                log.debug("Triggering sink task failure");
                throw new RuntimeException();
            }
            setTimeout();
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void stop() {

    }

    private void setTimeout() {
        // Set a reasonable minimum delay. Since this mock task may not actually consume any data from Kafka, it may only
        // see put() calls triggered by wakeups for offset commits. To make sure we aren't tied to the offset commit
        // interval, we force a wakeup every 250ms or after the failure delay, whichever is smaller. This is not overly
        // aggressive but ensures any scheduled tasks this connector performs are reasonably close to the target time.
        context.timeout(Math.min(failureDelayMs, 250));
    }
}
