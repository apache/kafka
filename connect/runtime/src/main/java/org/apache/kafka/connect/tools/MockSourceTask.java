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

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockSourceTask extends SourceTask {
    private static final Logger log = LoggerFactory.getLogger(MockSourceTask.class);

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

            log.debug("Started MockSourceTask at {} with failure scheduled in {} ms", startTimeMs, failureDelayMs);
        }
    }

    @Override
    public List<SourceRecord> poll() {
        if (MockConnector.TASK_FAILURE.equals(mockMode)) {
            long now = System.currentTimeMillis();
            if (now - startTimeMs > failureDelayMs) {
                log.debug("Triggering source task failure");
                throw new RuntimeException();
            }
        }
        return Collections.emptyList();
    }

    @Override
    public void stop() {

    }
}
