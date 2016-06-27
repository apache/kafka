/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package org.apache.kafka.connect.tools;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.Map;

public class MockSinkTask extends SinkTask {

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
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (MockConnector.TASK_FAILURE.equals(mockMode)) {
            long now = System.currentTimeMillis();
            if (now > startTimeMs + failureDelayMs)
                throw new RuntimeException();
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void stop() {

    }
}
