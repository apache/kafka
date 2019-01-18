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

import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MirrorMonitorTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorMonitorTask.class);

    private MirrorMetrics metrics;
    private String sourceClusterAlias;
    private String targetClusterAlias;

    @Override
    public void start(Map<String, String> props) {
        MirrorConnectorConfig config = new MirrorConnectorConfig(props);
        sourceClusterAlias = config.sourceClusterAlias();
        targetClusterAlias = config.targetClusterAlias();
        metrics = MirrorMetrics.metricsFor(sourceClusterAlias, targetClusterAlias);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            metrics.replicationLag(System.currentTimeMillis() - record.timestamp());
        }
    }

    @Override
    public void stop() {
        // this space intentionally left blank
    }

    @Override
    public String version() {
        return "WIP";
    }
}
