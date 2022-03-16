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
package org.apache.kafka.common.metrics;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientTelemetryMetricsReporter implements MetricsReporter {

    public static final String OUTPUT_DIRECTORY_CONFIG = "telemetry.output.dir";

    private static final Logger log = LoggerFactory.getLogger(ClientTelemetryMetricsReporter.class);

    private File outputDirectory;

    @Override
    public void configure(Map<String, ?> configs) {
        String outputDirectoryName = (String) configs.get(OUTPUT_DIRECTORY_CONFIG);
        outputDirectory = new File(outputDirectoryName);

        log.info("configure - outputDirectory: {}", outputDirectory);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        log.debug("init - metrics: {}", metrics);
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        log.debug("metricChange - metric: {}", metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        log.debug("metricRemoval - metric: {}", metric);
    }

    @Override
    public void close() {
        log.info("close");
    }

    @Override
    public ClientTelemetryReceiver clientReceiver() {
        log.info("clientReceiver");

        return (context, payload) -> {
            log.info("exportMetrics - context: {}, payload: {}", context, payload);

            if (payload != null && payload.data() != null) {
                log.info("exportMetrics - payload.clientInstanceId: {}", payload.clientInstanceId());
                log.info("exportMetrics - payload.contentType: {}", payload.contentType());
                log.info("exportMetrics - payload.isTerminating: {}", payload.isTerminating());
                log.info("exportMetrics - payload.data: {}", payload.data());

                String fileName = "metrics-" + TestUtils.randomString(16) + ".otlp";
                log.info("exportMetrics - fileName: {}", fileName);
                File file = new File(outputDirectory, fileName);
                log.info("exportMetrics - file: {}", file);
                file.deleteOnExit();

                try (FileOutputStream out = new FileOutputStream(file);
                    FileChannel channel = out.getChannel()) {
                    log.info("exportMetrics - writing payload to {}", file);
                    Utils.writeFully(channel, payload.data());
                } catch (Throwable t) {
                    log.warn(t.getMessage(), t);
                }
            } else {
                log.info("exportMetrics - could not write payload - payload or its data was null");
            }
        };
    }
}
