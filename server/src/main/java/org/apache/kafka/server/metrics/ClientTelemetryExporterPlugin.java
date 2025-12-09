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
package org.apache.kafka.server.metrics;

import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Plugin to register client telemetry receivers/exporters and export metrics. This class is used by the Kafka
 * server to export client metrics to the registered receivers and exporters, supporting both the deprecated
 * {@link ClientTelemetryReceiver} and the new {@link ClientTelemetryExporter} interfaces.
 */
@SuppressWarnings({"deprecation", "overloads", "removal"})
public class ClientTelemetryExporterPlugin {

    private final List<ClientTelemetryReceiver> receivers;
    private final List<ClientTelemetryExporter> exporters;

    public ClientTelemetryExporterPlugin() {
        this.receivers = Collections.synchronizedList(new ArrayList<>());
        this.exporters = Collections.synchronizedList(new ArrayList<>());
    }

    public boolean isEmpty() {
        return receivers.isEmpty() && exporters.isEmpty();
    }

    public void add(ClientTelemetryReceiver receiver) {
        receivers.add(receiver);
    }

    public void add(ClientTelemetryExporter exporter) {
        exporters.add(exporter);
    }

    public DefaultClientTelemetryPayload getPayLoad(PushTelemetryRequest request) {
        return new DefaultClientTelemetryPayload(request);
    }

    public void exportMetrics(RequestContext context, PushTelemetryRequest request, int pushIntervalMs) {
        DefaultClientTelemetryPayload payload = getPayLoad(request);

        // Export to deprecated receivers
        for (ClientTelemetryReceiver receiver : receivers) {
            receiver.exportMetrics(context, payload);
        }

        // Export to new exporters with push interval context
        if (!exporters.isEmpty()) {
            DefaultClientTelemetryContext telemetryContext = new DefaultClientTelemetryContext(pushIntervalMs, context);
            for (ClientTelemetryExporter exporter : exporters) {
                exporter.exportMetrics(telemetryContext, payload);
            }
        }
    }
}
