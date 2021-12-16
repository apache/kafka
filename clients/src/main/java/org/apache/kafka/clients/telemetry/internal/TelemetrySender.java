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

package org.apache.kafka.clients.telemetry.internal;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClientUtils;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionRequest.Builder;
import org.apache.kafka.common.requests.GetTelemetrySubscriptionResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

public class TelemetrySender implements Runnable, Closeable {

    private final KafkaClient client;

    private final Metadata metadata;

    private final Time time;

    private volatile TelemetrySubscription telemetrySubscription;

    private Node node;

    private final Logger log;

    public TelemetrySender(KafkaClient client, Metadata metadata, Time time, LogContext logContext) {
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.log = logContext.logger(TelemetrySender.class);
    }

    public TelemetrySender(KafkaClient client, Metadata metadata, LogContext logContext) {
        this(client, metadata, Time.SYSTEM, logContext);
    }

    public void run() {
        while (true) {
            long now = time.milliseconds();

            try {
                selectNode(now);
                updateTelemetrySubscription(now);
                Utils.sleep(telemetrySubscription.getPushIntervalMs());
                pushTelemetry();
            } catch (IOException e) {
                log.warn(e.getMessage(), e);
            }
        }
    }

    @Override
    public void close() {
        // TOOD: shut down stuff?
    }

    public TelemetrySubscription telemetrySubscription(Duration timeout) {
        // TODO: implement wait for request to happen
        return null;
    }

    private void selectNode(long now) {
        if (node == null || !client.isReady(node, now))
            node = client.leastLoadedNode(now);

        assert node != null;
    }

    private void updateTelemetrySubscription(long now) throws IOException {
        GetTelemetrySubscriptionsResponseData data;

        try {
            AbstractRequest.Builder<GetTelemetrySubscriptionRequest> builder = new Builder(
                telemetrySubscription != null ? telemetrySubscription.getClientInstanceId() : null);
            ClientRequest clientRequest = client.newClientRequest(node.idString(), builder, now, true);
            ClientResponse clientResponse = NetworkClientUtils.sendAndReceive(client, clientRequest, time);
            GetTelemetrySubscriptionResponse responseBody = (GetTelemetrySubscriptionResponse) clientResponse.responseBody();
            data = responseBody.data();
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }

        List<String> requestedMetricsRaw = data.requestedMetrics();
        Set<MetricDef> metricDefs;

        if (requestedMetricsRaw == null || requestedMetricsRaw.isEmpty()) {
            // no metrics
            metricDefs = Collections.emptySet();
        } else if (requestedMetricsRaw.size() == 1 && requestedMetricsRaw.get(0).isEmpty()) {
            // all metrics
            metricDefs = new HashSet<>();
        } else {
            // prefix string match...
            metricDefs = new HashSet<>();
        }

        List<Byte> acceptedCompressionTypesRaw = data.acceptedCompressionTypes();
        Set<CompressionType> acceptedCompressionTypes;

        if (acceptedCompressionTypesRaw == null || acceptedCompressionTypesRaw.isEmpty()) {
            acceptedCompressionTypes = Collections.emptySet();
        } else {
            acceptedCompressionTypes = new HashSet<>();
        }

        telemetrySubscription = new TelemetrySubscription(data.throttleTimeMs(),
            data.clientInstanceId(),
            data.subscriptionId(),
            acceptedCompressionTypes,
            data.pushIntervalMs(),
            data.deltaTemporality() ? DeltaTemporality.delta : DeltaTemporality.cumulative,
            metricDefs);
    }

    private void pushTelemetry() {
        // TODO: implement the request/response
    }

}
