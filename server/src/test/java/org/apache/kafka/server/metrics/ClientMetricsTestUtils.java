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

import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class ClientMetricsTestUtils {

    public static final String DEFAULT_METRICS =
        "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency";
    public static final int DEFAULT_PUSH_INTERVAL_MS = 30 * 1000; // 30 seconds
    public static final List<String> DEFAULT_CLIENT_MATCH_PATTERNS = Collections.unmodifiableList(Arrays.asList(
        ClientMetricsConfigs.CLIENT_SOFTWARE_NAME + "=apache-kafka-java",
        ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION + "=3.5.*"
    ));
    public static final int CLIENT_PORT = 56078;

    public static Properties defaultProperties() {
        Properties props = new Properties();
        props.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, DEFAULT_METRICS);
        props.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, Integer.toString(DEFAULT_PUSH_INTERVAL_MS));
        props.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, String.join(",", DEFAULT_CLIENT_MATCH_PATTERNS));
        return props;
    }

    public static RequestContext requestContext() throws UnknownHostException {
        return new RequestContext(
            new RequestHeader(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, (short) 0, "producer-1", 0),
            "1",
            InetAddress.getLocalHost(),
            Optional.of(CLIENT_PORT),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            new ClientInformation("apache-kafka-java", "3.5.2"),
            false);
    }

    public static RequestContext requestContextWithNullClientInfo() throws UnknownHostException {
        return new RequestContext(
            new RequestHeader(ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS, (short) 0, "producer-1", 0),
             "1",
            InetAddress.getLocalHost(),
            Optional.of(CLIENT_PORT),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            null,
            false);
    }

    public static class TestClientMetricsReceiver implements ClientTelemetryReceiver {
        public int exportMetricsInvokedCount = 0;
        public List<ByteBuffer> metricsData = new ArrayList<>();

        public void exportMetrics(AuthorizableRequestContext context, ClientTelemetryPayload payload) {
            exportMetricsInvokedCount += 1;
            metricsData.add(payload.data());
        }
    }
}
