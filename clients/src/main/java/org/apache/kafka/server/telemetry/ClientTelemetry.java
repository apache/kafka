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

package org.apache.kafka.server.telemetry;

import org.apache.kafka.common.metrics.MetricsReporter;

/**
 * A {@link MetricsReporter} may implement this interface to indicate support for collecting client
 * telemetry on the server side.
 *
 * @deprecated Since 4.2.0, use {@link ClientTelemetryExporterProvider} instead. This interface will be
 *             removed in Kafka 5.0.0. The new interface provides a {@link ClientTelemetryExporter}
 *             which includes additional context such as the push interval.
 */
@Deprecated(since = "4.2", forRemoval = true)
@SuppressWarnings("removal")
public interface ClientTelemetry {

    /**
     * Called by the broker to fetch instance of {@link ClientTelemetryReceiver}.
     * <p>
     * This instance may be cached by the broker.
     *
     * @return broker side instance of {@link ClientTelemetryReceiver}.
     */
    ClientTelemetryReceiver clientReceiver();
}
