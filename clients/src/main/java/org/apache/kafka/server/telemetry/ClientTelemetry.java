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

import org.apache.kafka.common.metrics.MetricsContext;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.annotation.InterfaceStability;

/**
 * A {@link MetricsReporter} may implement this interface to indicate support for collecting client
 * telemetry on the server side.
 */
@InterfaceStability.Evolving
public interface ClientTelemetry {

    /**
     * Implemented by the broker {@link MetricsReporter} to provide a {@link ClientTelemetryReceiver}
     * instance.
     * <p>
     * This instance may be cached by the broker.
     * <p>
     * This method must always be called after the initial call to
     * {@link MetricsReporter#contextChange(MetricsContext)} on the {@link MetricsReporter}
     * implementing this interface.
     *
     * @return broker side instance of {@link ClientTelemetryReceiver}.
     */
    ClientTelemetryReceiver clientReceiver();
}
