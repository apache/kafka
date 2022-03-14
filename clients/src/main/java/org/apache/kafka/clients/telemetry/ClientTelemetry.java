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
package org.apache.kafka.clients.telemetry;

import java.io.Closeable;
import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.requests.AbstractRequest;

public interface ClientTelemetry extends Closeable {

    int DEFAULT_PUSH_INTERVAL_MS = 5 * 60 * 1000;

    long MAX_TERMINAL_PUSH_WAIT_MS = 100;

    /**
     * Determines the client's unique client instance ID used for telemetry. This ID is unique to
     * the specific enclosing client instance and will not change after it is initially generated.
     * The ID is useful for correlating client operations with telemetry sent to the broker and
     * to its eventual monitoring destination(s).
     *
     * This method waits up to <code>timeout</code> for the subscription to become available in
     * order to complete the request.
     *
     * If telemetry is disabled, the method will immediately return {@code Optional.empty()} or
     * equivalent.
     *
     * @param timeout The maximum time to wait for enclosing client instance to determine its
     *                client instance ID. The value should be non-negative. Specifying a timeout
     *                of zero means do not wait for the initial request to complete if it hasn't
     *                already.
     * @throws InterruptException If the thread is interrupted while blocked.
     * @throws KafkaException If an unexpected error occurs while trying to determine the client
     *                        instance ID, though this error does not necessarily imply the
     *                        enclosing client instance is otherwise unusable.
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     * @return Human-readable string representation of the client instance ID if telemetry enabled,
     * <i>empty</i> Optional if not
     */
    Optional<String> clientInstanceId(Duration timeout);

    void initiateClose(Duration timeout);

    void setState(TelemetryState state);

    Optional<TelemetryState> state();

    Optional<TelemetrySubscription> subscription();

    void telemetrySubscriptionFailed(Throwable error);

    void pushTelemetryFailed(Throwable error);

    void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data);

    void pushTelemetrySucceeded(PushTelemetryResponseData data);

    Optional<Long> timeToNextUpdate(long requestTimeoutMs);

    Optional<AbstractRequest.Builder<?>> createRequest();

    ClientInstanceMetricRecorder clientInstanceMetricRecorder();

    ConsumerMetricRecorder consumerMetricRecorder();

    HostProcessMetricRecorder hostProcessMetricRecorder();

    ProducerMetricRecorder producerMetricRecorder();

    ProducerTopicMetricRecorder producerTopicMetricRecorder();

}
