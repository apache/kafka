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

import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

/**
 * {@code ClientTelemetryContext} provides context information for client telemetry requests,
 * including the push interval.
 */
public interface ClientTelemetryContext {

    /**
     * Returns the interval at which the client pushes telemetry metrics to the broker.
     * This is the interval from the subscription.
     * <p>
     * Note that for the initial metric push and pushes following a subscription update
     * or error, a jitter (between 0.5x and 1.5x of this interval) is applied to avoid
     * multiple clients sending requests simultaneously.
     * <p>
     * This value can be used by metrics exporters to determine when metrics should be
     * considered stale or expired.
     *
     * @return the push interval in milliseconds from the subscription
     */
    int pushIntervalMs();

    /**
     * Returns the authorization context for the client request.
     *
     * @return the client request context for the corresponding {@code PushTelemetryRequest} API call
     */
    AuthorizableRequestContext authorizableRequestContext();
}
