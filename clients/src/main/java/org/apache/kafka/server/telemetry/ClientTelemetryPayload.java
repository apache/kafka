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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.annotation.InterfaceStability;

import java.nio.ByteBuffer;

@InterfaceStability.Evolving
public interface ClientTelemetryPayload {

    /**
     * @return Client's instance id.
     */
    Uuid clientInstanceId();

    /**
     * Indicates whether client is terminating, e.g., the last metrics push from this client instance.
     * <p>
     *To avoid the receiving broker’s metrics rate-limiter discarding this out-of-profile push, the
     * PushTelemetryRequest.Terminating field must be set to true. A broker must only allow one such
     * unthrottled metrics push for each combination of client instance ID and SubscriptionId.
     *
     * @return {@code true} if client is terminating, else false
     */
    boolean isTerminating();

    /**
     * @return Metrics data content-type/serialization format.
     */
    String contentType();

    /**
     * @return Serialized metrics data.
     */
    ByteBuffer data();
}
