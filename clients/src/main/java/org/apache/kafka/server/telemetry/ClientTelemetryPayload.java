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

/**
 * A client telemetry payload as sent by the client to the telemetry receiver. The payload is
 * received by the broker's {@link ClientTelemetryReceiver} implementation.
 */
@InterfaceStability.Evolving
public interface ClientTelemetryPayload {

    /**
     * Method returns the client's instance id.
     *
     * @return Client's instance id.
     */
    Uuid clientInstanceId();

    /**
     * Indicates whether the client is terminating and thus making its last metrics push.
     *
     * @return {@code true} if client is terminating, else false
     */
    boolean isTerminating();

    /**
     * Method returns the content-type format of the metrics data which is being sent by the client.
     *
     * @return Metrics data content-type/serialization format.
     */
    String contentType();

    /**
     * Method returns the serialized metrics data as received by the client.
     *
     * @return Serialized metrics data.
     */
    ByteBuffer data();
}
