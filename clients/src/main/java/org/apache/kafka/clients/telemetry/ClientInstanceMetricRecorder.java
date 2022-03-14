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

/**
 * A {@link ClientMetricRecorder} that exposes methods to record the client instance-level metrics.
 */

public interface ClientInstanceMetricRecorder extends ClientMetricRecorder {

    enum ConnectionErrorReason {
        auth, close, disconnect, timeout, TLS
    }

    enum RequestErrorReason {
        disconnect, error, timeout
    }

    String PREFIX = ClientMetricRecorder.PREFIX;

    String CONNECTION_CREATIONS_NAME = PREFIX + "connection.creations";

    String CONNECTION_CREATIONS_DESCRIPTION = "Total number of broker connections made.";

    String CONNECTION_COUNT_NAME = PREFIX + "connection.count";

    String CONNECTION_COUNT_DESCRIPTION = "Current number of broker connections.";

    String CONNECTION_ERRORS_NAME = PREFIX + "connection.errors";

    String CONNECTION_ERRORS_DESCRIPTION = "Total number of broker connection failures.";

    String REQUEST_RTT_NAME = PREFIX + "request.rtt";

    String REQUEST_RTT_DESCRIPTION = "Average request latency / round-trip-time to broker and back.";

    String REQUEST_QUEUE_LATENCY_NAME = PREFIX + "request.queue.latency";

    String REQUEST_QUEUE_LATENCY_DESCRIPTION = "Average request queue latency waiting for request to be sent to broker.";

    String REQUEST_QUEUE_COUNT_NAME = PREFIX + "request.queue.count";

    String REQUEST_QUEUE_COUNT_DESCRIPTION = "Number of requests in queue waiting to be sent to broker.";

    String REQUEST_SUCCESS_NAME = PREFIX + "request.success";

    String REQUEST_SUCCESS_NAME_DESCRIPTION = "Number of successful requests to broker, that is where a response is received without no request-level error (but there may be per-sub-resource errors, e.g., errors for certain partitions within an OffsetCommitResponse).";

    String REQUEST_ERRORS_NAME = PREFIX + "request.errors";

    String REQUEST_ERRORS_DESCRIPTION = "Number of failed requests.";

    String IO_WAIT_TIME_NAME = PREFIX + "io.wait.time";

    String IO_WAIT_TIME_DESCRIPTION = "Amount of time waiting for socket I/O writability (POLLOUT). A high number indicates socket send buffer congestion.";

    String BROKER_ID_LABEL = "broker_id";

    String REASON_LABEL = "reason";

    String REQUEST_TYPE_LABEL = "request_type";

    void addConnectionCreations(String brokerId, long amount);

    void incrementConnectionActive(long amount);

    default void decrementConnectionActive(long amount) {
        incrementConnectionActive(-amount);
    }

    void addConnectionErrors(ConnectionErrorReason reason, long amount);

    void recordRequestRtt(String brokerId, String requestType, long amount);

    void recordRequestQueueLatency(String brokerId, long amount);

    void incrementRequestQueueCount(String brokerId, long amount);

    default void decrementRequestQueueCount(String brokerId, long amount) {
        incrementRequestQueueCount(brokerId, -amount);
    }

    void addRequestSuccess(String brokerId, String requestType, long amount);

    void addRequestErrors(String brokerId, String requestType, RequestErrorReason reason, long amount);

    void recordIoWaitTime(long amount);

}
