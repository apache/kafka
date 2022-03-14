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

import java.time.Duration;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.GetTelemetrySubscriptionsResponseData;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.requests.AbstractRequest;

public class NoopClientTelemetry implements ClientTelemetry {

    @Override
    public Optional<String> clientInstanceId(Duration timeout) {
        return Optional.empty();
    }

    @Override
    public void initiateClose(Duration timeout) {
    }

    @Override
    public void close() {
    }

    @Override
    public void setState(TelemetryState state) {
    }

    @Override
    public Optional<TelemetryState> state() {
        return Optional.empty();
    }

    @Override
    public Optional<TelemetrySubscription> subscription() {
        return Optional.empty();
    }

    @Override
    public void telemetrySubscriptionFailed(Throwable error) {
    }

    @Override
    public void pushTelemetryFailed(Throwable error) {
    }

    @Override
    public void telemetrySubscriptionSucceeded(GetTelemetrySubscriptionsResponseData data) {
    }

    @Override
    public void pushTelemetrySucceeded(PushTelemetryResponseData data) {
    }

    @Override
    public Optional<Long> timeToNextUpdate(long requestTimeoutMs) {
        return Optional.empty();
    }

    @Override
    public Optional<AbstractRequest.Builder<?>> createRequest() {
        return Optional.empty();
    }

    @Override
    public ConsumerMetricRecorder consumerMetricRecorder() {
        return new ConsumerMetricRecorder() {
            @Override
            public void recordPollInterval(long amount) {
                
            }

            @Override
            public void setPollLast(long seconds) {

            }

            @Override
            public void recordPollLatency(long amount) {

            }

            @Override
            public void addCommitCount(long amount) {

            }

            @Override
            public void setGroupAssignmentPartitionCount(long amount) {

            }

            @Override
            public void setAssignmentPartitionCount(long amount) {

            }

            @Override
            public void addGroupRebalanceCount(long amount) {

            }

            @Override
            public void addGroupErrorCount(String error, long amount) {

            }

            @Override
            public void incrementRecordQueueCount(long amount) {

            }

            @Override
            public void incrementRecordQueueBytes(long amount) {

            }

            @Override
            public void addRecordApplicationCount(long amount) {

            }

            @Override
            public void addRecordApplicationBytes(long amount) {

            }
        };
    }

    @Override
    public ClientInstanceMetricRecorder clientInstanceMetricRecorder() {
        return new ClientInstanceMetricRecorder() {
            @Override
            public void addConnectionCreations(String brokerId, long amount) {

            }

            @Override
            public void incrementConnectionActive(long amount) {

            }

            @Override
            public void addConnectionErrors(ConnectionErrorReason reason, long amount) {

            }

            @Override
            public void recordRequestRtt(String brokerId, String requestType, long amount) {

            }

            @Override
            public void recordRequestQueueLatency(String brokerId, long amount) {

            }

            @Override
            public void incrementRequestQueueCount(String brokerId, long amount) {

            }

            @Override
            public void addRequestSuccess(String brokerId, String requestType, long amount) {

            }

            @Override
            public void addRequestErrors(String brokerId, String requestType, RequestErrorReason reason, long amount) {

            }

            @Override
            public void recordIoWaitTime(long amount) {

            }
        };
    }

    @Override
    public HostProcessMetricRecorder hostProcessMetricRecorder() {
        return new HostProcessMetricRecorder() {
            @Override
            public void setMemoryBytes(long amount) {

            }

            @Override
            public void setCpuUserTime(long seconds) {

            }

            @Override
            public void setCpuSystemTime(long seconds) {

            }

            @Override
            public void setPid(long pid) {

            }
        };
    }

    @Override
    public ProducerMetricRecorder producerMetricRecorder() {
        return new ProducerMetricRecorder() {
            @Override
            public void incrementRecordQueueBytes(long amount) {

            }

            @Override
            public void incrementRecordQueueMaxBytes(long amount) {

            }

            @Override
            public void incrementRecordQueueCount(long amount) {

            }

            @Override
            public void incrementRecordQueueMaxCount(long amount) {

            }
        };
    }

    @Override
    public ProducerTopicMetricRecorder producerTopicMetricRecorder() {
        return new ProducerTopicMetricRecorder() {
            @Override
            public void incrementRecordQueueBytes(TopicPartition topicPartition, short acks, long amount) {

            }

            @Override
            public void incrementRecordQueueCount(TopicPartition topicPartition, short acks, long amount) {

            }

            @Override
            public void recordRecordLatency(TopicPartition topicPartition, short acks, long amount) {

            }

            @Override
            public void recordRecordQueueLatency(TopicPartition topicPartition, short acks, long amount) {

            }

            @Override
            public void addRecordRetries(TopicPartition topicPartition, short acks, long amount) {

            }

            @Override
            public void addRecordFailures(TopicPartition topicPartition, short acks, Throwable error, long amount) {

            }

            @Override
            public void addRecordSuccess(TopicPartition topicPartition, short acks, long amount) {

            }
        };
    }
}
