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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Gauge;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedSum;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.QuorumState;

import java.util.Arrays;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public class KafkaRaftMetrics implements AutoCloseable {

    private final Metrics metrics;

    private volatile OffsetAndEpoch logEndOffset;
    private volatile int numUnknownVoterConnections;
    private volatile OptionalLong electionStartMs;
    private volatile OptionalLong pollStartMs;

    private final MetricName currentLeaderIdMetricName;
    private final MetricName currentVotedIdMetricName;
    private final MetricName currentEpochMetricName;
    private final MetricName currentStateMetricName;
    private final MetricName highWatermarkMetricName;
    private final MetricName logEndOffsetMetricName;
    private final MetricName logEndEpochMetricName;
    private final MetricName numUnknownVoterConnectionsMetricName;
    private final Sensor commitTimeSensor;
    private final Sensor electionTimeSensor;
    private final Sensor fetchRecordsSensor;
    private final Sensor appendRecordsSensor;
    private final Sensor pollDurationSensor;

    public KafkaRaftMetrics(Metrics metrics, String metricGrpPrefix, QuorumState state) {
        this.metrics = metrics;
        String metricGroupName = metricGrpPrefix + "-metrics";

        this.pollStartMs = OptionalLong.empty();
        this.electionStartMs = OptionalLong.empty();
        this.numUnknownVoterConnections = 0;
        this.logEndOffset = new OffsetAndEpoch(0L, 0);

        this.currentStateMetricName = metrics.metricName("current-state", metricGroupName, "The current state of this member; possible values are leader, candidate, voted, follower, unattached, observer");
        Gauge<String> stateProvider = (mConfig, currentTimeMs) -> {
            if (state.isLeader()) {
                return "leader";
            } else if (state.isCandidate()) {
                return "candidate";
            } else if (state.isVoted()) {
                return "voted";
            } else if (state.isFollower()) {
                // a broker is special kind of follower, as not being a voter, it's an observer
                if (state.isObserver()) {
                    return "observer";
                } else {
                    return "follower";
                }
            } else {
                return "unattached";
            }
        };
        metrics.addMetric(this.currentStateMetricName, null, stateProvider);

        this.currentLeaderIdMetricName = metrics.metricName("current-leader", metricGroupName, "The current quorum leader's id; -1 indicates unknown");
        metrics.addMetric(this.currentLeaderIdMetricName, (mConfig, currentTimeMs) -> state.leaderId().orElse(-1));

        this.currentVotedIdMetricName = metrics.metricName("current-vote", metricGroupName, "The current voted leader's id; -1 indicates not voted for anyone");
        metrics.addMetric(this.currentVotedIdMetricName, (mConfig, currentTimeMs) -> {
            if (state.isLeader() || state.isCandidate()) {
                return state.localIdOrThrow();
            } else if (state.isVoted()) {
                return state.votedStateOrThrow().votedId();
            } else {
                return -1;
            }
        });

        this.currentEpochMetricName = metrics.metricName("current-epoch", metricGroupName, "The current quorum epoch.");
        metrics.addMetric(this.currentEpochMetricName, (mConfig, currentTimeMs) -> state.epoch());

        this.highWatermarkMetricName = metrics.metricName("high-watermark", metricGroupName, "The high watermark maintained on this member; -1 if it is unknown");
        metrics.addMetric(this.highWatermarkMetricName, (mConfig, currentTimeMs) -> state.highWatermark().map(hw -> hw.offset).orElse(-1L));

        this.logEndOffsetMetricName = metrics.metricName("log-end-offset", metricGroupName, "The current raft log end offset.");
        metrics.addMetric(this.logEndOffsetMetricName, (mConfig, currentTimeMs) -> logEndOffset.offset());

        this.logEndEpochMetricName = metrics.metricName("log-end-epoch", metricGroupName, "The current raft log end epoch.");
        metrics.addMetric(this.logEndEpochMetricName, (mConfig, currentTimeMs) -> logEndOffset.epoch());

        this.numUnknownVoterConnectionsMetricName = metrics.metricName("number-unknown-voter-connections", metricGroupName,
                "Number of unknown voters whose connection information is not cached; would never be larger than quorum-size.");
        metrics.addMetric(this.numUnknownVoterConnectionsMetricName, (mConfig, currentTimeMs) -> numUnknownVoterConnections);

        this.commitTimeSensor = metrics.sensor("commit-latency");
        this.commitTimeSensor.add(metrics.metricName("commit-latency-avg", metricGroupName,
                "The average time in milliseconds to commit an entry in the raft log."), new Avg());
        this.commitTimeSensor.add(metrics.metricName("commit-latency-max", metricGroupName,
                "The maximum time in milliseconds to commit an entry in the raft log."), new Max());

        this.electionTimeSensor = metrics.sensor("election-latency");
        this.electionTimeSensor.add(metrics.metricName("election-latency-avg", metricGroupName,
                "The average time in milliseconds spent on electing a new leader."), new Avg());
        this.electionTimeSensor.add(metrics.metricName("election-latency-max", metricGroupName,
                "The maximum time in milliseconds spent on electing a new leader."), new Max());

        this.fetchRecordsSensor = metrics.sensor("fetch-records");
        this.fetchRecordsSensor.add(metrics.metricName("fetch-records-rate", metricGroupName,
                "The average number of records fetched from the leader of the raft quorum."),
                new Rate(TimeUnit.SECONDS, new WindowedSum()));

        this.appendRecordsSensor = metrics.sensor("append-records");
        this.appendRecordsSensor.add(metrics.metricName("append-records-rate", metricGroupName,
                "The average number of records appended per sec as the leader of the raft quorum."),
                new Rate(TimeUnit.SECONDS, new WindowedSum()));

        this.pollDurationSensor = metrics.sensor("poll-idle-ratio");
        this.pollDurationSensor.add(
            metrics.metricName(
                "poll-idle-ratio-avg",
                metricGroupName,
                "The ratio of time the Raft IO thread is idle as opposed to " +
                    "doing work (e.g. handling requests or replicating from the leader)"
            ),
            new TimeRatio(1.0)
        );
    }

    public void updatePollStart(long currentTimeMs) {
        this.pollStartMs = OptionalLong.of(currentTimeMs);
    }

    public void updatePollEnd(long currentTimeMs) {
        if (pollStartMs.isPresent()) {
            long pollDurationMs = Math.max(currentTimeMs - pollStartMs.getAsLong(), 0L);
            this.pollDurationSensor.record(pollDurationMs);
            this.pollStartMs = OptionalLong.empty();
        }
    }

    public void updateLogEnd(OffsetAndEpoch logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    public void updateNumUnknownVoterConnections(int numUnknownVoterConnections) {
        this.numUnknownVoterConnections = numUnknownVoterConnections;
    }

    public void updateAppendRecords(long numRecords) {
        appendRecordsSensor.record(numRecords);
    }

    public void updateFetchedRecords(long numRecords) {
        fetchRecordsSensor.record(numRecords);
    }

    public void updateCommitLatency(double latencyMs, long currentTimeMs) {
        commitTimeSensor.record(latencyMs, currentTimeMs);
    }

    public void updateElectionStartMs(long currentTimeMs) {
        electionStartMs = OptionalLong.of(currentTimeMs);
    }

    public void maybeUpdateElectionLatency(long currentTimeMs) {
        if (electionStartMs.isPresent()) {
            electionTimeSensor.record(currentTimeMs - electionStartMs.getAsLong(), currentTimeMs);
            electionStartMs = OptionalLong.empty();
        }
    }

    @Override
    public void close() {
        Arrays.asList(
            currentLeaderIdMetricName,
            currentVotedIdMetricName,
            currentEpochMetricName,
            currentStateMetricName,
            highWatermarkMetricName,
            logEndOffsetMetricName,
            logEndEpochMetricName,
            numUnknownVoterConnectionsMetricName
        ).forEach(metrics::removeMetric);

        Arrays.asList(
            commitTimeSensor.name(),
            electionTimeSensor.name(),
            fetchRecordsSensor.name(),
            appendRecordsSensor.name(),
            pollDurationSensor.name()
        ).forEach(metrics::removeSensor);
    }
}
