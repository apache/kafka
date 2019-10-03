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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.MetricNameTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectMetricsRegistry {

    public static final String CONNECTOR_TAG_NAME = "connector";
    public static final String TASK_TAG_NAME = "task";
    public static final String CONNECTOR_GROUP_NAME = "connector-metrics";
    public static final String TASK_GROUP_NAME = "connector-task-metrics";
    public static final String SOURCE_TASK_GROUP_NAME = "source-task-metrics";
    public static final String SINK_TASK_GROUP_NAME = "sink-task-metrics";
    public static final String WORKER_GROUP_NAME = "connect-worker-metrics";
    public static final String WORKER_REBALANCE_GROUP_NAME = "connect-worker-rebalance-metrics";
    public static final String TASK_ERROR_HANDLING_GROUP_NAME = "task-error-metrics";

    private final List<MetricNameTemplate> allTemplates = new ArrayList<>();
    public final MetricNameTemplate connectorStatus;
    public final MetricNameTemplate connectorType;
    public final MetricNameTemplate connectorClass;
    public final MetricNameTemplate connectorVersion;
    public final MetricNameTemplate connectorTotalTaskCount;
    public final MetricNameTemplate connectorRunningTaskCount;
    public final MetricNameTemplate connectorPausedTaskCount;
    public final MetricNameTemplate connectorFailedTaskCount;
    public final MetricNameTemplate connectorUnassignedTaskCount;
    public final MetricNameTemplate connectorDestroyedTaskCount;
    public final MetricNameTemplate taskStatus;
    public final MetricNameTemplate taskRunningRatio;
    public final MetricNameTemplate taskPauseRatio;
    public final MetricNameTemplate taskCommitTimeMax;
    public final MetricNameTemplate taskCommitTimeAvg;
    public final MetricNameTemplate taskBatchSizeMax;
    public final MetricNameTemplate taskBatchSizeAvg;
    public final MetricNameTemplate taskCommitFailurePercentage;
    public final MetricNameTemplate taskCommitSuccessPercentage;
    public final MetricNameTemplate sourceRecordPollRate;
    public final MetricNameTemplate sourceRecordPollTotal;
    public final MetricNameTemplate sourceRecordWriteRate;
    public final MetricNameTemplate sourceRecordWriteTotal;
    public final MetricNameTemplate sourceRecordPollBatchTimeMax;
    public final MetricNameTemplate sourceRecordPollBatchTimeAvg;
    public final MetricNameTemplate sourceRecordActiveCount;
    public final MetricNameTemplate sourceRecordActiveCountMax;
    public final MetricNameTemplate sourceRecordActiveCountAvg;
    public final MetricNameTemplate sinkRecordReadRate;
    public final MetricNameTemplate sinkRecordReadTotal;
    public final MetricNameTemplate sinkRecordSendRate;
    public final MetricNameTemplate sinkRecordSendTotal;
    public final MetricNameTemplate sinkRecordLagMax;
    public final MetricNameTemplate sinkRecordPartitionCount;
    public final MetricNameTemplate sinkRecordOffsetCommitSeqNum;
    public final MetricNameTemplate sinkRecordOffsetCommitCompletionRate;
    public final MetricNameTemplate sinkRecordOffsetCommitCompletionTotal;
    public final MetricNameTemplate sinkRecordOffsetCommitSkipRate;
    public final MetricNameTemplate sinkRecordOffsetCommitSkipTotal;
    public final MetricNameTemplate sinkRecordPutBatchTimeMax;
    public final MetricNameTemplate sinkRecordPutBatchTimeAvg;
    public final MetricNameTemplate sinkRecordActiveCount;
    public final MetricNameTemplate sinkRecordActiveCountMax;
    public final MetricNameTemplate sinkRecordActiveCountAvg;
    public final MetricNameTemplate connectorCount;
    public final MetricNameTemplate taskCount;
    public final MetricNameTemplate connectorStartupAttemptsTotal;
    public final MetricNameTemplate connectorStartupSuccessTotal;
    public final MetricNameTemplate connectorStartupSuccessPercentage;
    public final MetricNameTemplate connectorStartupFailureTotal;
    public final MetricNameTemplate connectorStartupFailurePercentage;
    public final MetricNameTemplate taskStartupAttemptsTotal;
    public final MetricNameTemplate taskStartupSuccessTotal;
    public final MetricNameTemplate taskStartupSuccessPercentage;
    public final MetricNameTemplate taskStartupFailureTotal;
    public final MetricNameTemplate taskStartupFailurePercentage;
    public final MetricNameTemplate connectProtocol;
    public final MetricNameTemplate leaderName;
    public final MetricNameTemplate epoch;
    public final MetricNameTemplate rebalanceCompletedTotal;
    public final MetricNameTemplate rebalanceMode;
    public final MetricNameTemplate rebalanceTimeMax;
    public final MetricNameTemplate rebalanceTimeAvg;
    public final MetricNameTemplate rebalanceTimeSinceLast;
    public final MetricNameTemplate recordProcessingFailures;
    public final MetricNameTemplate recordProcessingErrors;
    public final MetricNameTemplate recordsSkipped;
    public final MetricNameTemplate retries;
    public final MetricNameTemplate errorsLogged;
    public final MetricNameTemplate dlqProduceRequests;
    public final MetricNameTemplate dlqProduceFailures;
    public final MetricNameTemplate lastErrorTimestamp;

    public Map<MetricNameTemplate, TaskStatus.State> connectorStatusMetrics;

    public ConnectMetricsRegistry() {
        this(new LinkedHashSet<String>());
    }

    public ConnectMetricsRegistry(Set<String> tags) {
        /***** Connector level *****/
        Set<String> connectorTags = new LinkedHashSet<>(tags);
        connectorTags.add(CONNECTOR_TAG_NAME);

        connectorStatus = createTemplate("status", CONNECTOR_GROUP_NAME,
                                         "The status of the connector. One of 'unassigned', 'running', 'paused', 'failed', or " +
                                         "'destroyed'.",
                                         connectorTags);
        connectorType = createTemplate("connector-type", CONNECTOR_GROUP_NAME, "The type of the connector. One of 'source' or 'sink'.",
                                       connectorTags);
        connectorClass = createTemplate("connector-class", CONNECTOR_GROUP_NAME, "The name of the connector class.", connectorTags);
        connectorVersion = createTemplate("connector-version", CONNECTOR_GROUP_NAME,
                                          "The version of the connector class, as reported by the connector.", connectorTags);

        /***** Worker task level *****/
        Set<String> workerTaskTags = new LinkedHashSet<>(tags);
        workerTaskTags.add(CONNECTOR_TAG_NAME);
        workerTaskTags.add(TASK_TAG_NAME);

        taskStatus = createTemplate("status", TASK_GROUP_NAME,
                                    "The status of the connector task. One of 'unassigned', 'running', 'paused', 'failed', or " +
                                    "'destroyed'.",
                                    workerTaskTags);
        taskRunningRatio = createTemplate("running-ratio", TASK_GROUP_NAME,
                                          "The fraction of time this task has spent in the running state.", workerTaskTags);
        taskPauseRatio = createTemplate("pause-ratio", TASK_GROUP_NAME, "The fraction of time this task has spent in the pause state.",
                                        workerTaskTags);
        taskCommitTimeMax = createTemplate("offset-commit-max-time-ms", TASK_GROUP_NAME,
                                           "The maximum time in milliseconds taken by this task to commit offsets.", workerTaskTags);
        taskCommitTimeAvg = createTemplate("offset-commit-avg-time-ms", TASK_GROUP_NAME,
                                           "The average time in milliseconds taken by this task to commit offsets.", workerTaskTags);
        taskBatchSizeMax = createTemplate("batch-size-max", TASK_GROUP_NAME, "The maximum size of the batches processed by the connector.",
                                          workerTaskTags);
        taskBatchSizeAvg = createTemplate("batch-size-avg", TASK_GROUP_NAME, "The average size of the batches processed by the connector.",
                                          workerTaskTags);
        taskCommitFailurePercentage = createTemplate("offset-commit-failure-percentage", TASK_GROUP_NAME,
                                                     "The average percentage of this task's offset commit attempts that failed.",
                                                     workerTaskTags);
        taskCommitSuccessPercentage = createTemplate("offset-commit-success-percentage", TASK_GROUP_NAME,
                                                     "The average percentage of this task's offset commit attempts that succeeded.",
                                                     workerTaskTags);

        /***** Source worker task level *****/
        Set<String> sourceTaskTags = new LinkedHashSet<>(tags);
        sourceTaskTags.add(CONNECTOR_TAG_NAME);
        sourceTaskTags.add(TASK_TAG_NAME);

        sourceRecordPollRate = createTemplate("source-record-poll-rate", SOURCE_TASK_GROUP_NAME,
                                              "The average per-second number of records produced/polled (before transformation) by " +
                                              "this task belonging to the named source connector in this worker.",
                                              sourceTaskTags);
        sourceRecordPollTotal = createTemplate("source-record-poll-total", SOURCE_TASK_GROUP_NAME,
                                               "The total number of records produced/polled (before transformation) by this task " +
                                               "belonging to the named source connector in this worker.",
                                               sourceTaskTags);
        sourceRecordWriteRate = createTemplate("source-record-write-rate", SOURCE_TASK_GROUP_NAME,
                                               "The average per-second number of records output from the transformations and written" +
                                               " to Kafka for this task belonging to the named source connector in this worker. This" +
                                               " is after transformations are applied and excludes any records filtered out by the " +
                                               "transformations.",
                                               sourceTaskTags);
        sourceRecordWriteTotal = createTemplate("source-record-write-total", SOURCE_TASK_GROUP_NAME,
                                                "The number of records output from the transformations and written to Kafka for this" +
                                                " task belonging to the named source connector in this worker, since the task was " +
                                                "last restarted.",
                                                sourceTaskTags);
        sourceRecordPollBatchTimeMax = createTemplate("poll-batch-max-time-ms", SOURCE_TASK_GROUP_NAME,
                                                      "The maximum time in milliseconds taken by this task to poll for a batch of " +
                                                      "source records.",
                                                      sourceTaskTags);
        sourceRecordPollBatchTimeAvg = createTemplate("poll-batch-avg-time-ms", SOURCE_TASK_GROUP_NAME,
                                                      "The average time in milliseconds taken by this task to poll for a batch of " +
                                                      "source records.",
                                                      sourceTaskTags);
        sourceRecordActiveCount = createTemplate("source-record-active-count", SOURCE_TASK_GROUP_NAME,
                                                 "The number of records that have been produced by this task but not yet completely " +
                                                 "written to Kafka.",
                                                 sourceTaskTags);
        sourceRecordActiveCountMax = createTemplate("source-record-active-count-max", SOURCE_TASK_GROUP_NAME,
                                                    "The maximum number of records that have been produced by this task but not yet " +
                                                    "completely written to Kafka.",
                                                    sourceTaskTags);
        sourceRecordActiveCountAvg = createTemplate("source-record-active-count-avg", SOURCE_TASK_GROUP_NAME,
                                                    "The average number of records that have been produced by this task but not yet " +
                                                    "completely written to Kafka.",
                                                    sourceTaskTags);

        /***** Sink worker task level *****/
        Set<String> sinkTaskTags = new LinkedHashSet<>(tags);
        sinkTaskTags.add(CONNECTOR_TAG_NAME);
        sinkTaskTags.add(TASK_TAG_NAME);

        sinkRecordReadRate = createTemplate("sink-record-read-rate", SINK_TASK_GROUP_NAME,
                                            "The average per-second number of records read from Kafka for this task belonging to the" +
                                            " named sink connector in this worker. This is before transformations are applied.",
                                            sinkTaskTags);
        sinkRecordReadTotal = createTemplate("sink-record-read-total", SINK_TASK_GROUP_NAME,
                                             "The total number of records read from Kafka by this task belonging to the named sink " +
                                             "connector in this worker, since the task was last restarted.",
                                             sinkTaskTags);
        sinkRecordSendRate = createTemplate("sink-record-send-rate", SINK_TASK_GROUP_NAME,
                                            "The average per-second number of records output from the transformations and sent/put " +
                                            "to this task belonging to the named sink connector in this worker. This is after " +
                                            "transformations are applied and excludes any records filtered out by the " +
                                            "transformations.",
                                            sinkTaskTags);
        sinkRecordSendTotal = createTemplate("sink-record-send-total", SINK_TASK_GROUP_NAME,
                                             "The total number of records output from the transformations and sent/put to this task " +
                                             "belonging to the named sink connector in this worker, since the task was last " +
                                             "restarted.",
                                             sinkTaskTags);
        sinkRecordLagMax = createTemplate("sink-record-lag-max", SINK_TASK_GROUP_NAME,
                                          "The maximum lag in terms of number of records that the sink task is behind the consumer's " +
                                          "position for any topic partitions.",
                                          sinkTaskTags);
        sinkRecordPartitionCount = createTemplate("partition-count", SINK_TASK_GROUP_NAME,
                                                  "The number of topic partitions assigned to this task belonging to the named sink " +
                                                  "connector in this worker.",
                                                  sinkTaskTags);
        sinkRecordOffsetCommitSeqNum = createTemplate("offset-commit-seq-no", SINK_TASK_GROUP_NAME,
                                                      "The current sequence number for offset commits.", sinkTaskTags);
        sinkRecordOffsetCommitCompletionRate = createTemplate("offset-commit-completion-rate", SINK_TASK_GROUP_NAME,
                                                              "The average per-second number of offset commit completions that were " +
                                                              "completed successfully.",
                                                              sinkTaskTags);
        sinkRecordOffsetCommitCompletionTotal = createTemplate("offset-commit-completion-total", SINK_TASK_GROUP_NAME,
                                                               "The total number of offset commit completions that were completed " +
                                                               "successfully.",
                                                               sinkTaskTags);
        sinkRecordOffsetCommitSkipRate = createTemplate("offset-commit-skip-rate", SINK_TASK_GROUP_NAME,
                                                        "The average per-second number of offset commit completions that were " +
                                                        "received too late and skipped/ignored.",
                                                        sinkTaskTags);
        sinkRecordOffsetCommitSkipTotal = createTemplate("offset-commit-skip-total", SINK_TASK_GROUP_NAME,
                                                         "The total number of offset commit completions that were received too late " +
                                                         "and skipped/ignored.",
                                                         sinkTaskTags);
        sinkRecordPutBatchTimeMax = createTemplate("put-batch-max-time-ms", SINK_TASK_GROUP_NAME,
                                                   "The maximum time taken by this task to put a batch of sinks records.", sinkTaskTags);
        sinkRecordPutBatchTimeAvg = createTemplate("put-batch-avg-time-ms", SINK_TASK_GROUP_NAME,
                                                   "The average time taken by this task to put a batch of sinks records.", sinkTaskTags);
        sinkRecordActiveCount = createTemplate("sink-record-active-count", SINK_TASK_GROUP_NAME,
                                               "The number of records that have been read from Kafka but not yet completely " +
                                               "committed/flushed/acknowledged by the sink task.",
                                               sinkTaskTags);
        sinkRecordActiveCountMax = createTemplate("sink-record-active-count-max", SINK_TASK_GROUP_NAME,
                                                  "The maximum number of records that have been read from Kafka but not yet completely "
                                                  + "committed/flushed/acknowledged by the sink task.",
                                                  sinkTaskTags);
        sinkRecordActiveCountAvg = createTemplate("sink-record-active-count-avg", SINK_TASK_GROUP_NAME,
                                                  "The average number of records that have been read from Kafka but not yet completely "
                                                  + "committed/flushed/acknowledged by the sink task.",
                                                  sinkTaskTags);

        /***** Worker level *****/
        Set<String> workerTags = new LinkedHashSet<>(tags);

        connectorCount = createTemplate("connector-count", WORKER_GROUP_NAME, "The number of connectors run in this worker.", workerTags);
        taskCount = createTemplate("task-count", WORKER_GROUP_NAME, "The number of tasks run in this worker.", workerTags);
        connectorStartupAttemptsTotal = createTemplate("connector-startup-attempts-total", WORKER_GROUP_NAME,
                                                  "The total number of connector startups that this worker has attempted.", workerTags);
        connectorStartupSuccessTotal = createTemplate("connector-startup-success-total", WORKER_GROUP_NAME,
                                                 "The total number of connector starts that succeeded.", workerTags);
        connectorStartupSuccessPercentage = createTemplate("connector-startup-success-percentage", WORKER_GROUP_NAME,
                                                      "The average percentage of this worker's connectors starts that succeeded.", workerTags);
        connectorStartupFailureTotal = createTemplate("connector-startup-failure-total", WORKER_GROUP_NAME,
                                                 "The total number of connector starts that failed.", workerTags);
        connectorStartupFailurePercentage = createTemplate("connector-startup-failure-percentage", WORKER_GROUP_NAME,
                                                      "The average percentage of this worker's connectors starts that failed.", workerTags);
        taskStartupAttemptsTotal = createTemplate("task-startup-attempts-total", WORKER_GROUP_NAME,
                                                  "The total number of task startups that this worker has attempted.", workerTags);
        taskStartupSuccessTotal = createTemplate("task-startup-success-total", WORKER_GROUP_NAME,
                                                 "The total number of task starts that succeeded.", workerTags);
        taskStartupSuccessPercentage = createTemplate("task-startup-success-percentage", WORKER_GROUP_NAME,
                                                      "The average percentage of this worker's tasks starts that succeeded.", workerTags);
        taskStartupFailureTotal = createTemplate("task-startup-failure-total", WORKER_GROUP_NAME,
                                                 "The total number of task starts that failed.", workerTags);
        taskStartupFailurePercentage = createTemplate("task-startup-failure-percentage", WORKER_GROUP_NAME,
                                                      "The average percentage of this worker's tasks starts that failed.", workerTags);

        Set<String> workerConnectorTags = new LinkedHashSet<>(tags);
        workerConnectorTags.add(CONNECTOR_TAG_NAME);
        connectorTotalTaskCount = createTemplate("connector-total-task-count", WORKER_GROUP_NAME,
            "The number of tasks of the connector on the worker.", workerConnectorTags);
        connectorRunningTaskCount = createTemplate("connector-running-task-count", WORKER_GROUP_NAME,
            "The number of running tasks of the connector on the worker.", workerConnectorTags);
        connectorPausedTaskCount = createTemplate("connector-paused-task-count", WORKER_GROUP_NAME,
            "The number of paused tasks of the connector on the worker.", workerConnectorTags);
        connectorFailedTaskCount = createTemplate("connector-failed-task-count", WORKER_GROUP_NAME,
            "The number of failed tasks of the connector on the worker.", workerConnectorTags);
        connectorUnassignedTaskCount = createTemplate("connector-unassigned-task-count",
            WORKER_GROUP_NAME,
            "The number of unassigned tasks of the connector on the worker.", workerConnectorTags);
        connectorDestroyedTaskCount = createTemplate("connector-destroyed-task-count",
            WORKER_GROUP_NAME,
            "The number of destroyed tasks of the connector on the worker.", workerConnectorTags);

        connectorStatusMetrics = new HashMap<>();
        connectorStatusMetrics.put(connectorRunningTaskCount, TaskStatus.State.RUNNING);
        connectorStatusMetrics.put(connectorPausedTaskCount, TaskStatus.State.PAUSED);
        connectorStatusMetrics.put(connectorFailedTaskCount, TaskStatus.State.FAILED);
        connectorStatusMetrics.put(connectorUnassignedTaskCount, TaskStatus.State.UNASSIGNED);
        connectorStatusMetrics.put(connectorDestroyedTaskCount, TaskStatus.State.DESTROYED);
        connectorStatusMetrics = Collections.unmodifiableMap(connectorStatusMetrics);

        /***** Worker rebalance level *****/
        Set<String> rebalanceTags = new LinkedHashSet<>(tags);

        connectProtocol = createTemplate("connect-protocol", WORKER_REBALANCE_GROUP_NAME, "The Connect protocol used by this cluster", rebalanceTags);
        leaderName = createTemplate("leader-name", WORKER_REBALANCE_GROUP_NAME, "The name of the group leader.", rebalanceTags);
        epoch = createTemplate("epoch", WORKER_REBALANCE_GROUP_NAME, "The epoch or generation number of this worker.", rebalanceTags);
        rebalanceCompletedTotal = createTemplate("completed-rebalances-total", WORKER_REBALANCE_GROUP_NAME,
                                                "The total number of rebalances completed by this worker.", rebalanceTags);
        rebalanceMode = createTemplate("rebalancing", WORKER_REBALANCE_GROUP_NAME,
                                               "Whether this worker is currently rebalancing.", rebalanceTags);
        rebalanceTimeMax = createTemplate("rebalance-max-time-ms", WORKER_REBALANCE_GROUP_NAME,
                                          "The maximum time in milliseconds spent by this worker to rebalance.", rebalanceTags);
        rebalanceTimeAvg = createTemplate("rebalance-avg-time-ms", WORKER_REBALANCE_GROUP_NAME,
                                          "The average time in milliseconds spent by this worker to rebalance.", rebalanceTags);
        rebalanceTimeSinceLast = createTemplate("time-since-last-rebalance-ms", WORKER_REBALANCE_GROUP_NAME,
                                                "The time in milliseconds since this worker completed the most recent rebalance.", rebalanceTags);

        /***** Task Error Handling Metrics *****/
        Set<String> taskErrorHandlingTags = new LinkedHashSet<>(tags);
        taskErrorHandlingTags.add(CONNECTOR_TAG_NAME);
        taskErrorHandlingTags.add(TASK_TAG_NAME);

        recordProcessingFailures = createTemplate("total-record-failures", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of record processing failures in this task.", taskErrorHandlingTags);
        recordProcessingErrors = createTemplate("total-record-errors", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of record processing errors in this task. ", taskErrorHandlingTags);
        recordsSkipped = createTemplate("total-records-skipped", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of records skipped due to errors.", taskErrorHandlingTags);
        retries = createTemplate("total-retries", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of operations retried.", taskErrorHandlingTags);
        errorsLogged = createTemplate("total-errors-logged", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of errors that were logged.", taskErrorHandlingTags);
        dlqProduceRequests = createTemplate("deadletterqueue-produce-requests", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of attempted writes to the dead letter queue.", taskErrorHandlingTags);
        dlqProduceFailures = createTemplate("deadletterqueue-produce-failures", TASK_ERROR_HANDLING_GROUP_NAME,
                "The number of failed writes to the dead letter queue.", taskErrorHandlingTags);
        lastErrorTimestamp = createTemplate("last-error-timestamp", TASK_ERROR_HANDLING_GROUP_NAME,
                "The epoch timestamp when this task last encountered an error.", taskErrorHandlingTags);
    }

    private MetricNameTemplate createTemplate(String name, String group, String doc, Set<String> tags) {
        MetricNameTemplate template = new MetricNameTemplate(name, group, doc, tags);
        allTemplates.add(template);
        return template;
    }

    public List<MetricNameTemplate> getAllTemplates() {
        return Collections.unmodifiableList(allTemplates);
    }

    public String connectorTagName() {
        return CONNECTOR_TAG_NAME;
    }

    public String taskTagName() {
        return TASK_TAG_NAME;
    }

    public String connectorGroupName() {
        return CONNECTOR_GROUP_NAME;
    }

    public String taskGroupName() {
        return TASK_GROUP_NAME;
    }

    public String sinkTaskGroupName() {
        return SINK_TASK_GROUP_NAME;
    }

    public String sourceTaskGroupName() {
        return SOURCE_TASK_GROUP_NAME;
    }

    public String workerGroupName() {
        return WORKER_GROUP_NAME;
    }

    public String workerRebalanceGroupName() {
        return WORKER_REBALANCE_GROUP_NAME;
    }

    public String taskErrorHandlingGroupName() {
        return TASK_ERROR_HANDLING_GROUP_NAME;
    }
}
