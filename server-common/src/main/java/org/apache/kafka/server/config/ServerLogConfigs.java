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

package org.apache.kafka.server.config;

import org.apache.kafka.common.config.TopicConfig;

import static org.apache.kafka.server.config.ServerTopicConfigSynonyms.LOG_PREFIX;

/**
 * Common home for broker-side log configs which need to be accessible from the libraries shared
 * between the broker and the multiple modules in Kafka.
 *
 * Note this is an internal API and subject to change without notice.
 */
public class ServerLogConfigs {
    public static final String NUM_PARTITIONS_CONFIG = "num.partitions";
    public static final int NUM_PARTITIONS_DEFAULT = 1;
    public static final String NUM_PARTITIONS_DOC = "The default number of log partitions per topic";

    public static final String LOG_DIRS_CONFIG = LOG_PREFIX + "dirs";
    public static final String LOG_DIR_CONFIG = LOG_PREFIX + "dir";
    public static final String LOG_DIR_DEFAULT = "/tmp/kafka-logs";
    public static final String LOG_DIR_DOC = "The directory in which the log data is kept (supplemental for " + LOG_DIRS_CONFIG + " property)";
    public static final String LOG_DIRS_DOC = "A comma-separated list of the directories where the log data is stored. If not set, the value in " + LOG_DIR_CONFIG + " is used.";

    public static final String LOG_SEGMENT_BYTES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_BYTES_CONFIG);
    public static final String LOG_SEGMENT_BYTES_DOC = "The maximum size of a single log file";

    public static final String LOG_ROLL_TIME_MILLIS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_MS_CONFIG);
    public static final String LOG_ROLL_TIME_HOURS_CONFIG = LOG_PREFIX + "roll.hours";
    public static final String LOG_ROLL_TIME_MILLIS_DOC = "The maximum time before a new log segment is rolled out (in milliseconds). If not set, the value in " + LOG_ROLL_TIME_HOURS_CONFIG + " is used";
    public static final String LOG_ROLL_TIME_HOURS_DOC = "The maximum time before a new log segment is rolled out (in hours), secondary to " + LOG_ROLL_TIME_MILLIS_CONFIG + " property";

    public static final String LOG_ROLL_TIME_JITTER_MILLIS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_JITTER_MS_CONFIG);
    public static final String LOG_ROLL_TIME_JITTER_HOURS_CONFIG = LOG_PREFIX + "roll.jitter.hours";
    public static final String LOG_ROLL_TIME_JITTER_MILLIS_DOC = "The maximum jitter to subtract from logRollTimeMillis (in milliseconds). If not set, the value in " + LOG_ROLL_TIME_JITTER_HOURS_CONFIG + " is used";
    public static final String LOG_ROLL_TIME_JITTER_HOURS_DOC = "The maximum jitter to subtract from logRollTimeMillis (in hours), secondary to " + LOG_ROLL_TIME_JITTER_MILLIS_CONFIG + " property";


    public static final String LOG_RETENTION_TIME_MILLIS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.RETENTION_MS_CONFIG);
    public static final String LOG_RETENTION_TIME_MINUTES_CONFIG = LOG_PREFIX + "retention.minutes";
    public static final String LOG_RETENTION_TIME_HOURS_CONFIG = LOG_PREFIX + "retention.hours";
    public static final String LOG_RETENTION_TIME_MILLIS_DOC = "The number of milliseconds to keep a log file before deleting it (in milliseconds), If not set, the value in " + LOG_RETENTION_TIME_MINUTES_CONFIG + " is used. If set to -1, no time limit is applied.";
    public static final String LOG_RETENTION_TIME_MINUTES_DOC = "The number of minutes to keep a log file before deleting it (in minutes), secondary to " + LOG_RETENTION_TIME_MILLIS_CONFIG + " property. If not set, the value in " + LOG_RETENTION_TIME_HOURS_CONFIG + " is used";
    public static final String LOG_RETENTION_TIME_HOURS_DOC = "The number of hours to keep a log file before deleting it (in hours), tertiary to " + LOG_RETENTION_TIME_MILLIS_CONFIG + " property";

    public static final String LOG_RETENTION_BYTES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.RETENTION_BYTES_CONFIG);
    public static final long LOG_RETENTION_BYTES_DEFAULT = -1L;
    public static final String LOG_RETENTION_BYTES_DOC = "The maximum size of the log before deleting it";

    public static final String LOG_CLEANUP_INTERVAL_MS_CONFIG = LOG_PREFIX + "retention.check.interval.ms";
    public static final long LOG_CLEANUP_INTERVAL_MS_DEFAULT = 5 * 60 * 1000L;
    public static final String LOG_CLEANUP_INTERVAL_MS_DOC = "The frequency in milliseconds that the log cleaner checks whether any log is eligible for deletion";

    public static final String LOG_CLEANUP_POLICY_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.CLEANUP_POLICY_CONFIG);
    public static final String LOG_CLEANUP_POLICY_DEFAULT = TopicConfig.CLEANUP_POLICY_DELETE;
    public static final String LOG_CLEANUP_POLICY_DOC = "The default cleanup policy for segments beyond the retention window. A comma separated list of valid policies.";

    public static final String LOG_INDEX_SIZE_MAX_BYTES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG);
    public static final int LOG_INDEX_SIZE_MAX_BYTES_DEFAULT = 10 * 1024 * 1024;
    public static final String LOG_INDEX_SIZE_MAX_BYTES_DOC = "The maximum size in bytes of the offset index";

    public static final String LOG_INDEX_INTERVAL_BYTES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG);
    public static final int LOG_INDEX_INTERVAL_BYTES_DEFAULT = 4096;
    public static final String LOG_INDEX_INTERVAL_BYTES_DOC = "The interval with which we add an entry to the offset index.";

    public static final String LOG_FLUSH_INTERVAL_MESSAGES_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG);
    public static final long LOG_FLUSH_INTERVAL_MESSAGES_DEFAULT = Long.MAX_VALUE;
    public static final String LOG_FLUSH_INTERVAL_MESSAGES_DOC = "The number of messages accumulated on a log partition before messages are flushed to disk.";

    public static final String LOG_DELETE_DELAY_MS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG);
    public static final long LOG_DELETE_DELAY_MS_DEFAULT = 60000L;
    public static final String LOG_DELETE_DELAY_MS_DOC = "The amount of time to wait before deleting a file from the filesystem. If the value is 0 and there is no file to delete, the system will wait 1 millisecond. Low value will cause busy waiting";

    public static final String LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG = LOG_PREFIX + "flush.scheduler.interval.ms";
    public static final long LOG_FLUSH_SCHEDULER_INTERVAL_MS_DEFAULT = Long.MAX_VALUE;
    public static final String LOG_FLUSH_SCHEDULER_INTERVAL_MS_DOC = "The frequency in ms that the log flusher checks whether any log needs to be flushed to disk";

    public static final String LOG_FLUSH_INTERVAL_MS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.FLUSH_MS_CONFIG);
    public static final String LOG_FLUSH_INTERVAL_MS_DOC = "The maximum time in ms that a message in any topic is kept in memory before flushed to disk. If not set, the value in " + LOG_FLUSH_SCHEDULER_INTERVAL_MS_CONFIG + " is used";

    public static final String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG = LOG_PREFIX + "flush.offset.checkpoint.interval.ms";
    public static final int LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT = 60000;
    public static final String LOG_FLUSH_OFFSET_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which we update the persistent record of the last flush which acts as the log recovery point.";

    public static final String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_CONFIG = LOG_PREFIX + "flush.start.offset.checkpoint.interval.ms";
    public static final int LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DEFAULT = 60000;
    public static final String LOG_FLUSH_START_OFFSET_CHECKPOINT_INTERVAL_MS_DOC = "The frequency with which we update the persistent record of log start offset";

    public static final String LOG_PRE_ALLOCATE_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.PREALLOCATE_CONFIG);
    public static final String LOG_PRE_ALLOCATE_ENABLE_DOC = "Should pre allocate file when create new segment? If you are using Kafka on Windows, you probably need to set it to true.";

    public static final String LOG_MESSAGE_TIMESTAMP_TYPE_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG);
    public static final String LOG_MESSAGE_TIMESTAMP_TYPE_DEFAULT = "CreateTime";
    public static final String LOG_MESSAGE_TIMESTAMP_TYPE_DOC = "Define whether the timestamp in the message is message create time or log append time. The value should be either " +
            "<code>CreateTime</code> or <code>LogAppendTime</code>.";

    public static final String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG);
    public static final long LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DEFAULT = Long.MAX_VALUE;
    public static final String LOG_MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC = "This configuration sets the allowable timestamp difference between the " +
            "broker's timestamp and the message timestamp. The message timestamp can be earlier than or equal to the broker's " +
            "timestamp, with the maximum allowable difference determined by the value set in this configuration. " +
            "If log.message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds " +
            "this specified threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime.";
    public static final String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG);
    public static final long LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DEFAULT = Long.MAX_VALUE;
    public static final String LOG_MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC = "This configuration sets the allowable timestamp difference between the " +
            "message timestamp and the broker's timestamp. The message timestamp can be later than or equal to the broker's " +
            "timestamp, with the maximum allowable difference determined by the value set in this configuration. " +
            "If log.message.timestamp.type=CreateTime, the message will be rejected if the difference in timestamps exceeds " +
            "this specified threshold. This configuration is ignored if log.message.timestamp.type=LogAppendTime.";

    public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR_CONFIG = "num.recovery.threads.per.data.dir";
    public static final int NUM_RECOVERY_THREADS_PER_DATA_DIR_DEFAULT = 1;
    public static final String NUM_RECOVERY_THREADS_PER_DATA_DIR_DOC = "The number of threads per data directory to be used for log recovery at startup and flushing at shutdown";

    public static final String AUTO_CREATE_TOPICS_ENABLE_CONFIG = "auto.create.topics.enable";
    public static final boolean AUTO_CREATE_TOPICS_ENABLE_DEFAULT = true;
    public static final String AUTO_CREATE_TOPICS_ENABLE_DOC = "Enable auto creation of topic on the server.";

    public static final String MIN_IN_SYNC_REPLICAS_CONFIG = ServerTopicConfigSynonyms.serverSynonym(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG);
    public static final int MIN_IN_SYNC_REPLICAS_DEFAULT = 1;
    public static final String MIN_IN_SYNC_REPLICAS_DOC = "When a producer sets acks to \"all\" (or \"-1\"), " +
            "<code>min.insync.replicas</code> specifies the minimum number of replicas that must acknowledge " +
            "a write for the write to be considered successful. If this minimum cannot be met, " +
            "then the producer will raise an exception (either <code>NotEnoughReplicas</code> or " +
            "<code>NotEnoughReplicasAfterAppend</code>).<br>When used together, <code>min.insync.replicas</code> and acks " +
            "allow you to enforce greater durability guarantees. A typical scenario would be to " +
            "create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2, and " +
            "produce with acks of \"all\". This will ensure that the producer raises an exception " +
            "if a majority of replicas do not receive a write.";

    public static final String CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG = "create.topic.policy.class.name";
    public static final String CREATE_TOPIC_POLICY_CLASS_NAME_DOC = "The create topic policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.CreateTopicPolicy</code> interface.";
    public static final String ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG = "alter.config.policy.class.name";
    public static final String ALTER_CONFIG_POLICY_CLASS_NAME_DOC = "The alter configs policy class that should be used for validation. The class should " +
            "implement the <code>org.apache.kafka.server.policy.AlterConfigPolicy</code> interface.";

    public static final String LOG_INITIAL_TASK_DELAY_MS_CONFIG = LOG_PREFIX + "initial.task.delay.ms";
    public static final long LOG_INITIAL_TASK_DELAY_MS_DEFAULT = 30 * 1000L;
    public static final String LOG_INITIAL_TASK_DELAY_MS_DOC = "The initial task delay in millisecond when initializing " +
            "tasks in LogManager. This should be used for testing only.";

    public static final String LOG_DIR_FAILURE_TIMEOUT_MS_CONFIG = LOG_PREFIX + "dir.failure.timeout.ms";
    public static final Long LOG_DIR_FAILURE_TIMEOUT_MS_DEFAULT = 30000L;
    public static final String LOG_DIR_FAILURE_TIMEOUT_MS_DOC = "If the broker is unable to successfully communicate to the controller that some log " +
        "directory has failed for longer than this time, the broker will fail and shut down.";
}
