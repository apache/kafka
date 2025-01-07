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

package org.apache.kafka.common.config;

/**
 * <p>Keys that can be used to configure a topic. These keys are useful when creating or reconfiguring a
 * topic using the AdminClient.
 *
 * <p>The intended pattern is for broker configs to include a <code>`log.`</code> prefix. For example, to set the default broker
 * cleanup policy, one would set <code>log.cleanup.policy</code> instead of <code>cleanup.policy</code>. Unfortunately, there are many cases
 * where this pattern is not followed.
 */
// This is a public API, so we should not remove or alter keys without a discussion and a deprecation period.
// Eventually this should replace LogConfig.scala.
public class TopicConfig {
    public static final String SEGMENT_BYTES_CONFIG = "segment.bytes";
    public static final String SEGMENT_BYTES_DOC = "This configuration controls the segment file size for " +
        "the log. Retention and cleaning is always done a file at a time so a larger segment size means " +
        "fewer files but less granular control over retention.";

    public static final String SEGMENT_MS_CONFIG = "segment.ms";
    public static final String SEGMENT_MS_DOC = "This configuration controls the period of time after " +
        "which Kafka will force the log to roll even if the segment file isn't full to ensure that retention " +
        "can delete or compact old data.";

    public static final String SEGMENT_JITTER_MS_CONFIG = "segment.jitter.ms";
    public static final String SEGMENT_JITTER_MS_DOC = "The maximum random jitter subtracted from the scheduled " +
        "segment roll time to avoid thundering herds of segment rolling";

    public static final String SEGMENT_INDEX_BYTES_CONFIG = "segment.index.bytes";
    public static final String SEGMENT_INDEX_BYTES_DOC = "This configuration controls the size of the index that " +
        "maps offsets to file positions. We preallocate this index file and shrink it only after log " +
        "rolls. You generally should not need to change this setting.";

    public static final String FLUSH_MESSAGES_INTERVAL_CONFIG = "flush.messages";
    public static final String FLUSH_MESSAGES_INTERVAL_DOC = "This setting allows specifying an interval at " +
        "which we will force an fsync of data written to the log. For example if this was set to 1 " +
        "we would fsync after every message; if it were 5 we would fsync after every five messages. " +
        "In general we recommend you not set this and use replication for durability and allow the " +
        "operating system's background flush capabilities as it is more efficient. This setting can " +
        "be overridden on a per-topic basis (see <a href=\"#topicconfigs\">the per-topic configuration section</a>).";

    public static final String FLUSH_MS_CONFIG = "flush.ms";
    public static final String FLUSH_MS_DOC = "This setting allows specifying a time interval at which we will " +
        "force an fsync of data written to the log. For example if this was set to 1000 " +
        "we would fsync after 1000 ms had passed. In general we recommend you not set " +
        "this and use replication for durability and allow the operating system's background " +
        "flush capabilities as it is more efficient.";

    public static final String RETENTION_BYTES_CONFIG = "retention.bytes";
    public static final String RETENTION_BYTES_DOC = "This configuration controls the maximum size a partition " +
        "(which consists of log segments) can grow to before we will discard old log segments to free up space if we " +
        "are using the \"delete\" retention policy. By default there is no size limit only a time limit. " +
        "Since this limit is enforced at the partition level, multiply it by the number of partitions to compute " +
        "the topic retention in bytes. Additionally, retention.bytes configuration " +
        "operates independently of \"segment.ms\" and \"segment.bytes\" configurations. " +
        "Moreover, it triggers the rolling of new segment if the retention.bytes is configured to zero.";

    public static final String RETENTION_MS_CONFIG = "retention.ms";
    public static final String RETENTION_MS_DOC = "This configuration controls the maximum time we will retain a " +
        "log before we will discard old log segments to free up space if we are using the " +
        "\"delete\" retention policy. This represents an SLA on how soon consumers must read " +
        "their data. If set to -1, no time limit is applied. Additionally, retention.ms configuration " +
        "operates independently of \"segment.ms\" and \"segment.bytes\" configurations. " +
        "Moreover, it triggers the rolling of new segment if the retention.ms condition is satisfied.";

    public static final String REMOTE_LOG_STORAGE_ENABLE_CONFIG = "remote.storage.enable";
    public static final String REMOTE_LOG_STORAGE_ENABLE_DOC = "To enable tiered storage for a topic, set this configuration as true. " +
            "You can not disable this config once it is enabled. It will be provided in future versions.";

    public static final String LOCAL_LOG_RETENTION_MS_CONFIG = "local.retention.ms";
    public static final String LOCAL_LOG_RETENTION_MS_DOC = "The number of milliseconds to keep the local log segment before it gets deleted. " +
            "Default value is -2, it represents `retention.ms` value is to be used. The effective value should always be less than or equal " +
            "to `retention.ms` value.";

    public static final String LOCAL_LOG_RETENTION_BYTES_CONFIG = "local.retention.bytes";
    public static final String LOCAL_LOG_RETENTION_BYTES_DOC = "The maximum size of local log segments that can grow for a partition before it " +
            "deletes the old segments. Default value is -2, it represents `retention.bytes` value to be used. The effective value should always be " +
            "less than or equal to `retention.bytes` value.";

    public static final String REMOTE_LOG_COPY_DISABLE_CONFIG = "remote.log.copy.disable";
    public static final String REMOTE_LOG_COPY_DISABLE_DOC = "Determines whether tiered data for a topic should become read only," +
            " and no more data uploading on a topic. Once this config is set to true, the local retention configuration " +
            "(i.e. local.retention.ms/bytes) becomes irrelevant, and all data expiration follows the topic-wide retention configuration" +
            "(i.e. retention.ms/bytes).";

    public static final String REMOTE_LOG_DELETE_ON_DISABLE_CONFIG = "remote.log.delete.on.disable";
    public static final String REMOTE_LOG_DELETE_ON_DISABLE_DOC = "Determines whether tiered data for a topic should be " +
            "deleted after tiered storage is disabled on a topic. This configuration should be enabled when trying to " +
            "set `remote.storage.enable` from true to false";

    public static final String MAX_MESSAGE_BYTES_CONFIG = "max.message.bytes";
    public static final String MAX_MESSAGE_BYTES_DOC =
        "The largest record batch size allowed by Kafka (after compression if compression is enabled). " +
        "If this is increased and there are consumers older than 0.10.2, the consumers' fetch " +
        "size must also be increased so that they can fetch record batches this large. " +
        "In the latest message format version, records are always grouped into batches for efficiency. " +
        "In previous message format versions, uncompressed records are not grouped into batches and this " +
        "limit only applies to a single record in that case.";

    public static final String INDEX_INTERVAL_BYTES_CONFIG = "index.interval.bytes";
    public static final String INDEX_INTERVAL_BYTES_DOC = "This setting controls how frequently " +
        "Kafka adds an index entry to its offset index. The default setting ensures that we index a " +
        "message roughly every 4096 bytes. More indexing allows reads to jump closer to the exact " +
        "position in the log but makes the index larger. You probably don't need to change this.";

    public static final String FILE_DELETE_DELAY_MS_CONFIG = "file.delete.delay.ms";
    public static final String FILE_DELETE_DELAY_MS_DOC = "The time to wait before deleting a file from the " +
        "filesystem";

    public static final String DELETE_RETENTION_MS_CONFIG = "delete.retention.ms";
    public static final String DELETE_RETENTION_MS_DOC = "The amount of time to retain delete tombstone markers " +
        "for <a href=\"#compaction\">log compacted</a> topics. This setting also gives a bound " +
        "on the time in which a consumer must complete a read if they begin from offset 0 " +
        "to ensure that they get a valid snapshot of the final stage (otherwise delete " +
        "tombstones may be collected before they complete their scan).";

    public static final String MIN_COMPACTION_LAG_MS_CONFIG = "min.compaction.lag.ms";
    public static final String MIN_COMPACTION_LAG_MS_DOC = "The minimum time a message will remain " +
        "uncompacted in the log. Only applicable for logs that are being compacted.";

    public static final String MAX_COMPACTION_LAG_MS_CONFIG = "max.compaction.lag.ms";
    public static final String MAX_COMPACTION_LAG_MS_DOC = "The maximum time a message will remain " +
        "ineligible for compaction in the log. Only applicable for logs that are being compacted.";

    public static final String MIN_CLEANABLE_DIRTY_RATIO_CONFIG = "min.cleanable.dirty.ratio";
    public static final String MIN_CLEANABLE_DIRTY_RATIO_DOC = "This configuration controls how frequently " +
        "the log compactor will attempt to clean the log (assuming <a href=\"#compaction\">log " +
        "compaction</a> is enabled). By default we will avoid cleaning a log where more than " +
        "50% of the log has been compacted. This ratio bounds the maximum space wasted in " +
        "the log by duplicates (at 50% at most 50% of the log could be duplicates). A " +
        "higher ratio will mean fewer, more efficient cleanings but will mean more wasted " +
        "space in the log. If the " + MAX_COMPACTION_LAG_MS_CONFIG + " or the " + MIN_COMPACTION_LAG_MS_CONFIG +
        " configurations are also specified, then the log compactor considers the log to be eligible for compaction " +
        "as soon as either: (i) the dirty ratio threshold has been met and the log has had dirty (uncompacted) " +
        "records for at least the " + MIN_COMPACTION_LAG_MS_CONFIG + " duration, or (ii) if the log has had " +
        "dirty (uncompacted) records for at most the " + MAX_COMPACTION_LAG_MS_CONFIG + " period.";

    public static final String CLEANUP_POLICY_CONFIG = "cleanup.policy";
    public static final String CLEANUP_POLICY_COMPACT = "compact";
    public static final String CLEANUP_POLICY_DELETE = "delete";
    public static final String CLEANUP_POLICY_DOC = "This config designates the retention policy to " +
        "use on log segments. The \"delete\" policy (which is the default) will discard old segments " +
        "when their retention time or size limit has been reached. The \"compact\" policy will enable " +
        "<a href=\"#compaction\">log compaction</a>, which retains the latest value for each key. " +
        "It is also possible to specify both policies in a comma-separated list (e.g. \"delete,compact\"). " +
        "In this case, old segments will be discarded per the retention time and size configuration, " +
        "while retained segments will be compacted.";

    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG = "unclean.leader.election.enable";
    public static final String UNCLEAN_LEADER_ELECTION_ENABLE_DOC = "Indicates whether to enable replicas " +
        "not in the ISR set to be elected as leader as a last resort, even though doing so may result in data " +
        "loss.<p>Note: In KRaft mode, when enabling this config dynamically, it needs to wait for the unclean leader election" +
        "thread to trigger election periodically (default is 5 minutes). Please run `kafka-leader-election.sh` with `unclean` option " +
         "to trigger the unclean leader election immediately if needed.</p>";

    public static final String MIN_IN_SYNC_REPLICAS_CONFIG = "min.insync.replicas";
    public static final String MIN_IN_SYNC_REPLICAS_DOC = "When a producer sets acks to \"all\" (or \"-1\"), " +
        "this configuration specifies the minimum number of replicas that must acknowledge " +
        "a write for the write to be considered successful. If this minimum cannot be met, " +
        "then the producer will raise an exception (either NotEnoughReplicas or " +
        "NotEnoughReplicasAfterAppend).<br>When used together, <code>min.insync.replicas</code> and <code>acks</code> " +
        "allow you to enforce greater durability guarantees. A typical scenario would be to " +
        "create a topic with a replication factor of 3, set <code>min.insync.replicas</code> to 2, and " +
        "produce with <code>acks</code> of \"all\". This will ensure that the producer raises an exception " +
        "if a majority of replicas do not receive a write.";

    public static final String COMPRESSION_TYPE_CONFIG = "compression.type";
    public static final String COMPRESSION_TYPE_DOC = "Specify the final compression type for a given topic. " +
        "This configuration accepts the standard compression codecs ('gzip', 'snappy', 'lz4', 'zstd'). It additionally " +
        "accepts 'uncompressed' which is equivalent to no compression; and 'producer' which means retain the " +
        "original compression codec set by the producer.";


    public static final String COMPRESSION_GZIP_LEVEL_CONFIG = "compression.gzip.level";
    public static final String COMPRESSION_GZIP_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>gzip</code>.";
    public static final String COMPRESSION_LZ4_LEVEL_CONFIG = "compression.lz4.level";
    public static final String COMPRESSION_LZ4_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>lz4</code>.";
    public static final String COMPRESSION_ZSTD_LEVEL_CONFIG = "compression.zstd.level";
    public static final String COMPRESSION_ZSTD_LEVEL_DOC = "The compression level to use if " + COMPRESSION_TYPE_CONFIG + " is set to <code>zstd</code>.";

    public static final String PREALLOCATE_CONFIG = "preallocate";
    public static final String PREALLOCATE_DOC = "True if we should preallocate the file on disk when " +
        "creating a new log segment.";

    public static final String MESSAGE_TIMESTAMP_TYPE_CONFIG = "message.timestamp.type";
    public static final String MESSAGE_TIMESTAMP_TYPE_DOC = "Define whether the timestamp in the message is " +
        "message create time or log append time.";

    public static final String MESSAGE_TIMESTAMP_BEFORE_MAX_MS_CONFIG = "message.timestamp.before.max.ms";
    public static final String MESSAGE_TIMESTAMP_BEFORE_MAX_MS_DOC = "This configuration sets the allowable timestamp " +
        "difference between the broker's timestamp and the message timestamp. The message timestamp can be earlier than " +
        "or equal to the broker's timestamp, with the maximum allowable difference determined by the value set in this " +
        "configuration. If message.timestamp.type=CreateTime, the message will be rejected if the difference in " +
        "timestamps exceeds this specified threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.";
    public static final String MESSAGE_TIMESTAMP_AFTER_MAX_MS_CONFIG = "message.timestamp.after.max.ms";
    public static final String MESSAGE_TIMESTAMP_AFTER_MAX_MS_DOC = "This configuration sets the allowable timestamp " +
        "difference between the message timestamp and the broker's timestamp. The message timestamp can be later than " +
        "or equal to the broker's timestamp, with the maximum allowable difference determined by the value set in this " +
        "configuration. If message.timestamp.type=CreateTime, the message will be rejected if the difference in " +
        "timestamps exceeds this specified threshold. This configuration is ignored if message.timestamp.type=LogAppendTime.";

    /**
     * @deprecated down-conversion is not possible in Apache Kafka 4.0 and newer, hence this configuration is a no-op,
     *             and it is deprecated for removal in Apache Kafka 5.0.
     */
    @Deprecated
    public static final String MESSAGE_DOWNCONVERSION_ENABLE_CONFIG = "message.downconversion.enable";

    /**
     * @deprecated see {@link #MESSAGE_DOWNCONVERSION_ENABLE_CONFIG}.
     */
    @Deprecated
    public static final String MESSAGE_DOWNCONVERSION_ENABLE_DOC = "Down-conversion is not possible in Apache Kafka 4.0 and newer, " +
        "hence this configuration is no-op and it is deprecated for removal in Apache Kafka 5.0.";
}
