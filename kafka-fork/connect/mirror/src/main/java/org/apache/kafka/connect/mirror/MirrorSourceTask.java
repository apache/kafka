package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    public MirrorSourceTask() {}

    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
                     ReplicationPolicy replicationPolicy, OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        this.consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.", Thread.currentThread().getName(),
            taskTopicPartitions.size(), sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        if (offsetSyncWriter != null) {
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer."); 
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(), System.currentTimeMillis() - start);
    }
   
    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            consumerAccess.release();
            return null;
        }
        try {
            validateSourceTopicState();
            
            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);

            // =================================================================
            // TASK 2 FAIL-FAST TRUNCATION DETECTOR
            // =================================================================
            if (consumer.assignment() != null && !consumer.assignment().isEmpty()) {
                try {
                    // Query the primary cluster for the beginning (earliest) offsets currently available
                    java.util.Map<TopicPartition, Long> earliestOffsets = consumer.beginningOffsets(consumer.assignment());
                    for (TopicPartition tp : consumer.assignment()) {
                        long currentPosition = consumer.position(tp);
                        Long logStartOffset = earliestOffsets.get(tp);

                        // If MirrorMaker expects an offset that has been completely pruned/deleted on the source cluster
                        if (logStartOffset != null && currentPosition < logStartOffset) {
                            log.error("Source log truncation detected for partition {}! Current replication position {} is behind source log start offset {}.", 
                                tp, currentPosition, logStartOffset);
                            throw new KafkaException("Source log truncation detected");
                        }
                    }
                } catch (KafkaException ke) {
                    // Propagate our specific error upwards to force a container fail-fast crash
                    throw ke;
                } catch (Exception ex) {
                    log.warn("Non-fatal evaluation error while checking partition boundary limits: ", ex);
                }
            }
            // =================================================================

            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));
            }
            if (sourceRecords.isEmpty()) {
                return null;
            } else {
                return sourceRecords;
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            return null;
        } catch (OffsetOutOfRangeException e) {
            // Hard capture fallback if the driver surfaces the exception before our validation block
            log.error("Source log truncation detected due to OffsetOutOfRangeException! Consumer offset is out of bounds.", e);
            throw new KafkaException("Source log truncation detected", e);
        } catch (KafkaException e) {
            throw e; 
        } finally {
            consumerAccess.release();
        }
    }

    private void validateSourceTopicState() {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) return;
        
        Map<TopicPartition, Long> beginningOffsets;
        Map<TopicPartition, Long> endOffsets;
        try {
            beginningOffsets = consumer.beginningOffsets(assignment);
            endOffsets = consumer.endOffsets(assignment);
        } catch (Exception e) {
            return;
        }

        for (TopicPartition tp : assignment) {
            long currentPosition;
            try {
                currentPosition = consumer.position(tp);
            } catch (OffsetOutOfRangeException e) {
                handleOffsetBreach(Set.of(tp));
                return;
            }

            long beginningOffset = beginningOffsets.getOrDefault(tp, 0L);
            long endOffset = endOffsets.getOrDefault(tp, 0L);

            // Task 3: Administrative reset check
            if (beginningOffset == 0 && currentPosition > endOffset) {
                log.warn("Detected source topic reset for {}. Re-aligning offsets to 0.", tp);
                consumer.seek(tp, 0L);
                continue;
            }

            // Task 2: Critical truncation block
            if (beginningOffset > 0 && currentPosition < beginningOffset) {
                log.error("Source log truncation detected for partition {}", tp);
                throw new KafkaException("Source log truncation detected for " + tp + ". Failing fast.");
            }
        }
    }

    private void handleOffsetBreach(Set<TopicPartition> breachedPartitions) {
        if (breachedPartitions == null || breachedPartitions.isEmpty()) return;

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(breachedPartitions);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(breachedPartitions);

        for (TopicPartition tp : breachedPartitions) {
            long beginningOffset = beginningOffsets.getOrDefault(tp, 0L);
            long endOffset = endOffsets.getOrDefault(tp, 0L);
            long currentPosition;
            try {
                currentPosition = consumer.position(tp);
            } catch (Exception e) {
                currentPosition = beginningOffset - 1;
            }

            if (beginningOffset == 0 && currentPosition > endOffset) {
                log.warn("Detected source topic reset for {} during breach handling. Re-aligning offsets to 0.", tp);
                consumer.seek(tp, 0L);
                continue;
            }

            if (beginningOffset > 0 && currentPosition < beginningOffset) {
                log.error("Source log truncation detected for partition {}", tp);
                throw new KafkaException("Source log truncation detected for " + tp + ". Failing fast.");
            }
        }
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping || metadata == null || !metadata.hasOffset()) return;
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, System.currentTimeMillis() - record.timestamp());
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }
 
    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());

        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(topicPartitionOffsets.keySet());
        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (isUncommitted(offset)) return;
            
            long nextOffset = offset + 1L;
            long beginningOffset = beginningOffsets.getOrDefault(topicPartition, 0L);
            
            if (beginningOffset > nextOffset) {
                log.error("Source log truncation detected for partition {}", topicPartition);
                throw new KafkaException("Source log truncation detected for " + topicPartition + ". Failing fast.");
            }
            if (beginningOffset == 0 && nextOffset > 0) {
                log.warn("Detected source topic reset for {}. Re-aligning offsets to 0.", topicPartition);
                consumer.seek(topicPartition, 0L);
                return;
            }
            consumer.seek(topicPartition, nextOffset);
        });
    }

    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        return (bytes == null) ? 0 : bytes.length;
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }
}