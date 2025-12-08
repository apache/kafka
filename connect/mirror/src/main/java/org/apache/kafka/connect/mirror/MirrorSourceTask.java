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
package org.apache.kafka.connect.mirror;

import java.net.ConnectException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Replicates a set of topic-partitions. */
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
	private Map<TopicPartition, Long> lastReplicatedSourceOffset = new HashMap<>();  // ← TASK 1: Silent data loss
	private Map<TopicPartition, Long> knownTopicCreationTime = new HashMap<>();
	private Map<TopicPartition, Long> knownLogStartOffset = new HashMap<>();// ← TASK 2: Topic reset detection

	public MirrorSourceTask() {
	}

	// for testing
	MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics, String sourceClusterAlias,
			ReplicationPolicy replicationPolicy, OffsetSyncWriter offsetSyncWriter) {
		this.consumer = consumer;
		this.metrics = metrics;
		this.sourceClusterAlias = sourceClusterAlias;
		this.replicationPolicy = replicationPolicy;
		consumerAccess = new Semaphore(1);
		this.offsetSyncWriter = offsetSyncWriter;
	}

	@Override
	public void start(Map<String, String> props) {
		MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
		consumerAccess = new Semaphore(1); // let one thread at a time access the consumer
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
		// Handle delayed and pending offset syncs only when offsetSyncWriter is
		// available
		if (offsetSyncWriter != null) {
			// Offset syncs which were not emitted immediately due to their offset spacing
			// should be sent periodically
			// This ensures that low-volume topics aren't left with persistent lag at the
			// end of the topic
			offsetSyncWriter.promoteDelayedOffsetSyncs();
			// Publish any offset syncs that we've queued up, but have not yet been able to
			// publish
			// (likely because we previously reached our limit for number of outstanding
			// syncs)
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
			log.warn("Interrupted waiting for access to consumer. Will try closing anyway.");
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
			return null;
		}
		try {
			ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
			List<SourceRecord> sourceRecords = new ArrayList<>(records.count());
			for (ConsumerRecord<byte[], byte[]> record : records) {
				SourceRecord converted = convertRecord(record);
				sourceRecords.add(converted);
				TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
				metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
				metrics.recordBytes(topicPartition, byteSize(record.value()));
			}
			if (!sourceRecords.isEmpty()) {
				detectTruncationIfAny(records);
			}
			if (sourceRecords.isEmpty()) {
				// WorkerSourceTasks expects non-zero batch size
				return null;
			} else {
				log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
				return sourceRecords;
			}
		} catch (WakeupException e) {
			return null;
		} catch (KafkaException e) {
			log.warn("Failure during poll.", e);
			return null;
		} catch (Throwable e) {
			log.error("Failure during poll.", e);
			// allow Connect to deal with the exception
			throw e;
		} finally {
			consumerAccess.release();
		}
	}

	private void detectTruncationIfAny(ConsumerRecords<byte[], byte[]> rawRecords) {
		if (rawRecords.isEmpty())
			return;

		Map<TopicPartition, Long> batchStartOffsets = new HashMap<>();

		 
		for (ConsumerRecord<byte[], byte[]> record : rawRecords) {
			TopicPartition tp = new TopicPartition(record.topic(), record.partition());
			long currentOffset = record.offset();

			// Agar pehli baar dekh rahe ho → daal do
			// Agar pehle se hai → sirf chhota wala rakho
			if (!batchStartOffsets.containsKey(tp)) {
				batchStartOffsets.put(tp, currentOffset);
			} else {
				long oldValue = batchStartOffsets.get(tp);
				if (currentOffset < oldValue) {
					batchStartOffsets.put(tp, currentOffset); // update to smaller one
				}
			}
		}

		 
		for (TopicPartition tp : batchStartOffsets.keySet()) {
			long actualStart = batchStartOffsets.get(tp);
			Long lastReplicated = lastReplicatedSourceOffset.get(tp);

			if (lastReplicated != null && actualStart > lastReplicated + 1) {
				long gap = actualStart - (lastReplicated + 1);

				String error = String.format(
						"[FATAL] SILENT DATA LOSS DETECTED! TopicPartition=%s | "
								+ "Last replicated source offset=%d | Expected next offset=%d | "
								+ "But poll started from=%d → GAP OF %d MESSAGES LOST FOREVER!",
						tp, lastReplicated, lastReplicated + 1, actualStart, gap);

				log.error(error);
				throw new org.apache.kafka.connect.errors.ConnectException(error);
			}
		}
	}

	@Override
	public void commitRecord(SourceRecord record, RecordMetadata metadata) {
		if (stopping) {
			return;
		}
		if (metadata == null) {
			log.debug(
					"No RecordMetadata (source record was probably filtered out during transformation) -- can't sync offsets for {}.",
					record.topic());
			return;
		}
		if (!metadata.hasOffset()) {
			log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
			return;
		}
		TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
		long sourceOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
		lastReplicatedSourceOffset.put(sourceTopicPartition, sourceOffset); // ← ye line add kar
		TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
		long latency = System.currentTimeMillis() - record.timestamp();
		metrics.countRecord(topicPartition);
		metrics.replicationLatency(topicPartition, latency);
		// Queue offset syncs only when offsetWriter is available
		if (offsetSyncWriter != null) {
			long upstreamOffset = sourceOffset;
			long downstreamOffset = metadata.offset();
			offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
			// We may be able to immediately publish an offset sync that we've queued up
			// here
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

	// visible for testing
	void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
		Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
		consumer.assign(topicPartitionOffsets.keySet());
		log.info("Starting with {} previously uncommitted partitions.",
				topicPartitionOffsets.values().stream().filter(this::isUncommitted).count());
		
		for (TopicPartition tp : taskTopicPartitions) {
	        Long savedOffset = topicPartitionOffsets.get(tp);

	        if (isTopicReset(tp)) {
	            log.warn("Topic reset detected for {}. Forcing replication from offset 0.", tp);
	            consumer.seek(tp, 0L);
	            continue; 
	        }

	        // Normal flow
	        if (isUncommitted(savedOffset)) {
	            log.trace("No committed offset found for {}, starting from beginning", tp);
	            consumer.seek(tp, 0L);
	        } else {
	            long nextOffset = savedOffset + 1;
	            log.trace("Resuming replication from offset {}", nextOffset);
	            consumer.seek(tp, nextOffset);
	        }
	    }
	}

	// visible for testing
	SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
		String targetTopic = formatRemoteTopic(record.topic());
		Headers headers = convertHeaders(record);
		return new SourceRecord(
				MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
				MirrorUtils.wrapOffset(record.offset()), targetTopic, record.partition(), Schema.OPTIONAL_BYTES_SCHEMA,
				record.key(), Schema.BYTES_SCHEMA, record.value(), record.timestamp(), headers);
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
		if (bytes == null) {
			return 0;
		} else {
			return bytes.length;
		}
	}

	private boolean isUncommitted(Long offset) {
		return offset == null || offset < 0;
	}
	
	private boolean isTopicReset(TopicPartition tp) {
	    try {
	        consumer.assign(java.util.Collections.singletonList(tp));
	        Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(
	            java.util.Collections.singletonList(tp)
	        );

	        Long currentBeginOffset = beginningOffsets.get(tp);

	        if (currentBeginOffset == null) {
	            log.warn("Could not fetch beginning offset for {}, assuming no reset", tp);
	            return false;
	        }

	        Long previouslyKnownBegin = knownLogStartOffset.get(tp);

	        if (previouslyKnownBegin == null) {
	            knownLogStartOffset.put(tp, currentBeginOffset);
	            return false;
	        }

	        if (currentBeginOffset < previouslyKnownBegin) {
	            log.warn("[TOPIC RESET DETECTED] {} – log start offset changed from {} to {} → topic was recreated!",
	                     tp, previouslyKnownBegin, currentBeginOffset);
	            knownLogStartOffset.put(tp, currentBeginOffset);
	            return true;
	        }

	        return false;

	    } catch (Exception e) {
	        log.warn("Failed to check topic reset for {}, assuming no reset", tp, e);
	        return false;
	    }
	}
}
