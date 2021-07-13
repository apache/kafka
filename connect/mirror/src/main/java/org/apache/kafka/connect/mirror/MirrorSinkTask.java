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

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.converters.ByteArrayConverter;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.CommitFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/** Replicates a set of topic-partitions. */
public class MirrorSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MirrorSinkTask.class);
    private static final int DEFAULT_ABORT_RETRY_BACKOFF_MS = 300;
    private static final int MAX_OUTSTANDING_OFFSET_SYNCS = 10;
    
    private KafkaProducer<byte[], byte[]> producer;
    private KafkaProducer<byte[], byte[]> offsetProducer;
    private String sourceClusterAlias;
    private MirrorMetrics metrics;
    // use kafka transaction or not
    private boolean isTransactional;
    // a transaction currently open or not
    private boolean transactionOpen = false;
    private int maxRetries = 10; // TODO: make configuragble

    private Converter converter;
    private MirrorTaskConfig config;
    private String connectConsumerGroup;
    protected long abortRetryBackoffMs = DEFAULT_ABORT_RETRY_BACKOFF_MS; // TODO: make configuragble
    private Admin targetAdminClient;
    private Admin sourceAdminClient;
    private Set<TopicPartition> taskTopicPartitions;
    private ReplicationPolicy replicationPolicy;
    private Scheduler scheduler;
    private Map<TopicPartition, Long> currentOffsets = new HashMap<>();
    
    public MirrorSinkTask() {}

    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
    
    @Override
    public void start(Map<String, String> props) {
        config = new MirrorTaskConfig(props);
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        taskTopicPartitions = config.taskTopicPartitions();
        isTransactional = config.transactionalProducer();
        replicationPolicy = config.replicationPolicy();
        connectConsumerGroup = config.connectorConsumerGroup();
        producer = initProducer();       
        initTransactions(producer);
        offsetProducer = MirrorUtils.newProducer(config.sourceProducerConfig());
        converter = new ByteArrayConverter();
        converter.configure(Collections.emptyMap(), false);

        if (isTransactional) {
            loadContextOffsets();
        }
    }
    
    @Override
    public void open(Collection<TopicPartition> partitions) {
        if (isTransactional) {
            loadContextOffsets();
        }
    }
    
    /*
     * load SinkTaskContext with self-managed offsets only for the partitions assigned to this task
     */
    private void loadContextOffsets() {
        log.info("trying to load offsets from target cluster");
        targetAdminClient = AdminClient.create(config.targetAdminConfig());

        scheduler = new Scheduler(MirrorSinkTask.class, config.adminTimeout());
        scheduler.execute(this::loadInitialOffsets, "load initial consumer offsets on target cluster");
        
        Set<TopicPartition> assignments = context.assignment();
        log.info("assignments:");
        
        assignments.forEach(x -> log.info("{}", x.toString()));
        // only keep the offsets of the partitions assigned to this task
        Map<TopicPartition, Long> contextOffsets = assignments.stream()
                                                              .filter(x -> currentOffsets.containsKey(x))
                                                              .collect(Collectors.toMap(
                                                                  x -> x, x -> currentOffsets.get(x)));

        log.info("loaded consumer offsets:");
        contextOffsets.entrySet().forEach(x -> log.info("{}, {}", x.getKey(), x.getValue()));
        context.offset(contextOffsets);
    }

    /*
     * read SinkConnector's group offsets from target cluster
     */
    private void loadInitialOffsets() throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> wrappedOffsets = 
               MirrorUtils.listConsumerGroupOffset(targetAdminClient, connectConsumerGroup);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : wrappedOffsets.entrySet()) {
            TopicPartition targetTopicPartition = entry.getKey();
            //  restore the remote topic name on target cluster to the source topic name
            String sourceTopic = restoreSourceTopic(targetTopicPartition.topic());
            TopicPartition sourceTopicPartition = new TopicPartition(sourceTopic, targetTopicPartition.partition());
            // To rewind consumer to correct offset per <topic, partition> assigned to this task, there are more than one case:
            // (1) offset exists on target cluster, use (target offset + 1) as the starting point of consumption
            // (2) offset DOES NOT exist on target cluster, e.g. a new topic, set offset to 0
            if (entry.getValue().offset() == 0)
                currentOffsets.put(sourceTopicPartition, 0L);
            else
                currentOffsets.put(sourceTopicPartition, entry.getValue().offset() + 1);
 
        }
    }

    /*
     * create trannsaction or non-transaciton Kafka producer 
     */
    private KafkaProducer<byte[], byte[]> initProducer() {
        Map<String, Object> producerConfig = config.targetProducerConfig(); 
        String uniqueId = getHostName() + context.toString(); 
        if (isTransactional) {
            log.info("use transactional producer with tran Id: {} ", uniqueId);
            producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
            producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            producerConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, uniqueId);
        }
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, uniqueId);
        return MirrorUtils.newProducer(producerConfig);
    }

    /*
     * Receive records from consumer in WorkerSinkTask and call sendBatchWithRetries() to forward
     * them to producer
     * 
     * @see org.apache.kafka.connect.sink.SinkTask#put(java.util.Collection)
     */
    @Override
    public void put(Collection<SinkRecord> records) {

        if (records.size() == 0) {
            return;
        }
        log.info("receive {} messages from consumner", records.size());

        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
        for (SinkRecord record : records) {
            // use binary converter to translate from SinkRecord to byte[]
            byte[] value = converter.fromConnectData(record.topic(), record.valueSchema(), record.value());
            byte[] key = converter.fromConnectData(record.topic(), record.keySchema(), record.key());
            addConsumerRecordToRecordsMap(new ConsumerRecord<byte[], byte[]>(
                formatRemoteTopic(record.topic()), // prefix the topic with source cluster alias
                record.kafkaPartition(), record.kafkaOffset(), key, value), recordsMap);
        }
 
        ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<byte[], byte[]>(recordsMap);

        try {
            sendBatchWithRetries(consumerRecords);
        } catch (RebalanceException e) {
            // when a rebalance occurs, do not abort the consumer as this will cause rebalance again.
            log.warn("Rebalance may occurred during transaction.");
            producer.close();
            producer = initProducer();  
        } catch (Throwable e) {
            log.error(getHostName() + " terminating on exception: {}", e);
        }
    }

    /*
     * Call sendBatch() with retries.
     *
     * If an abortable Exception occurs, this method will abort the transaction and
     * retry up to maxRetries times, with an increasing backoff between attempts.
     */
    protected void sendBatchWithRetries(ConsumerRecords<byte[], byte[]> records) throws Throwable {
        for (int retries = 0; retries < maxRetries; retries++) {
            try {
                sendBatch(records);
            } catch (RemainingRecordsException remainingRecordsException) {
                log.info("Caught remainingRecordsException with num " + remainingRecordsException.getRemainingRecords().count() + " of " + records.count());
                Throwable cause = remainingRecordsException.getCause();

                // remainingRecordsException returns a set of records that have not yet been sent
                // if NOT transactional, only retry the remaining records, not all
                records = remainingRecordsException.getRemainingRecords();
                if (cause instanceof ProducerFencedException ||
                        cause instanceof OutOfOrderSequenceException ||
                        cause instanceof AuthorizationException ||
                        cause instanceof UnsupportedVersionException) {

                    // from Kafka Producer doc, due to above exceptions can not be recovered, 
                    // the only option is to throw and close the producer
                    log.error(getHostName() + " non-abortable transaction exception: {}", cause.getMessage());
                    throw cause;
                }

                // for any other KafkaException, abort the transaction and retry
                abortTransaction(producer, "on exception: " + cause.getMessage());

                if (retries < maxRetries - 1) {
                    long backOffMillis = abortRetryBackoffMs * retries;
                    log.info("Sleep " + backOffMillis + " before retry " + retries);
                    Thread.sleep(backOffMillis);
                    continue;
                }
                throw new TransactionRetriesExceededException("Produce exceeded retries after " + retries + " attempts.", cause);
            } catch (CommitFailedException e) {
                // CommitFailedException may thrown when rebalance is in progress
                // abort the transaction and throw RebalanceException
                abortTransaction(producer, "on rebalance");
                throw new RebalanceException();
            } catch (RuntimeException e) {
                // not much to do for RuntimeException
                throw e;
            }
            // if reach here, do not need to retry further
            return;
        }
    }
    
    private LinkedList<ConsumerRecord<byte[], byte[]>> populateOffsetMap(ConsumerRecords<byte[], byte[]> records, 
            Map<TopicPartition, OffsetAndMetadata> offsetsMap) {
        List<ConsumerRecord<byte[], byte[]>> outRecords;
        outRecords = records.partitions().stream().map(records::records)
                .flatMap(Collection::stream)
                .peek(consumerRecord -> {
                    offsetsMap.compute(new TopicPartition(consumerRecord.topic(), consumerRecord.partition()),
                        (tp, curOffsetMeta) ->
                                   (curOffsetMeta == null || consumerRecord.offset() > curOffsetMeta.offset())
                                            ?
                                            new OffsetAndMetadata(consumerRecord.offset())
                                            : curOffsetMeta);
                })
                .collect(Collectors.toList());

        return new LinkedList<>(outRecords);
    }
    /*
     * if transaction enabled, use transactional producer which also commits the consumer offsets as a part of the transaction, 
     */
    protected void sendBatch(ConsumerRecords<byte[], byte[]> records) throws RemainingRecordsException, Throwable {
        // map of the largest offset for each partition in the current batch
        Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();
        // map of the remaining records for each partition
        Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> remainingRecordsMap = new HashMap<>();
        // populate offset map from consumer records and flatten them into linkedlist
        LinkedList<ConsumerRecord<byte[], byte[]>> outList = populateOffsetMap(records, offsetsMap);
        // track the Future from Producer::send
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        Map<Future<RecordMetadata>, ConsumerRecord<byte[], byte[]>> futureMap = new HashMap<>();
        Throwable lastException = null;
        KafkaException lastKafkaException = null;
        log.info("preparing to produce {} messages", outList.size());
        try {
            // if transaction enabled, begin a new transaction 
            beginTransaction(producer);

            ConsumerRecord<byte[], byte[]> consumerRecord;
            while ((consumerRecord = outList.peek()) != null) {
                // Construct a producer record and send
                ProducerRecord<byte[], byte[]> producerRecord = convertToProducerRecord(consumerRecord);
                Future<RecordMetadata> future = producer.send(producerRecord);
                futures.add(future);
                futureMap.put(future, consumerRecord);
                // pop current record off from the list of remaining records.
                outList.poll();
            }
        } catch (KafkaException e) {
            // exception may be thrown when sending messages
            // any unsent messages are added to the remaining list
            for (ConsumerRecord<byte[], byte[]> record = outList.poll(); record != null; record = outList.poll()) {
                addConsumerRecordToRecordsMap(record, remainingRecordsMap);
            }
            lastKafkaException = e;
        } finally {
            // some or all messages are sent to kafka, then resolve the futures
            // any futures which have failed are added to the remainingRecordsMap
            for (Future<RecordMetadata> future : futures) {
                try {
                    RecordMetadata metadata = future.get();
                    // TODO: retrieve the offset from metadata and invoke the same logic as 
                    // CommitRecord() in MirrorSourceTask about sending OffsetSync
                } catch (Exception e) {
                    ConsumerRecord<byte[], byte[]> failedRecords = futureMap.get(future);
                    // when record failed to send, add it to the list of remaining records
                    addConsumerRecordToRecordsMap(failedRecords, remainingRecordsMap);
                    lastException = e.getCause();
                    if (KafkaException.class.isAssignableFrom(e.getCause().getClass())) {
                        lastKafkaException = (KafkaException) e.getCause();
                    }
                }
            }
            
            if (isTransactional && remainingRecordsMap.size() == 0) {
                producer.sendOffsetsToTransaction(offsetsMap, connectConsumerGroup);
                commitTransaction(producer);
            } else {
                // If a kafka exception is thrown and there are records that did not successfully produce
                // then throw a RemainingRecordsException and so the caller can retry the failed records.
                if ((remainingRecordsMap.size() > 0) && (lastKafkaException != null)) {
                    ConsumerRecords<byte[], byte[]> recordsToThrow;
                    if (isTransactional) {
                        // for transaction, all records are put into remainingRecords since the whole transaction should redo
                        recordsToThrow = records;
                    } else {
                        // non-transaction: only retry failed records, others already were finished and sent.
                        recordsToThrow = new ConsumerRecords<>(remainingRecordsMap);
                    }
                    throw new RemainingRecordsException(recordsToThrow, lastKafkaException);
                }
                if (lastException != null) {
                    throw lastException;
                }
            }
            
        }
    }
    
    @Override
    public void stop() {
        long start = System.currentTimeMillis();

        Utils.closeQuietly(producer, "sink producer");
        Utils.closeQuietly(offsetProducer, "offset producer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", getHostName(), System.currentTimeMillis() - start);
    }
    
    /**
     * convert a ProducerRecord to a ConsumerRecord with topic name prefix /w sourceClusterAlias 
     */
    protected ProducerRecord<byte[], byte[]> convertToProducerRecord(ConsumerRecord<byte[], byte[]> record) {
        return new ProducerRecord<>(record.topic(), record.key(), record.value());
    }
    
    private String restoreSourceTopic(String topic) {
        return replicationPolicy.restoreSourceTopic(sourceClusterAlias, topic);
    }
    
    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }
    
    private void beginTransaction(KafkaProducer<byte[], byte[]> producer) {
        if (isTransactional) {
            producer.beginTransaction();
            transactionOpen = true;
        }
    }

    private void initTransactions(KafkaProducer<byte[], byte[]> producer) {
        if (isTransactional) {
            producer.initTransactions();
        }
    }
    
    private void commitTransaction(KafkaProducer<byte[], byte[]> producer) {
        if (isTransactional) {
            producer.commitTransaction();
            transactionOpen = false;
        }
    }
    
    private void abortTransaction(KafkaProducer<byte[], byte[]> producer, String reason) {
        if (isTransactional && transactionOpen) {
            log.warn(getHostName() + " aborting transaction: " + reason);
            producer.abortTransaction();
            transactionOpen = false;
        }
    }
    
    /**
     * given a ConsumerRecord, add to to a list in the recordsMap keyed on the topic/partition
     */
    private void addConsumerRecordToRecordsMap(ConsumerRecord<byte[], byte[]> record,
                                             Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        List<ConsumerRecord<byte[], byte[]>> tpRecList = recordsMap.getOrDefault(tp, new ArrayList<>());
        tpRecList.add(record);
        recordsMap.put(tp, tpRecList);
    }
     
    protected String getHostName() {
        return System.getenv("HOSTNAME") == null ? "" : System.getenv("HOSTNAME");
    }
    
    private static class RebalanceException extends Exception {
    }
    
    private static class TransactionRetriesExceededException extends Exception {
        public TransactionRetriesExceededException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    public static class RemainingRecordsException extends Exception {
        private final ConsumerRecords<byte[], byte[]> remainingRecords;

        public RemainingRecordsException(ConsumerRecords<byte[], byte[]> remainingRecords,
                                         KafkaException cause) {
            super(cause);
            this.remainingRecords = remainingRecords;
        }

        public ConsumerRecords<byte[], byte[]> getRemainingRecords() {
            return remainingRecords;
        }
    }
}
