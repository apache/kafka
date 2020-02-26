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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.TaskMigratedException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * {@code StreamsProducer} manages the producers within a Kafka Streams application.
 * <p>
 * If EOS is enabled, it is responsible to init and begin transactions if necessary.
 * It also tracks the transaction status, ie, if a transaction is in-fight.
 * <p>
 * For non-EOS, the user should not call transaction related methods.
 */
public class StreamsProducer {
    private final Logger log;

    private final Producer<byte[], byte[]> producer;
    private final String applicationId;
    private final TaskId taskId;
    private final String logMessage;
    private final boolean eosEnabled;

    private boolean transactionInFlight = false;
    private boolean transactionInitialized = false;

    public StreamsProducer(final LogContext logContext,
                           final Producer<byte[], byte[]> producer) {
        this(logContext, producer, null, null);
    }

    public StreamsProducer(final LogContext logContext,
                           final Producer<byte[], byte[]> producer,
                           final String applicationId,
                           final TaskId taskId) {
        if ((applicationId != null && taskId == null) ||
            (applicationId == null && taskId != null)) {
            throw new IllegalArgumentException("applicationId and taskId must either be both null or both be not null");
        }

        this.log = logContext.logger(getClass());

        this.producer = Objects.requireNonNull(producer, "producer cannot be null");
        this.applicationId = applicationId;
        this.taskId = taskId;
        if (taskId != null) {
            logMessage = "task " + taskId.toString();
            eosEnabled = true;
        } else {
            logMessage = "all owned active tasks";
            eosEnabled = false;
        }
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     */
    public void initTransaction() {
        if (!eosEnabled) {
            throw new IllegalStateException("EOS is disabled");
        }
        if (!transactionInitialized) {
            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            try {
                producer.initTransactions();
                transactionInitialized = true;
            } catch (final TimeoutException exception) {
                log.warn("Timeout exception caught when initializing transactions for {}. " +
                    "\nThe broker is either slow or in bad state (like not having enough replicas) in responding to the request, " +
                    "or the connection to broker was interrupted sending the request or receiving the response. " +
                    "Will retry initializing the task in the next loop. " +
                    "\nConsider overwriting {} to a larger value to avoid timeout errors",
                    logMessage,
                    ProducerConfig.MAX_BLOCK_MS_CONFIG);

                throw exception;
            } catch (final KafkaException exception) {
                throw new StreamsException("Error encountered while initializing transactions for " + logMessage, exception);
            }
        }
    }

    private void maybeBeginTransaction() throws ProducerFencedException {
        if (eosEnabled && !transactionInFlight) {
            try {
                producer.beginTransaction();
                transactionInFlight = true;
            } catch (final ProducerFencedException error) {
                throw new TaskMigratedException("Producer get fenced trying to begin a new transaction", error);
            } catch (final KafkaException error) {
                throw new StreamsException("Producer encounter unexpected error trying to begin a new transaction for " + logMessage, error);
            }
        }
    }

    public Future<RecordMetadata> send(final ProducerRecord<byte[], byte[]> record,
                                       final Callback callback) {
        maybeBeginTransaction();
        try {
            return producer.send(record, callback);
        } catch (final KafkaException uncaughtException) {
            if (isRecoverable(uncaughtException)) {
                // producer.send() call may throw a KafkaException which wraps a FencedException,
                // in this case we should throw its wrapped inner cause so that it can be captured and re-wrapped as TaskMigrationException
                throw new TaskMigratedException("Producer cannot send records anymore since it got fenced", uncaughtException.getCause());
            } else {
                final String errorMessage = String.format(
                    "Error encountered sending record to topic %s%s due to:%n%s",
                    record.topic(),
                    taskId == null ? "" : " " + logMessage,
                    uncaughtException.toString());
                throw new StreamsException(errorMessage, uncaughtException);
            }
        }
    }

    private static boolean isRecoverable(final KafkaException uncaughtException) {
        return uncaughtException.getCause() instanceof ProducerFencedException ||
            uncaughtException.getCause() instanceof UnknownProducerIdException;
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     * @throws TaskMigratedException
     */
    public void commitTransaction(final Map<TopicPartition, OffsetAndMetadata> offsets) throws ProducerFencedException {
        if (!eosEnabled) {
            throw new IllegalStateException("EOS is disabled");
        }
        maybeBeginTransaction();
        try {
            producer.sendOffsetsToTransaction(offsets, applicationId);
            producer.commitTransaction();
            transactionInFlight = false;
        } catch (final ProducerFencedException error) {
            throw new TaskMigratedException("Producer get fenced trying to commit a transaction", error);
        } catch (final TimeoutException error) {
            // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
            throw new StreamsException("Timed out while committing a transaction for " + logMessage, error);
        } catch (final KafkaException error) {
            throw new StreamsException("Producer encounter unexpected error trying to commit a transaction for " + logMessage, error);
        }
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     */
    public void abortTransaction() throws ProducerFencedException {
        if (!eosEnabled) {
            throw new IllegalStateException("EOS is disabled");
        }
        if (transactionInFlight) {
            try {
                producer.abortTransaction();
            } catch (final ProducerFencedException ignore) {
                /* TODO
                 * this should actually never happen atm as we guard the call to #abortTransaction
                 * -> the reason for the guard is a "bug" in the Producer -- it throws IllegalStateException
                 * instead of ProducerFencedException atm. We can remove the isZombie flag after KAFKA-5604 got
                 * fixed and fall-back to this catch-and-swallow code
                 */

                // can be ignored: transaction got already aborted by brokers/transactional-coordinator if this happens
            } catch (final KafkaException error) {
                throw new StreamsException("Producer encounter unexpected error trying to abort a transaction for " + logMessage, error);
            }
            transactionInFlight = false;
        }
    }

    public List<PartitionInfo> partitionsFor(final String topic) throws TimeoutException {
        return producer.partitionsFor(topic);
    }

    public void flush() {
        producer.flush();
    }

    public void close() {
        if (eosEnabled) {
            try {
                producer.close();
            } catch (final KafkaException error) {
                throw new StreamsException("Producer encounter unexpected " +
                    "error trying to close" + (taskId == null ? "" : " " + logMessage), error);
            }
        }
    }

    // for testing only
    Producer<byte[], byte[]> kafkaProducer() {
        return producer;
    }
}
