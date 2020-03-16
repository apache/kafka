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
    private final String logPrefix;

    private final Producer<byte[], byte[]> producer;
    private final String applicationId;
    private final boolean eosEnabled;

    private boolean transactionInFlight = false;
    private boolean transactionInitialized = false;

    public StreamsProducer(final Producer<byte[], byte[]> producer,
                           final boolean eosEnabled,
                           final LogContext logContext,
                           final String applicationId) {
        log = logContext.logger(getClass());
        logPrefix = logContext.logPrefix().trim();

        this.producer = Objects.requireNonNull(producer, "producer cannot be null");
        this.applicationId = applicationId;
        this.eosEnabled = eosEnabled;
    }

    private String formatException(final String message) {
        return message + " [" + logPrefix + ", " + (eosEnabled ? "eos" : "alo") + "]";
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     */
    public void initTransaction() {
        if (!eosEnabled) {
            throw new IllegalStateException(formatException("EOS is disabled"));
        }
        if (!transactionInitialized) {
            // initialize transactions if eos is turned on, which will block if the previous transaction has not
            // completed yet; do not start the first transaction until the topology has been initialized later
            try {
                producer.initTransactions();
                transactionInitialized = true;
            } catch (final TimeoutException exception) {
                log.warn(
                    "Timeout exception caught when initializing transactions. " +
                        "The broker is either slow or in bad state (like not having enough replicas) in " +
                        "responding to the request, or the connection to broker was interrupted sending " +
                        "the request or receiving the response. " +
                        "Will retry initializing the task in the next loop. " +
                        "Consider overwriting {} to a larger value to avoid timeout errors",
                    ProducerConfig.MAX_BLOCK_MS_CONFIG
                );

                throw exception;
            } catch (final KafkaException exception) {
                throw new StreamsException(
                    formatException("Error encountered while initializing transactions"),
                    exception
                );
            }
        }
    }

    private void maybeBeginTransaction() throws ProducerFencedException {
        if (eosEnabled && !transactionInFlight) {
            try {
                producer.beginTransaction();
                transactionInFlight = true;
            } catch (final ProducerFencedException error) {
                throw new TaskMigratedException(
                    formatException("Producer get fenced trying to begin a new transaction"),
                    error
                );
            } catch (final KafkaException error) {
                throw new StreamsException(
                    formatException("Producer encounter unexpected error trying to begin a new transaction"),
                    error
                );
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
                // in this case we should throw its wrapped inner cause so that it can be
                // captured and re-wrapped as TaskMigrationException
                throw new TaskMigratedException(
                    formatException("Producer cannot send records anymore since it got fenced"),
                    uncaughtException.getCause()
                );
            } else {
                throw new StreamsException(
                    formatException(String.format("Error encountered sending record to topic %s", record.topic())),
                    uncaughtException
                );
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
            throw new IllegalStateException(formatException("EOS is disabled"));
        }
        maybeBeginTransaction();
        try {
            producer.sendOffsetsToTransaction(offsets, applicationId);
            producer.commitTransaction();
            transactionInFlight = false;
        } catch (final ProducerFencedException error) {
            throw new TaskMigratedException(
                formatException("Producer get fenced trying to commit a transaction"),
                error
            );
        } catch (final TimeoutException error) {
            // TODO KIP-447: we can consider treating it as non-fatal and retry on the thread level
            throw new StreamsException(formatException("Timed out while committing a transaction"), error);
        } catch (final KafkaException error) {
            throw new StreamsException(
                formatException("Producer encounter unexpected error trying to commit a transaction"),
                error
            );
        }
    }

    /**
     * @throws IllegalStateException if EOS is disabled
     */
    public void abortTransaction() throws ProducerFencedException {
        if (!eosEnabled) {
            throw new IllegalStateException(formatException("EOS is disabled"));
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
                throw new StreamsException(
                    formatException("Producer encounter unexpected error trying to abort a transaction"),
                    error
                );
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

    // for testing only
    Producer<byte[], byte[]> kafkaProducer() {
        return producer;
    }
}
