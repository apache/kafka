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
package org.apache.kafka.clients.producer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_EPOCH;
import static org.apache.kafka.common.record.RecordBatch.NO_PRODUCER_ID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionState {
    private volatile PidAndEpoch pidAndEpoch;
    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final Time time;

    public static class PidAndEpoch {
        public final long producerId;
        public final short epoch;

        PidAndEpoch(long producerId, short epoch) {
            this.producerId = producerId;
            this.epoch = epoch;
        }

        public boolean isValid() {
            return NO_PRODUCER_ID < producerId;
        }
    }

    public TransactionState(Time time) {
        this.pidAndEpoch = new PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers = new HashMap<>();
        this.time = time;
    }

    public boolean hasPid() {
        return pidAndEpoch.isValid();
    }

    /**
     * A blocking call to get the pid and epoch for the producer. If the PID and epoch has not been set, this method
     * will block for at most maxWaitTimeMs. It is expected that this method be called from application thread
     * contexts (ie. through Producer.send). The PID it self will be retrieved in the background thread.
     * @param maxWaitTimeMs The maximum time to block.
     * @return a PidAndEpoch object. Callers must call the 'isValid' method fo the returned object to ensure that a
     *         valid Pid and epoch is actually returned.
     */
    public synchronized PidAndEpoch awaitPidAndEpoch(long maxWaitTimeMs) throws InterruptedException {
        long start = time.milliseconds();
        long elapsed = 0;
        while (!hasPid() && elapsed < maxWaitTimeMs) {
            wait(maxWaitTimeMs);
            elapsed = time.milliseconds() - start;
        }
        return pidAndEpoch;
    }

    /**
     * Get the current pid and epoch without blocking. Callers must use {@link PidAndEpoch#isValid()} to
     * verify that the result is valid.
     *
     * @return the current PidAndEpoch.
     */
    public PidAndEpoch pidAndEpoch() {
        return pidAndEpoch;
    }

    /**
     * Set the pid and epoch atomically. This method will signal any callers blocked on the `pidAndEpoch` method
     * once the pid is set. This method will be called on the background thread when the broker responds with the pid.
     */
    public synchronized void setPidAndEpoch(long pid, short epoch) {
        this.pidAndEpoch = new PidAndEpoch(pid, epoch);
        if (this.pidAndEpoch.isValid())
            notifyAll();
    }

    /**
     * This method is used when the producer needs to reset it's internal state because of an irrecoverable exception
     * from the broker.
     *
     * We need to reset the producer id and associated state when we have sent a batch to the broker, but we either get
     * a non-retriable exception or we run out of retries, or the batch expired in the producer queue after it was already
     * sent to the broker.
     *
     * In all of these cases, we don't know whether batch was actually committed on the broker, and hence whether the
     * sequence number was actually updated. If we don't reset the producer state, we risk the chance that all future
     * messages will return an OutOfOrderSequenceException.
     */
    public synchronized void resetProducerId() {
        setPidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH);
        this.sequenceNumbers.clear();
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    public synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null) {
            currentSequenceNumber = 0;
            sequenceNumbers.put(topicPartition, currentSequenceNumber);
        }
        return currentSequenceNumber;
    }

    public synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        Integer currentSequenceNumber = sequenceNumbers.get(topicPartition);
        if (currentSequenceNumber == null)
            throw new IllegalStateException("Attempt to increment sequence number for a partition with no current sequence.");

        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }
}
