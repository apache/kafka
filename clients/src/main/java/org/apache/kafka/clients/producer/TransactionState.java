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

import static org.apache.kafka.common.record.LogEntry.NO_PID;

/**
 * A class which maintains state for transactions. Also keeps the state necessary to ensure idempotent production.
 */
public class TransactionState {
    private volatile PidAndEpoch pidAndEpoch;
    private final Map<TopicPartition, Integer> sequenceNumbers;
    private final Time time;

    public static class PidAndEpoch {
        public final long pid;
        public final short epoch;

        PidAndEpoch(long pid, short epoch) {
            this.pid = pid;
            this.epoch = epoch;
        }

        public boolean isValid() {
            return pid != NO_PID;
        }
    }

    public TransactionState(Time time) {
        pidAndEpoch = new PidAndEpoch(NO_PID, (short) 0);
        sequenceNumbers = new HashMap<>();
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
    public synchronized PidAndEpoch pidAndEpoch(long maxWaitTimeMs) throws InterruptedException {
        while (!hasPid() && 0 < maxWaitTimeMs) {
            long start = time.milliseconds();
            wait(maxWaitTimeMs);
            maxWaitTimeMs = maxWaitTimeMs - (time.milliseconds() - start);
        }
        return pidAndEpoch;
    }

    public PidAndEpoch pidAndEpoch() {
        PidAndEpoch pidAndEpoch = null;
        while (pidAndEpoch == null) {
            try {
                pidAndEpoch = pidAndEpoch(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                // swallow
            }
        }
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
    */
    public synchronized void reset() {
        setPidAndEpoch(NO_PID, (short) 0);
        this.sequenceNumbers.clear();
    }

    /**
     * Returns the next sequence number to be written to the given TopicPartition.
     */
    public synchronized Integer sequenceNumber(TopicPartition topicPartition) {
        if (!sequenceNumbers.containsKey(topicPartition)) {
            sequenceNumbers.put(topicPartition, 0);
        }
        return sequenceNumbers.get(topicPartition);
    }

    public synchronized void incrementSequenceNumber(TopicPartition topicPartition, int increment) {
        if (!sequenceNumbers.containsKey(topicPartition)) {
            sequenceNumbers.put(topicPartition, 0);
        }
        int currentSequenceNumber = sequenceNumbers.get(topicPartition);
        currentSequenceNumber += increment;
        sequenceNumbers.put(topicPartition, currentSequenceNumber);
    }
}
