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
package org.apache.kafka.storage.internals.log;

/**
 * This class represents the verification state of a specific producer id.
 * It contains a VerificationGuard that is used to uniquely identify the transaction we want to verify.
 * After verifying, we retain this object until we append to the log. This prevents any race conditions where the transaction
 * may end via a control marker before we write to the log. This mechanism is used to prevent hanging transactions.
 * We remove the VerificationGuard whenever we write data to the transaction or write an end marker for the transaction.
 *
 * We also store the lowest seen sequence to block a higher sequence from being written in the case of the lower sequence needing retries.
 *
 * Any lingering entries that are never verified are removed via the producer state entry cleanup mechanism.
 */
public class VerificationStateEntry {

    final private long timestamp;
    final private VerificationGuard verificationGuard;
    private int lowestSequence;
    private short epoch;

    public VerificationStateEntry(long timestamp, int sequence, short epoch) {
        this.timestamp = timestamp;
        this.verificationGuard = new VerificationGuard();
        this.lowestSequence = sequence;
        this.epoch = epoch;
    }

    public long timestamp() {
        return timestamp;
    }

    public VerificationGuard verificationGuard() {
        return verificationGuard;
    }

    public int lowestSequence() {
        return lowestSequence;
    }

    public short epoch() {
        return epoch;
    }

    /**
     * An OutOfOrderSequence loop can happen for any idempotent/transactional producer when a lower sequence fails with
     * a retriable error and a higher sequence is successfully written. The lower sequence will fail with
     * OutOfOrderSequence and retry until retries run out.
     *
     * Here, we keep the lowest sequence seen in order to prevent an OutOfOrderSequence loop when verifying. This does
     * not solve the error loop for idempotent producers or transactional producers that fail before verification
     * starts. When verification fails with a retriable error (ie. NOT_COORDINATOR), the VerificationStateEntry
     * maintains the lowest sequence number it sees and blocks higher sequences from being written to the log. However,
     * if we encounter a new and lower sequence when verifying, we want to block sequences higher than that new
     * sequence. Additionally, if the epoch is bumped, the sequence is reset and any previous sequence must be disregarded.
     *
     * Thus, we update the lowest sequence if there is a batch needing verification that has:
     * a) a lower sequence for the same epoch
     * b) a higher epoch -- update the epoch here too
     */
    public void maybeUpdateLowestSequenceAndEpoch(int incomingSequence, short incomingEpoch) {
        if (incomingEpoch == epoch && incomingSequence < this.lowestSequence)
            this.lowestSequence = incomingSequence;
        if (incomingEpoch > this.epoch) {
            this.epoch = incomingEpoch;
            this.lowestSequence = incomingSequence;
        }
    }
}
