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

package org.apache.kafka.connect.integration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.connect.errors.DataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordLatches {

    private static final Logger log = LoggerFactory.getLogger(RecordLatches.class);

    private final String name;
    private final Map<String, Latch> recordsRemainingLatchesByTopic = new ConcurrentHashMap<>();
    private Latch recordsRemainingLatch;
    private Latch recordsToCommitLatch;

    public RecordLatches(String name) {
        this.name = name;
    }

    /**
     * Record a message arrival at the task and the connector overall.
     */
    public void record() {
        countDown(recordsRemainingLatch, 1);
    }

    /**
     * Record arrival of a batch of messages at the task and the connector overall.
     *
     * @param batchSize the number of messages
     */
    public void record(int batchSize) {
        countDown(recordsRemainingLatch, batchSize);
    }

    /**
     * Record a message arrival at the task and the connector overall.
     *
     * @param topic the name of the topic
     */
    public void record(String topic) {
        countDown(recordsRemainingLatchesByTopic.get(topic), 1);
        // also record a record for the "all topics" latch
        record();
    }

    /**
     * Record arrival of a batch of messages at the task and the connector overall.
     *
     * @param topic     the name of the topic
     * @param batchSize the number of messages
     */
    public void record(String topic, int batchSize) {
        countDown(recordsRemainingLatchesByTopic.get(topic), batchSize);
        // also record records for the "all topics" latch
        record(batchSize);
    }

    /**
     * Record a message commit from the task and the connector overall.
     */
    public void commit() {
        countDown(recordsToCommitLatch, 1);
    }

    /**
     * Record commit on a batch of messages from the task and the connector overall.
     *
     * @param batchSize the number of messages
     */
    public void commit(int batchSize) {
        countDown(recordsToCommitLatch, batchSize);
    }

    /**
     * Set the number of expected records for this task across any/all topics.
     *
     * @param expected number of records
     */
    public void expectedRecords(int expected) {
        expectedRecords(null, expected);
    }

    /**
     * Set the number of expected records for this task.
     *
     * @param topic    the name of the topic onto which the records are expected
     * @param expected number of records
     */
    public void expectedRecords(String topic, int expected) {
        if (topic != null) {
            recordsRemainingLatchesByTopic.put(topic, new Latch(expected));
        } else {
            recordsRemainingLatch = new Latch(expected);
        }
    }

    /**
     * Set the number of expected record commits performed by this task.
     *
     * @param expected number of commits
     */
    public void expectedCommits(int expected) {
        recordsToCommitLatch = new Latch(expected);
    }

    /**
     * Wait up to the specified number of milliseconds for this task to meet the expected number of
     * records as defined by {@code expectedRecords}.
     *
     * @param timeoutMillis number of milliseconds to wait for records
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(long timeoutMillis) throws InterruptedException {
        awaitRecords(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait up to the specified timeout for this task to meet the expected number of records as
     * defined by {@code expectedRecords}.
     *
     * @param timeout duration to wait for records
     * @param unit    the unit of duration; may not be null
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(long timeout, TimeUnit unit) throws InterruptedException {
        awaitRecords(recordsRemainingLatch, "records", timeout, unit);
    }

    /**
     * Wait up to the specified timeout for this task to meet the expected number of records as
     * defined by {@code expectedRecords}.
     *
     * @param topic   the name of the topic; may be null for all topics
     * @param timeout duration to wait for records
     * @param unit    the unit of duration; may not be null
     * @throws InterruptedException if another threads interrupts this one while waiting for records
     */
    public void awaitRecords(String topic, long timeout, TimeUnit unit) throws InterruptedException {
        Latch latch = recordsRemainingLatchesByTopic.get(topic);
        if (latch != null) {
            awaitRecords(latch, "records on topic '" + topic + "'", timeout, unit);
        } else {
            awaitRecords(recordsRemainingLatch, "records", timeout, unit);
        }
    }

    protected void awaitRecords(Latch latch, String type, long timeout, TimeUnit unit) throws InterruptedException {
        if (latch == null) {
            throw new IllegalStateException("Illegal state encountered. expectedRecords() was not set for this task?");
        }
        if (!latch.await(timeout, unit)) {
            String msg = String.format(
                    "Insufficient %s seen by %s in %d millis. Records expected=%d, actual=%d",
                    type,
                    name,
                    unit.toMillis(timeout),
                    latch.expected(),
                    latch.count());
            throw new DataException(msg);
        }
        log.debug("{} saw {} {}, expected {} records",
                name, latch.count(), type, latch.expected());
    }

    /**
     * Wait up to the specified timeout in milliseconds for this task to meet the expected number
     * of commits as defined by {@code expectedCommits}.
     *
     * @param timeoutMillis number of milliseconds to wait for commits
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(long timeoutMillis) throws InterruptedException {
        awaitCommits(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * Wait up to the specified timeout for this task to meet the expected number of commits as
     * defined by {@code expectedCommits}.
     *
     * @param timeout duration to wait for commits
     * @param unit    the unit of duration; may not be null
     * @throws InterruptedException if another threads interrupts this one while waiting for commits
     */
    public void awaitCommits(long timeout, TimeUnit unit) throws InterruptedException {
        if (recordsToCommitLatch == null) {
            throw new IllegalStateException("Illegal state encountered. expectedRecords() was not set for this task?");
        }
        if (!recordsToCommitLatch.await(timeout, unit)) {
            String msg = String.format(
                    "Insufficient records seen by %s in %d millis. Records expected=%d, actual=%d",
                    name,
                    unit.toMillis(timeout),
                    recordsToCommitLatch.expected(),
                    recordsToCommitLatch.count());
            throw new DataException(msg);
        }
        log.debug("{} saw {} records, expected {} records",
                name, recordsToCommitLatch.count(), recordsRemainingLatch.expected());
    }

    private void countDown(Latch latch, int count) {
        if (latch != null) {
            latch.countDown(count);
        } else {
            log.trace("No latch found due to no expectation");
        }
    }

    private static class Latch {
        protected final CountDownLatch latch;
        protected final int expected;

        public Latch(int expected) {
            this.latch = new CountDownLatch(expected);
            this.expected = expected;
        }

        public int count() {
            return expected - (int) latch.getCount();
        }

        public int expected() {
            return expected;
        }

        /**
         * Record a message arrival at the task and the connector overall.
         */
        public void countDown() {
            latch.countDown();
        }

        /**
         * Record arrival of a batch of messages at the task and the connector overall.
         *
         * @param batchSize the number of messages
         */
        public void countDown(int batchSize) {
            IntStream.range(0, batchSize).forEach(i -> latch.countDown());
        }

        /**
         * Wait up to the specified timeout for the current number to reach the expected number.
         *
         * @param timeout duration to wait
         * @param unit    the unit of duration; may not be null
         * @return true if the count has reached zero, or false otherwise
         * @throws InterruptedException if another threads interrupts this one while waiting for records
         */
        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return latch.await(timeout, unit);
        }
    }
}
