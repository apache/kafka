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
package org.apache.kafka.raft;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * This interface is used to send committed data from the {@link RaftClient}
 * down to registered {@link RaftClient.Listener} instances.
 *
 * The advantage of hiding the consumption of committed batches behind an interface
 * is that it allows us to push blocking operations such as reads from disk outside
 * of the Raft IO thread. This helps to ensure that a slow state machine will not
 * affect replication.
 *
 * @param <T> record type (see {@link org.apache.kafka.raft.RecordSerde})
 */
public interface BatchReader<T> extends Iterator<BatchReader.Batch<T>>, AutoCloseable {

    /**
     * Get the base offset of the readable batches. Note that this value is a constant
     * which is defined when the {@link BatchReader} instance is constructed. It does
     * not change based on reader progress.
     *
     * @return the base offset
     */
    long baseOffset();

    /**
     * Get the last offset of the batch if it is known. When reading from disk, we may
     * not know the last offset of a set of records until it has been read from disk.
     * In this case, the state machine cannot advance to the next committed data until
     * all batches from the {@link BatchReader} instance have been consumed.
     *
     * @return optional last offset
     */
    OptionalLong lastOffset();

    /**
     * Close this reader. It is the responsibility of the {@link RaftClient.Listener}
     * to close each reader passed to {@link RaftClient.Listener#handleCommit(BatchReader)}.
     */
    @Override
    void close();

    class Batch<T> {
        private final long baseOffset;
        private final int epoch;
        private final List<T> records;

        public Batch(long baseOffset, int epoch, List<T> records) {
            this.baseOffset = baseOffset;
            this.epoch = epoch;
            this.records = records;
        }

        public long lastOffset() {
            return baseOffset + records.size() - 1;
        }

        public long baseOffset() {
            return baseOffset;
        }

        public List<T> records() {
            return records;
        }

        public int epoch() {
            return epoch;
        }

        @Override
        public String toString() {
            return "Batch(" +
                "baseOffset=" + baseOffset +
                ", epoch=" + epoch +
                ", records=" + records +
                ')';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Batch<?> batch = (Batch<?>) o;
            return baseOffset == batch.baseOffset &&
                epoch == batch.epoch &&
                Objects.equals(records, batch.records);
        }

        @Override
        public int hashCode() {
            return Objects.hash(baseOffset, epoch, records);
        }
    }

}
