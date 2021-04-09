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

package org.apache.kafka.controller;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;

import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.metadata.ApiMessageAndVersion;
import org.slf4j.Logger;


final class SnapshotGenerator {
    static class Section {
        private final String name;
        private final Iterator<List<ApiMessageAndVersion>> iterator;

        Section(String name, Iterator<List<ApiMessageAndVersion>> iterator) {
            this.name = name;
            this.iterator = iterator;
        }

        String name() {
            return name;
        }

        Iterator<List<ApiMessageAndVersion>> iterator() {
            return iterator;
        }
    }

    private final Logger log;
    private final SnapshotWriter writer;
    private final int maxBatchesPerGenerateCall;
    private final ExponentialBackoff exponentialBackoff;
    private final List<Section> sections;
    private final Iterator<Section> sectionIterator;
    private Iterator<List<ApiMessageAndVersion>> batchIterator;
    private List<ApiMessageAndVersion> batch;
    private Section section;
    private long numRecords;
    private long numWriteTries;

    SnapshotGenerator(LogContext logContext,
                      SnapshotWriter writer,
                      int maxBatchesPerGenerateCall,
                      ExponentialBackoff exponentialBackoff,
                      List<Section> sections) {
        this.log = logContext.logger(SnapshotGenerator.class);
        this.writer = writer;
        this.maxBatchesPerGenerateCall = maxBatchesPerGenerateCall;
        this.exponentialBackoff = exponentialBackoff;
        this.sections = sections;
        this.sectionIterator = this.sections.iterator();
        this.batchIterator = Collections.emptyIterator();
        this.batch = null;
        this.section = null;
        this.numRecords = 0;
        this.numWriteTries = 0;
    }

    /**
     * Returns the epoch of the snapshot that we are generating.
     */
    long epoch() {
        return writer.epoch();
    }

    SnapshotWriter writer() {
        return writer;
    }

    /**
     * Generate the next batch of records.
     *
     * @return  0 if a batch was sent to the writer,
     *          -1 if there are no more batches to generate,
     *          or the number of times we tried to write and the writer
     *          was busy, otherwise.
     */
    private long generateBatch() throws Exception {
        if (batch == null) {
            while (!batchIterator.hasNext()) {
                if (section != null) {
                    log.info("Generated {} record(s) for the {} section of snapshot {}.",
                             numRecords, section.name(), writer.epoch());
                    section = null;
                    numRecords = 0;
                }
                if (!sectionIterator.hasNext()) {
                    writer.completeSnapshot();
                    return -1;
                }
                section = sectionIterator.next();
                log.info("Generating records for the {} section of snapshot {}.",
                         section.name(), writer.epoch());
                batchIterator = section.iterator();
            }
            batch = batchIterator.next();
        }
        if (writer.writeBatch(batch)) {
            numRecords += batch.size();
            numWriteTries = 0;
            batch = null;
            return 0;
        } else {
            return ++numWriteTries;
        }
    }

    /**
     * Generate the next few batches of records.
     *
     * @return  The number of nanoseconds to delay before rescheduling the
     *          generateBatches event, or empty if the snapshot is done.
     */
    OptionalLong generateBatches() throws Exception {
        for (int numBatches = 0; numBatches < maxBatchesPerGenerateCall; numBatches++) {
            long result = generateBatch();
            if (result < 0) {
                return OptionalLong.empty();
            } else if (result > 0) {
                return OptionalLong.of(exponentialBackoff.backoff(result - 1));
            }
        }
        return OptionalLong.of(0);
    }
}
