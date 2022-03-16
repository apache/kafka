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

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.snapshot.SnapshotWriter;
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
    private final SnapshotWriter<ApiMessageAndVersion> writer;
    private final int maxBatchesPerGenerateCall;
    private final List<Section> sections;
    private final Iterator<Section> sectionIterator;
    private Iterator<List<ApiMessageAndVersion>> batchIterator;
    private List<ApiMessageAndVersion> batch;
    private Section section;
    private long numRecords;

    SnapshotGenerator(LogContext logContext,
                      SnapshotWriter<ApiMessageAndVersion> writer,
                      int maxBatchesPerGenerateCall,
                      List<Section> sections) {
        this.log = logContext.logger(SnapshotGenerator.class);
        this.writer = writer;
        this.maxBatchesPerGenerateCall = maxBatchesPerGenerateCall;
        this.sections = sections;
        this.sectionIterator = this.sections.iterator();
        this.batchIterator = Collections.emptyIterator();
        this.batch = null;
        this.section = null;
        this.numRecords = 0;
    }

    /**
     * Returns the last offset from the log that will be included in the snapshot.
     */
    long lastContainedLogOffset() {
        return writer.lastContainedLogOffset();
    }

    SnapshotWriter writer() {
        return writer;
    }

    /**
     * Generate and write the next batch of records.
     *
     * @return true if the last batch was generated, otherwise false
     */
    private boolean generateBatch() throws Exception {
        if (batch == null) {
            while (!batchIterator.hasNext()) {
                if (section != null) {
                    log.info("Generated {} record(s) for the {} section of snapshot {}.",
                             numRecords, section.name(), writer.snapshotId());
                    section = null;
                    numRecords = 0;
                }
                if (!sectionIterator.hasNext()) {
                    writer.freeze();
                    return true;
                }
                section = sectionIterator.next();
                log.info("Generating records for the {} section of snapshot {}.",
                         section.name(), writer.snapshotId());
                batchIterator = section.iterator();
            }
            batch = batchIterator.next();
        }

        writer.append(batch);
        numRecords += batch.size();
        batch = null;
        return false;
    }

    /**
     * Generate the next few batches of records.
     *
     * @return  The number of nanoseconds to delay before rescheduling the
     *          generateBatches event, or empty if the snapshot is done.
     */
    OptionalLong generateBatches() throws Exception {
        for (int numBatches = 0; numBatches < maxBatchesPerGenerateCall; numBatches++) {
            if (generateBatch()) {
                return OptionalLong.empty();
            }
        }
        return OptionalLong.of(0);
    }
}
