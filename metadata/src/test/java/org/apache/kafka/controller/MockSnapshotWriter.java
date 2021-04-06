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

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.metadata.ApiMessageAndVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


class MockSnapshotWriter implements SnapshotWriter {
    private final long epoch;
    private boolean ready = true;
    private boolean completed = false;
    private boolean closed = false;
    private final List<List<ApiMessageAndVersion>> batches = new ArrayList<>();

    public MockSnapshotWriter(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public synchronized boolean writeBatch(List<ApiMessageAndVersion> batch) throws IOException {
        if (completed) throw new RuntimeException("writer has been completed");
        if (closed) throw new RuntimeException("writer is closed");
        if (!ready) return false;
        batches.add(batch);
        return true;
    }

    synchronized void setReady(boolean ready) {
        this.ready = ready;
    }

    @Override
    public synchronized void close() {
        this.closed = true;
    }

    @Override
    public synchronized void completeSnapshot() throws IOException {
        if (closed) throw new RuntimeException("writer is closed");
        this.notifyAll();
        this.completed = true;
    }

    synchronized void waitForCompletion() throws InterruptedException {
        while (!completed) {
            this.wait();
        }
    }

    synchronized boolean completed() {
        return completed;
    }

    synchronized List<List<ApiMessageAndVersion>> batches() {
        return batches;
    }

    public MockSnapshotReader toReader() {
        List<List<ApiMessage>> readerBatches = new ArrayList<>();
        for (List<ApiMessageAndVersion> batch : batches) {
            List<ApiMessage> readerBatch = new ArrayList<>();
            for (ApiMessageAndVersion messageAndVersion : batch) {
                readerBatch.add(messageAndVersion.message());
            }
            readerBatches.add(readerBatch);
        }
        return new MockSnapshotReader(epoch, readerBatches.iterator());
    }
}
