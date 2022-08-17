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

package org.apache.kafka.metadata.publisher;

import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.loader.LoadInformation;

import java.util.function.Consumer;


/**
 * Publishes metadata deltas which we have loaded from the log and snapshots.
 *
 * Publishers receive a stream of callbacks from the metadata loader which keeps them notified
 * of the latest cluster metadata. This interface abstracts away some of the complications of
 * following the cluster metadata. For example, if the loader needs to read a snapshot, it will
 * present the contents of the snapshot in the form of a delta from the previous state.
 */
public class SnapshotPublisher implements MetadataPublisher {
    public static class Builder {
        private Consumer<SnapshotCreationRequest> requestHandler = null;
        private long maxBytesBetweenSnapshots = Long.MAX_VALUE;

        public Builder setRequestHandler(Consumer<SnapshotCreationRequest> requestHandler) {
            this.requestHandler = requestHandler;
            return this;
        }

        public Builder setMaxBytesBetweenSnapshots(long maxBytesBetweenSnapshots) {
            this.maxBytesBetweenSnapshots = maxBytesBetweenSnapshots;
            return this;
        }

        public SnapshotPublisher build() {
            if (requestHandler == null) throw new RuntimeException("You must set the request handler.");
            return new SnapshotPublisher(requestHandler,
                    maxBytesBetweenSnapshots);
        }
    }

    @Override
    public String name() {
        return "SnapshotPublisher";
    }

    /**
     * The object which creates handles snapshot creation requests.
     */
    private final Consumer<SnapshotCreationRequest> requestHandler;

    /**
     * The maximum number of bytes of log messages we should allow before triggering a snapshot.
     */
    private final long maxBytesBetweenSnapshots;

    /**
     * The value that MetadataImage had when publish was last called.
     */
    private MetadataImage prevImage;

    /**
     * The value that lastContainedTimestamp had when publish was last called.
     */
    private long prevLastContainedTimestamp;

    /**
     * The number of bytes of log messages that we have seen since the last snapshot load or creation.
     */
    private long bytesSinceLastSnapshot;

    SnapshotPublisher(
            Consumer<SnapshotCreationRequest> requestHandler,
        long maxBytesBetweenSnapshots
    ) {
        this.requestHandler = requestHandler;
        this.maxBytesBetweenSnapshots = maxBytesBetweenSnapshots;
        this.prevImage = MetadataImage.EMPTY;
        this.prevLastContainedTimestamp = 0L;
        clearStats();
    }

    @Override
    public void publish(
        MetadataDelta delta,
        MetadataImage newImage,
        LoadInformation info
    ) {
        if (info.fromSnapshot()) {
            // If we loaded a snapshot, we never want to create a snapshot in response, since
            // that would merely duplicate what we just loaded. We also want to reset the clock
            // on when we should create the next snapshot.
            clearStats();
        } else if (delta.featuresDelta() != null && delta.featuresDelta().metadataVersionChange().isPresent()) {
            // If we are reading the log and the MetadataVersion changed, we must snapshot immediately.
            // Except in the trivial case where the previous image was literally empty.
            if (!prevImage.isEmpty()) {
                requestHandler.accept(new SnapshotCreationRequest(prevLastContainedTimestamp, prevImage, true));
            }
            clearStats();
        } else {
            // Check if the statistics indicate that we should snapshot.
            updateStats(info);
            if (shouldSnapshotBasedOnStats()) {
                requestHandler.accept(new SnapshotCreationRequest(info.lastContainedTimestamp(), newImage, false));
                clearStats();
            }
        }
        this.prevLastContainedTimestamp = info.lastContainedTimestamp();
    }

    void updateStats(LoadInformation info) {
        bytesSinceLastSnapshot += info.numBytes();
    }

    boolean shouldSnapshotBasedOnStats() {
        if (bytesSinceLastSnapshot >= maxBytesBetweenSnapshots) return true;
        return false;
    }

    void clearStats() {
        bytesSinceLastSnapshot = 0L;
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }
}
