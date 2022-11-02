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

package org.apache.kafka.image.loader;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.snapshot.SnapshotReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class MetadataLoaderTest {
    @Test
    public void testCreateAndClose() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testCreateAndClose");
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                build()) {
            assertEquals(-1L, loader.lastAppliedOffset());
        }
        faultHandler.maybeRethrowFirstException();
    }

    static class MockPublisher implements MetadataPublisher {
        MetadataDelta latestDelta = null;
        MetadataImage latestImage = null;
        LogDeltaManifest latestLogDeltaManifest = null;
        SnapshotManifest latestSnapshotManifest = null;
        MetadataDelta latestPreVersionChangeDelta = null;
        MetadataImage latestPreVersionChangeImage = null;
        PreVersionChangeManifest latestPreVersionChangeManifest = null;
        boolean closed = false;

        @Override
        public String name() {
            return "MockPublisher";
        }

        @Override
        public void publishSnapshot(
            MetadataDelta delta,
            MetadataImage newImage,
            SnapshotManifest manifest
        ) {
            latestDelta = delta;
            latestImage = newImage;
            latestSnapshotManifest = manifest;
        }

        @Override
        public void publishPreVersionChangeImage(
            MetadataDelta delta,
            MetadataImage preVersionChangeImage,
            PreVersionChangeManifest manifest
        ) {
            latestPreVersionChangeDelta = delta;
            latestPreVersionChangeImage = preVersionChangeImage;
            latestPreVersionChangeManifest = manifest;
        }

        @Override
        public void publishLogDelta(
            MetadataDelta delta,
            MetadataImage newImage,
            LogDeltaManifest manifest
        ) {
            latestDelta = delta;
            latestImage = newImage;
            latestLogDeltaManifest = manifest;
        }

        @Override
        public void close() throws Exception {
            closed = true;
        }
    }

    /**
     * Install 2 publishers and check that the publishers that were installed are closed when the
     * loader is closed.
     */
    @Test
    public void testInstallPublishers() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testInstallPublishers");
        List<MockPublisher> publishers = asList(new MockPublisher(),
                new MockPublisher(),
                new MockPublisher());
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                build()) {
            loader.installPublishers(publishers.subList(0, 2)).get();
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(MetadataImage.EMPTY, publishers.get(0).latestImage);
        assertTrue(publishers.get(1).closed);
        assertEquals(MetadataImage.EMPTY, publishers.get(1).latestImage);
        assertFalse(publishers.get(2).closed);
        assertNull(publishers.get(2).latestImage);
        faultHandler.maybeRethrowFirstException();
    }

    /**
     * Test that a publisher cannot be installed more than once.
     */
    @Test
    public void testPublisherCannotBeInstalledMoreThanOnce() throws Exception {
        MockFaultHandler faultHandler =
                new MockFaultHandler("testPublisherCannotBeInstalledMoreThanOnce");
        MockPublisher publisher = new MockPublisher();
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                build()) {
            loader.installPublishers(asList(publisher)).get();
            assertEquals("testPublisherCannotBeInstalledMoreThanOnce: Attempted to install " +
                "publisher MockPublisher, which is already installed.",
                    assertThrows(ExecutionException.class,
                        () -> loader.installPublishers(asList(publisher)).get()).
                            getCause().getMessage());
        }
    }

    static class MockSnapshotReader implements SnapshotReader<ApiMessageAndVersion> {
        private final MetadataProvenance provenance;
        private final Iterator<Batch<ApiMessageAndVersion>> iterator;
        private MockTime time = null;
        boolean closed = false;

        static MockSnapshotReader fromRecordLists(
            MetadataProvenance provenance,
            List<List<ApiMessageAndVersion>> lists
        ) {
            List<Batch<ApiMessageAndVersion>> batches = new ArrayList<>();
            lists.forEach(records -> batches.add(Batch.data(
                provenance.offset(),
                provenance.epoch(),
                provenance.lastContainedLogTimeMs(),
                0,
                records)));
            return new MockSnapshotReader(provenance, batches);
        }

        MockSnapshotReader(
            MetadataProvenance provenance,
            List<Batch<ApiMessageAndVersion>> batches
        ) {
            this.provenance = provenance;
            this.iterator = batches.iterator();
        }

        MockSnapshotReader setTime(MockTime time) {
            this.time = time;
            return this;
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return provenance.offsetAndEpoch();
        }

        @Override
        public long lastContainedLogOffset() {
            return provenance.offset();
        }

        @Override
        public int lastContainedLogEpoch() {
            return provenance.epoch();
        }

        @Override
        public long lastContainedLogTimestamp() {
            return provenance.lastContainedLogTimeMs();
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean hasNext() {
            if (time != null) time.sleep(1);
            return iterator.hasNext();
        }

        @Override
        public Batch<ApiMessageAndVersion> next() {
            if (time != null) time.sleep(1);
            return iterator.next();
        }
    }

    /**
     * Install 2 publishers and remove one.
     */
    @Test
    public void testRemovePublisher() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testRemovePublisher");
        List<MockPublisher> publishers = asList(new MockPublisher(),
                new MockPublisher(),
                new MockPublisher());
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                build()) {
            loader.installPublishers(publishers.subList(0, 2)).get();
            loader.removeAndClosePublisher(publishers.get(1)).get();
            MockSnapshotReader snapshotReader = MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(100, 50, 2000),
                asList(asList(new ApiMessageAndVersion(
                    new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(MetadataVersion.IBP_3_3_IV2.featureLevel()), (short) 0))));
            assertFalse(snapshotReader.closed);
            loader.handleSnapshot(snapshotReader);
            loader.waitForAllEventsToBeHandled();
            assertTrue(snapshotReader.closed);
            loader.removeAndClosePublisher(publishers.get(0)).get();
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(MetadataVersion.IBP_3_3_IV2,
                publishers.get(0).latestImage.features().metadataVersion());
        assertTrue(publishers.get(1).closed);
        assertEquals(MetadataImage.EMPTY, publishers.get(1).latestImage);
        assertFalse(publishers.get(2).closed);
        assertNull(publishers.get(2).latestImage);
        faultHandler.maybeRethrowFirstException();
    }

    /**
     * Test loading a snapshot with 0 records.
     */
    @Test
    public void testLoadEmptySnapshot() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testLoadEmptySnapshot");
        MockTime time = new MockTime();
        List<MockPublisher> publishers = asList(new MockPublisher());
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setTime(time).
                build()) {
            loader.installPublishers(publishers).get();
            MockSnapshotReader snapshotReader = new MockSnapshotReader(
                new MetadataProvenance(200, 100, 4000),
                    asList(Batch.control(200, 100, 4000, 10, 200))).
                        setTime(time);
            loader.handleSnapshot(snapshotReader);
            loader.waitForAllEventsToBeHandled();
            assertEquals(200L, loader.lastAppliedOffset());
            assertEquals(new SnapshotManifest(new MetadataProvenance(200, 100, 4000), 3000000L),
                publishers.get(0).latestSnapshotManifest);
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(MetadataVersion.IBP_3_0_IV1,
                publishers.get(0).latestImage.features().metadataVersion());
        assertTrue(publishers.get(0).latestImage.isEmpty());
        faultHandler.maybeRethrowFirstException();
    }

    static class MockBatchReader implements BatchReader<ApiMessageAndVersion> {
        private final long baseOffset;
        private final Iterator<Batch<ApiMessageAndVersion>> iterator;
        private boolean closed = false;
        private MockTime time = null;

        static Batch<ApiMessageAndVersion> newBatch(
            long batchBaseOffset,
            int epoch,
            List<ApiMessageAndVersion> records
        ) {
            return Batch.data(batchBaseOffset, epoch, 0, 0, records);
        }

        MockBatchReader(
            long baseOffset,
            List<Batch<ApiMessageAndVersion>> batches
        ) {
            this.baseOffset = baseOffset;
            this.iterator = batches.iterator();
        }

        private MockBatchReader setTime(MockTime time) {
            this.time = time;
            return this;
        }

        @Override
        public long baseOffset() {
            return baseOffset;
        }

        @Override
        public OptionalLong lastOffset() {
            return OptionalLong.empty();
        }

        @Override
        public void close() {
            this.closed = true;
        }

        @Override
        public boolean hasNext() {
            if (time != null) time.sleep(1);
            return iterator.hasNext();
        }

        @Override
        public Batch<ApiMessageAndVersion> next() {
            if (time != null) time.sleep(1);
            return iterator.next();
        }
    }

    /**
     * Test loading a batch with 0 records.
     */
    @Test
    public void testLoadEmptyBatch() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testLoadEmptyBatch");
        MockTime time = new MockTime();
        List<MockPublisher> publishers = asList(new MockPublisher());
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setTime(time).
                build()) {
            loader.installPublishers(publishers).get();
            MockBatchReader batchReader = new MockBatchReader(300, asList(
                Batch.control(300, 100, 4000, 10, 400))).
                    setTime(time);
            loader.handleCommit(batchReader);
            loader.waitForAllEventsToBeHandled();
            assertTrue(batchReader.closed);
            assertEquals(400L, loader.lastAppliedOffset());
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(new LogDeltaManifest(new MetadataProvenance(400, 100, 4000), 1, 3000000L, 10),
            publishers.get(0).latestLogDeltaManifest);
        assertEquals(MetadataVersion.IBP_3_0_IV1,
            publishers.get(0).latestImage.features().metadataVersion());
        assertTrue(publishers.get(0).latestImage.isEmpty());
        faultHandler.maybeRethrowFirstException();
    }

    /**
     * Test that the lastAppliedOffset moves forward as expected.
     */
    @Test
    public void testLastAppliedOffset() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testRemovePublisher");
        List<MockPublisher> publishers = asList(new MockPublisher(),
                new MockPublisher());
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                build()) {
            loader.installPublishers(publishers).get();
            loader.handleSnapshot(MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(200, 100, 4000), asList(
                    asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(MetadataVersion.IBP_3_3_IV1.featureLevel()), (short) 0)),
                    asList(new ApiMessageAndVersion(new TopicRecord().
                        setName("foo").
                        setTopicId(Uuid.fromString("Uum7sfhHQP-obSvfywmNUA")), (short) 0))
                )));
            loader.waitForAllEventsToBeHandled();
            assertEquals(200L, loader.lastAppliedOffset());
            loader.handleCommit(new MockBatchReader(201, asList(
                MockBatchReader.newBatch(201, 100, asList(
                    new ApiMessageAndVersion(new RemoveTopicRecord().
                        setTopicId(Uuid.fromString("Uum7sfhHQP-obSvfywmNUA")), (short) 0))))));
            loader.waitForAllEventsToBeHandled();
            assertEquals(201L, loader.lastAppliedOffset());
        }
        for (int i = 0; i < 2; i++) {
            assertTrue(publishers.get(i).closed);
            assertTrue(publishers.get(i).closed);
            assertEquals(MetadataVersion.IBP_3_3_IV1,
                    publishers.get(i).latestImage.features().metadataVersion());
        }
        faultHandler.maybeRethrowFirstException();
    }
}
