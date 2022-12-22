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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV1;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV2;
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
                setHighWaterMarkAccessor(() -> OptionalLong.empty()).
                build()) {
            assertEquals(-1L, loader.lastAppliedOffset());
        }
        faultHandler.maybeRethrowFirstException();
    }

    static class MockPublisher implements MetadataPublisher {
        private final String name;
        MetadataDelta latestDelta = null;
        MetadataImage latestImage = null;
        LogDeltaManifest latestLogDeltaManifest = null;
        SnapshotManifest latestSnapshotManifest = null;
        boolean closed = false;

        MockPublisher() {
            this("MockPublisher");
        }

        MockPublisher(String name) {
            this.name = name;
        }

        @Override
        public String name() {
            return name;
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
        List<MockPublisher> publishers = asList(new MockPublisher("a"),
                new MockPublisher("b"),
                new MockPublisher("c"));
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setHighWaterMarkAccessor(() -> OptionalLong.empty()).
                build()) {
            loader.installPublishers(publishers.subList(0, 2)).get();
        }
        assertTrue(publishers.get(0).closed);
        assertNull(publishers.get(0).latestImage);
        assertTrue(publishers.get(1).closed);
        assertNull(publishers.get(1).latestImage);
        assertFalse(publishers.get(2).closed);
        assertNull(publishers.get(2).latestImage);
        faultHandler.maybeRethrowFirstException();
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
     * Test that a publisher cannot be installed more than once.
     */
    @ParameterizedTest
    @CsvSource(value = {"false,false", "false,true", "true,false", "true,true"})
    public void testPublisherCannotBeInstalledMoreThanOnce(
        boolean loadSnapshot,
        boolean sameObject
    ) throws Exception {
        MockFaultHandler faultHandler =
                new MockFaultHandler("testPublisherCannotBeInstalledMoreThanOnce");
        MockPublisher publisher = new MockPublisher();
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setHighWaterMarkAccessor(() -> OptionalLong.of(0L)).
                build()) {
            loader.installPublishers(asList(publisher)).get();
            if (loadSnapshot) {
                MockSnapshotReader snapshotReader = new MockSnapshotReader(
                        new MetadataProvenance(200, 100, 4000),
                        asList(Batch.control(200, 100, 4000, 10, 200)));
                loader.handleSnapshot(snapshotReader);
            }
            loader.waitForAllEventsToBeHandled();
            if (sameObject) {
                assertEquals("testPublisherCannotBeInstalledMoreThanOnce: Attempted to install " +
                    "publisher MockPublisher, which is already installed.",
                        assertThrows(ExecutionException.class,
                                () -> loader.installPublishers(asList(publisher)).get()).
                                getCause().getMessage());
            } else {
                assertEquals("testPublisherCannotBeInstalledMoreThanOnce: Attempted to install " +
                    "a new publisher named MockPublisher, but there is already a publisher with that name.",
                        assertThrows(ExecutionException.class,
                                () -> loader.installPublishers(asList(new MockPublisher())).get()).
                                getCause().getMessage());
            }
        }
    }

    /**
     * Install 2 publishers and remove one.
     */
    @Test
    public void testRemovePublisher() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testRemovePublisher");
        List<MockPublisher> publishers = asList(new MockPublisher("a"),
                new MockPublisher("b"),
                new MockPublisher("c"));
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setHighWaterMarkAccessor(() -> OptionalLong.of(0L)).
                build()) {
            loader.installPublishers(publishers.subList(0, 2)).get();
            loader.removeAndClosePublisher(publishers.get(1)).get();
            MockSnapshotReader snapshotReader = MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(100, 50, 2000),
                asList(asList(new ApiMessageAndVersion(
                    new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(IBP_3_3_IV2.featureLevel()), (short) 0))));
            assertFalse(snapshotReader.closed);
            loader.handleSnapshot(snapshotReader);
            loader.waitForAllEventsToBeHandled();
            assertTrue(snapshotReader.closed);
            loader.removeAndClosePublisher(publishers.get(0)).get();
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(IBP_3_3_IV2,
                publishers.get(0).latestImage.features().metadataVersion());
        assertTrue(publishers.get(1).closed);
        assertNull(publishers.get(1).latestImage);
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
                setHighWaterMarkAccessor(() -> OptionalLong.of(0L)).
                build()) {
            loader.installPublishers(publishers).get();
            loadEmptySnapshot(loader, 200);
            assertEquals(200L, loader.lastAppliedOffset());
            loadEmptySnapshot(loader, 300);
            assertEquals(300L, loader.lastAppliedOffset());
            assertEquals(new SnapshotManifest(new MetadataProvenance(300, 100, 4000), 3000000L),
                publishers.get(0).latestSnapshotManifest);
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(MetadataVersion.IBP_3_0_IV1,
                publishers.get(0).latestImage.features().metadataVersion());
        assertTrue(publishers.get(0).latestImage.isEmpty());
        faultHandler.maybeRethrowFirstException();
    }

    private void loadEmptySnapshot(
        MetadataLoader loader,
        long offset
    ) throws Exception {
        MockSnapshotReader snapshotReader = new MockSnapshotReader(
                new MetadataProvenance(offset, 100, 4000),
                asList(Batch.control(200, 100, 4000, 10, 200)));
        if (loader.time() instanceof MockTime) {
            snapshotReader.setTime((MockTime) loader.time());
        }
        loader.handleSnapshot(snapshotReader);
        loader.waitForAllEventsToBeHandled();
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
                setHighWaterMarkAccessor(() -> OptionalLong.of(0L)).
                build()) {
            loader.installPublishers(publishers).get();
            loadTestSnapshot(loader, 200);
            MockBatchReader batchReader = new MockBatchReader(300, asList(
                Batch.control(300, 100, 4000, 10, 400))).
                    setTime(time);
            loader.handleCommit(batchReader);
            loader.waitForAllEventsToBeHandled();
            assertTrue(batchReader.closed);
            assertEquals(400L, loader.lastAppliedOffset());
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(new LogDeltaManifest(new MetadataProvenance(400, 100, 4000), 1,
                        3000000L, 10),
            publishers.get(0).latestLogDeltaManifest);
        assertEquals(MetadataVersion.IBP_3_3_IV1,
            publishers.get(0).latestImage.features().metadataVersion());
        faultHandler.maybeRethrowFirstException();
    }

    /**
     * Test that the lastAppliedOffset moves forward as expected.
     */
    @Test
    public void testLastAppliedOffset() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testLastAppliedOffset");
        List<MockPublisher> publishers = asList(new MockPublisher("a"),
                new MockPublisher("b"));
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setHighWaterMarkAccessor(() -> OptionalLong.of(0L)).
                build()) {
            loader.installPublishers(publishers).get();
            loader.handleSnapshot(MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(200, 100, 4000), asList(
                    asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(IBP_3_3_IV1.featureLevel()), (short) 0)),
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
            assertEquals(IBP_3_3_IV1,
                    publishers.get(i).latestImage.features().metadataVersion());
        }
        faultHandler.maybeRethrowFirstException();
    }

    /**
     * Test that we do not leave the catchingUp state state until we have loaded up to the high
     * water mark.
     */
    public void testCatchingUpState() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testLastAppliedOffset");
        List<MockPublisher> publishers = asList(new MockPublisher("a"),
                new MockPublisher("b"));
        AtomicReference<OptionalLong> highWaterMark = new AtomicReference<>(OptionalLong.empty());
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setHighWaterMarkAccessor(() -> highWaterMark.get()).
                build()) {
            loader.installPublishers(publishers).get();
            loadTestSnapshot(loader, 200);

            // We don't update lastAppliedOffset because we're still in catchingUp state due to
            // highWaterMark being OptionalLong.empty (aka unknown).
            assertEquals(-1L, loader.lastAppliedOffset());

            // Setting the high water mark here doesn't do anything because we only check it when
            // we're publishing an update. This is OK because we know that we'll get updates
            // frequently. If there is no other activity, there will at least be NoOpRecords.
            highWaterMark.set(OptionalLong.of(0));
            assertEquals(-1L, loader.lastAppliedOffset());

            // This still doesn't advance lastAppliedOffset since the high water mark at 220
            // is greater than our snapshot at 210.
            highWaterMark.set(OptionalLong.of(220));
            loadTestSnapshot(loader, 210);
            assertEquals(-1L, loader.lastAppliedOffset());

            // Loading a test snapshot at 220 allows us to leave catchUp state.
            loadTestSnapshot(loader, 220);
            assertEquals(220L, loader.lastAppliedOffset());
        }
        faultHandler.maybeRethrowFirstException();
    }

    private void loadTestSnapshot(
        MetadataLoader loader,
        long offset
    ) throws Exception {
        loader.handleSnapshot(MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(offset, 100, 4000), asList(
                        asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                                setName(MetadataVersion.FEATURE_NAME).
                                setFeatureLevel(IBP_3_3_IV1.featureLevel()), (short) 0)),
                        asList(new ApiMessageAndVersion(new TopicRecord().
                                setName("foo").
                                setTopicId(Uuid.fromString("Uum7sfhHQP-obSvfywmNUA")), (short) 0))
                )));
        loader.waitForAllEventsToBeHandled();
    }
}
