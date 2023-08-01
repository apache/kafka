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
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.ControlRecord;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV1;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_3_IV2;
import static org.apache.kafka.server.common.MetadataVersion.IBP_3_5_IV0;
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
        final CompletableFuture<Void> firstPublish = new CompletableFuture<>();
        private final String name;
        volatile MetadataDelta latestDelta = null;
        volatile MetadataImage latestImage = null;
        volatile LogDeltaManifest latestLogDeltaManifest = null;
        volatile SnapshotManifest latestSnapshotManifest = null;
        volatile boolean closed = false;

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
        public void onMetadataUpdate(
            MetadataDelta delta,
            MetadataImage newImage,
            LoaderManifest manifest
        ) {
            latestDelta = delta;
            latestImage = newImage;
            switch (manifest.type()) {
                case LOG_DELTA:
                    latestLogDeltaManifest = (LogDeltaManifest) manifest;
                    break;
                case SNAPSHOT:
                    latestSnapshotManifest = (SnapshotManifest) manifest;
                    break;
                default:
                    throw new RuntimeException("Invalid manifest type " + manifest.type());
            }
            firstPublish.complete(null);
        }

        @Override
        public void close() throws Exception {
            firstPublish.completeExceptionally(new RejectedExecutionException());
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
            List<Batch<ApiMessageAndVersion>> batches = lists
                .stream()
                .map(records -> Batch.data(0, 0, 0, 0, records))
                .collect(Collectors.toList());

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
            return provenance.snapshotId();
        }

        @Override
        public long lastContainedLogOffset() {
            return provenance.lastContainedOffset();
        }

        @Override
        public int lastContainedLogEpoch() {
            return provenance.lastContainedEpoch();
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
                    asList(
                        Batch.control(
                            200,
                            100,
                            4000,
                            10,
                            asList(new ControlRecord(ControlRecordType.SNAPSHOT_HEADER, new SnapshotHeaderRecord()))
                        )
                    )
                );
                loader.handleLoadSnapshot(snapshotReader);
                TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                    assertEquals(1L, loader.metrics().handleLoadSnapshotCount());
                });
            } else {
                TestUtils.retryOnExceptionWithTimeout(30_000, () -> {
                    assertEquals(0L, loader.metrics().handleLoadSnapshotCount());
                });
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
                setHighWaterMarkAccessor(() -> OptionalLong.of(1L)).
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
            loader.handleLoadSnapshot(snapshotReader);
            loader.waitForAllEventsToBeHandled();
            assertTrue(snapshotReader.closed);
            publishers.get(0).firstPublish.get(1, TimeUnit.MINUTES);
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
            publishers.get(0).firstPublish.get(10, TimeUnit.SECONDS);
            loadEmptySnapshot(loader, 200);
            assertEquals(200L, loader.lastAppliedOffset());
            loadEmptySnapshot(loader, 300);
            assertEquals(300L, loader.lastAppliedOffset());
            assertEquals(new SnapshotManifest(new MetadataProvenance(300, 100, 4000), 3000000L),
                publishers.get(0).latestSnapshotManifest);
            assertEquals(MetadataVersion.MINIMUM_KRAFT_VERSION,
                loader.metrics().currentMetadataVersion());
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
            asList(
                Batch.control(
                    200,
                    100,
                    4000,
                    10,
                    asList(new ControlRecord(ControlRecordType.SNAPSHOT_HEADER, new SnapshotHeaderRecord()))
                )
            )
        );
        if (loader.time() instanceof MockTime) {
            snapshotReader.setTime((MockTime) loader.time());
        }
        loader.handleLoadSnapshot(snapshotReader);
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
                setHighWaterMarkAccessor(() -> OptionalLong.of(1L)).
                build()) {
            loader.installPublishers(publishers).get();
            loadTestSnapshot(loader, 200);
            publishers.get(0).firstPublish.get(10, TimeUnit.SECONDS);
            MockBatchReader batchReader = new MockBatchReader(
                300,
                asList(
                    Batch.control(
                        300,
                        100,
                        4000,
                        10,
                        asList(new ControlRecord(ControlRecordType.SNAPSHOT_HEADER, new SnapshotHeaderRecord()))
                    )
                )
            ).setTime(time);
            loader.handleCommit(batchReader);
            loader.waitForAllEventsToBeHandled();
            assertTrue(batchReader.closed);
            assertEquals(300L, loader.lastAppliedOffset());
        }
        assertTrue(publishers.get(0).closed);
        assertEquals(new LogDeltaManifest(new MetadataProvenance(300, 100, 4000), LeaderAndEpoch.UNKNOWN, 1,
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
                setHighWaterMarkAccessor(() -> OptionalLong.of(1L)).
                build()) {
            loader.installPublishers(publishers).get();
            loader.handleLoadSnapshot(MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(200, 100, 4000), asList(
                    asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(IBP_3_3_IV1.featureLevel()), (short) 0)),
                    asList(new ApiMessageAndVersion(new TopicRecord().
                        setName("foo").
                        setTopicId(Uuid.fromString("Uum7sfhHQP-obSvfywmNUA")), (short) 0))
                )));
            for (MockPublisher publisher : publishers) {
                publisher.firstPublish.get(1, TimeUnit.MINUTES);
            }
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
    @Test
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
            assertFalse(publishers.get(0).firstPublish.isDone());

            // This still doesn't advance lastAppliedOffset since the high water mark at 221
            // is greater than our snapshot at 210.
            highWaterMark.set(OptionalLong.of(221));
            loadTestSnapshot(loader, 210);
            assertEquals(-1L, loader.lastAppliedOffset());

            // Loading a test snapshot at 220 allows us to leave catchUp state.
            loadTestSnapshot(loader, 220);
            assertEquals(220L, loader.lastAppliedOffset());
            publishers.get(0).firstPublish.get(1, TimeUnit.MINUTES);
        }
        faultHandler.maybeRethrowFirstException();
    }

    private void loadTestSnapshot(
        MetadataLoader loader,
        long offset
    ) throws Exception {
        loader.handleLoadSnapshot(MockSnapshotReader.fromRecordLists(
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

    private void loadTestSnapshot2(
        MetadataLoader loader,
        long offset
    ) throws Exception {
        loader.handleLoadSnapshot(MockSnapshotReader.fromRecordLists(
                new MetadataProvenance(offset, 100, 4000), asList(
                        asList(new ApiMessageAndVersion(new FeatureLevelRecord().
                                setName(MetadataVersion.FEATURE_NAME).
                                setFeatureLevel(IBP_3_3_IV2.featureLevel()), (short) 0)),
                        asList(new ApiMessageAndVersion(new TopicRecord().
                                setName("bar").
                                setTopicId(Uuid.fromString("VcL2Mw-cT4aL6XV9VujzoQ")), (short) 0))
                )));
        loader.waitForAllEventsToBeHandled();
    }

    /**
     * Test that loading a snapshot clears the previous state.
     */
    @Test
    public void testReloadSnapshot() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("testLastAppliedOffset");
        List<MockPublisher> publishers = asList(new MockPublisher("a"));
        try (MetadataLoader loader = new MetadataLoader.Builder().
                setFaultHandler(faultHandler).
                setHighWaterMarkAccessor(() -> OptionalLong.of(0)).
                build()) {
            loadTestSnapshot(loader, 100);
            loader.installPublishers(publishers).get();
            loader.waitForAllEventsToBeHandled();
            assertTrue(publishers.get(0).firstPublish.isDone());
            assertTrue(publishers.get(0).latestDelta.image().isEmpty());
            assertEquals(100L, publishers.get(0).latestImage.provenance().lastContainedOffset());

            loadTestSnapshot(loader, 200);
            assertEquals(200L, loader.lastAppliedOffset());
            assertEquals(IBP_3_3_IV1.featureLevel(),
                loader.metrics().currentMetadataVersion().featureLevel());
            assertFalse(publishers.get(0).latestDelta.image().isEmpty());

            loadTestSnapshot2(loader, 400);
            assertEquals(400L, loader.lastAppliedOffset());
            assertEquals(IBP_3_3_IV2.featureLevel(),
                loader.metrics().currentMetadataVersion().featureLevel());

            // Make sure the topic in the initial snapshot was overwritten by loading the new snapshot.
            assertFalse(publishers.get(0).latestImage.topics().topicsByName().containsKey("foo"));
            assertTrue(publishers.get(0).latestImage.topics().topicsByName().containsKey("bar"));

            loader.handleCommit(new MockBatchReader(500, asList(
                MockBatchReader.newBatch(500, 100, asList(
                    new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(IBP_3_5_IV0.featureLevel()), (short) 0))))));
            loader.waitForAllEventsToBeHandled();
            assertEquals(IBP_3_5_IV0.featureLevel(),
                loader.metrics().currentMetadataVersion().featureLevel());
        }
        faultHandler.maybeRethrowFirstException();
    }
}
