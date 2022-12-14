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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.server.fault.FaultHandlerException;
import org.apache.kafka.server.fault.MockFaultHandler;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@Timeout(value = 40)
public class SnapshotGeneratorTest {
    static class MockEmitter implements SnapshotGenerator.Emitter {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final List<MetadataImage> images = new ArrayList<>();
        private RuntimeException problem = null;

        MockEmitter setReady() {
            latch.countDown();
            return this;
        }

        synchronized MockEmitter setProblem(RuntimeException problem) {
            this.problem = problem;
            return this;
        }

        @Override
        public synchronized void maybeEmit(MetadataImage image) {
            RuntimeException currentProblem = problem;
            if (currentProblem != null) {
                throw currentProblem;
            }
            try {
                latch.await();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
            images.add(image);
        }

        synchronized List<MetadataImage> images() {
            return new ArrayList<>(images);
        }
    }

    private final static MetadataDelta TEST_DELTA;

    static {
        TEST_DELTA = new MetadataDelta.Builder().
                setImage(MetadataImage.EMPTY).
                build();
        TEST_DELTA.replay(RecordTestUtils.testRecord(0).message());
    }

    private final static MetadataImage TEST_IMAGE = TEST_DELTA.apply(MetadataProvenance.EMPTY);

    @Test
    public void testCreateSnapshot() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("SnapshotGenerator");
        MockEmitter emitter = new MockEmitter();
        try (SnapshotGenerator generator = new SnapshotGenerator.Builder(emitter).
                setFaultHandler(faultHandler).
                setMaxBytesSinceLastSnapshot(200).
                setMaxTimeSinceLastSnapshotNs(TimeUnit.DAYS.toNanos(10)).
                build()) {
            // Publish a log delta batch. This one will not trigger a snapshot yet.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 100));
            // Publish a log delta batch. This will trigger a snapshot.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 100));
            // Publish a log delta batch. This one will be ignored because there are other images
            // queued for writing.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 2000));
            assertEquals(Collections.emptyList(), emitter.images());
            emitter.setReady();
        }
        assertEquals(Arrays.asList(TEST_IMAGE), emitter.images());
        faultHandler.maybeRethrowFirstException();
    }

    @Test
    public void testSnapshotsDisabled() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("SnapshotGenerator");
        MockEmitter emitter = new MockEmitter().setReady();
        AtomicReference<String> disabledReason = new AtomicReference<>();
        try (SnapshotGenerator generator = new SnapshotGenerator.Builder(emitter).
                setFaultHandler(faultHandler).
                setMaxBytesSinceLastSnapshot(1).
                setMaxTimeSinceLastSnapshotNs(0).
                setDisabledReason(disabledReason).
                build()) {
            disabledReason.compareAndSet(null, "we are testing disable()");
            // No snapshots are generated because snapshots are disabled.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 100));
        }
        assertEquals(Collections.emptyList(), emitter.images());
        faultHandler.maybeRethrowFirstException();
    }

    @Test
    public void testTimeBasedSnapshots() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("SnapshotGenerator");
        MockEmitter emitter = new MockEmitter().setReady();
        MockTime mockTime = new MockTime();
        try (SnapshotGenerator generator = new SnapshotGenerator.Builder(emitter).
                setTime(mockTime).
                setFaultHandler(faultHandler).
                setMaxBytesSinceLastSnapshot(200).
                setMaxTimeSinceLastSnapshotNs(TimeUnit.MINUTES.toNanos(30)).
                build()) {
            // This image isn't published yet.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 50));
            assertEquals(Collections.emptyList(), emitter.images());
            mockTime.sleep(TimeUnit.MINUTES.toNanos(40));
            // Next image is published because of the time delay.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 50));
            TestUtils.waitForCondition(() -> emitter.images().size() == 1, "images.size == 1");
            // bytesSinceLastSnapshot was reset to 0 by the previous snapshot,
            // so this does not trigger a new snapshot.
            generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                    new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 100, 150));
        }
        assertEquals(Arrays.asList(TEST_IMAGE), emitter.images());
        faultHandler.maybeRethrowFirstException();
    }

    @Test
    public void testEmitterProblem() throws Exception {
        MockFaultHandler faultHandler = new MockFaultHandler("SnapshotGenerator");
        MockEmitter emitter = new MockEmitter().setProblem(new RuntimeException("oops"));
        try (SnapshotGenerator generator = new SnapshotGenerator.Builder(emitter).
                setFaultHandler(faultHandler).
                setMaxBytesSinceLastSnapshot(200).
                build()) {
            for (int i = 0; i < 2; i++) {
                generator.publishLogDelta(TEST_DELTA, TEST_IMAGE,
                        new LogDeltaManifest(MetadataProvenance.EMPTY, 1, 10000, 50000));
            }
        }
        assertEquals(Collections.emptyList(), emitter.images());
        assertNotNull(faultHandler.firstException());
        assertEquals(FaultHandlerException.class, faultHandler.firstException().getClass());
        assertEquals("SnapshotGenerator: KRaft snapshot file generation error: oops",
                faultHandler.firstException().getMessage());
    }
}
