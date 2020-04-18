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
package org.apache.kafka.rsm.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import kafka.log.LogSegment;
import kafka.log.LogUtils;
import kafka.utils.TestUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.log.remote.storage.LogSegmentData;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentContext;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.common.log.remote.storage.RemoteStorageException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

public class S3RemoteStorageManagerTest {
    private static final Logger log = LoggerFactory.getLogger(S3RemoteStorageManagerTest.class);

    static final String TOPIC = "connect-log";
    static final TopicPartition TP0 = new TopicPartition(TOPIC, 0);
    static final TopicPartition TP1 = new TopicPartition(TOPIC, 1);

    File logDir;

    private List<LogSegment> createdSegments;

    @ClassRule
    public static LocalStackContainer localstack = new LocalStackContainer().withServices(S3);
    private static AmazonS3 s3Client;

    private String bucket;
    private S3RemoteStorageManager remoteStorageManager;

    @BeforeClass
    public static void setUpClass() {
        s3Client = AmazonS3ClientBuilder.standard()
            .withCredentials(new AnonymousCredentialsProvider())
            .withEndpointConfiguration(localstack.getEndpointConfiguration(S3))
            .build();
    }

    @Before
    public void setUp() {
        logDir = TestUtils.tempDir();
        createdSegments = new ArrayList<>();

        bucket = TestUtils.randomString(10).toLowerCase(Locale.ROOT);
        s3Client.createBucket(bucket);
        remoteStorageManager = new S3RemoteStorageManager(localstack.getEndpointConfiguration(S3));
    }

    @After
    public void tearDown() throws IOException {
        remoteStorageManager.close();

        for (final LogSegment createdSegment : createdSegments) {
            createdSegment.close();
        }

        // Sometimes errors happen here on Windows machines.
        try {
            Utils.delete(logDir);
        } catch (final IOException e) {
            log.error("Error during tear down", e);
        }
    }


    @Test
    public void testCopyLogSegment() throws RemoteStorageException {
        remoteStorageManager.configure(basicProps(bucket));

        final LogSegment segment1 = createLogSegment(0);
        final LogSegment segment2 = createLogSegment(5);

        final UUID uuid1 = UUID.randomUUID();
        final RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(TP0, uuid1);
        final LogSegmentData lsd1 = new LogSegmentData(segment1.log().file(), segment1.offsetIndex().file(), segment1.timeIndex().file());
        remoteStorageManager.copyLogSegment(segmentId1, lsd1);

        final UUID uuid2 = UUID.randomUUID();
        final RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(TP1, uuid2);
        final LogSegmentData lsd2 = new LogSegmentData(segment2.log().file(), segment2.offsetIndex().file(), segment2.timeIndex().file());
        remoteStorageManager.copyLogSegment(segmentId2, lsd2);

        final List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP0, segment1.log().file(), uuid1),
            s3Key(TP0, segment1.offsetIndex().file(), uuid1),
            s3Key(TP0, segment1.timeIndex().file(), uuid1),
            s3Key(TP1, segment2.log().file(), uuid2),
            s3Key(TP1, segment2.offsetIndex().file(), uuid2),
            s3Key(TP1, segment2.timeIndex().file(), uuid2)
        ));
    }

    @Test
    public void testFetchLogSegmentDataComplete() throws RemoteStorageException, IOException {
        remoteStorageManager.configure(basicProps(bucket));

        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, UUID.randomUUID());
        final LogSegmentData lsd = new LogSegmentData(segment.log().file(), segment.offsetIndex().file(), segment.timeIndex().file());
        final RemoteLogSegmentContext context = remoteStorageManager.copyLogSegment(segmentId, lsd);

        final RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, -1, -1, -1, -1, context.asBytes());
        try (final InputStream remoteInputStream = remoteStorageManager.fetchLogSegmentData(metadata, 0L, null)) {
            assertStreamContentEqualToFile(segment.log().file(), null, null, remoteInputStream);
        }
    }

    @Test
    public void testFetchLogSegmentDataPartially() throws RemoteStorageException, IOException {
        remoteStorageManager.configure(basicProps(bucket));

        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, UUID.randomUUID());
        final LogSegmentData lsd = new LogSegmentData(segment.log().file(), segment.offsetIndex().file(), segment.timeIndex().file());
        final RemoteLogSegmentContext context = remoteStorageManager.copyLogSegment(segmentId, lsd);

        final RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, -1, -1, -1, -1, context.asBytes());

        final long skipBeginning = 23L;
        final long startPosition = skipBeginning;
        final long skipEnd = 42L;
        final long endPosition = segment.log().file().length() - skipEnd;
        try (final InputStream remoteInputStream = remoteStorageManager.fetchLogSegmentData(metadata, startPosition, endPosition)) {
            assertStreamContentEqualToFile(segment.log().file(), skipBeginning, skipEnd, remoteInputStream);
        }
    }

    @Test
    public void testFetchOffsetIndex() throws RemoteStorageException, IOException {
        remoteStorageManager.configure(basicProps(bucket));

        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, UUID.randomUUID());
        final LogSegmentData lsd = new LogSegmentData(segment.log().file(), segment.offsetIndex().file(), segment.timeIndex().file());
        final RemoteLogSegmentContext context = remoteStorageManager.copyLogSegment(segmentId, lsd);

        final RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, -1, -1, -1, -1, context.asBytes());
        try (final InputStream remoteInputStream = remoteStorageManager.fetchOffsetIndex(metadata)) {
            assertStreamContentEqualToFile(segment.offsetIndex().file(), null, null, remoteInputStream);
        }
    }

    @Test
    public void testFetchTimestampIndex() throws RemoteStorageException, IOException {
        remoteStorageManager.configure(basicProps(bucket));

        final LogSegment segment = createLogSegment(0);

        final RemoteLogSegmentId segmentId = new RemoteLogSegmentId(TP0, UUID.randomUUID());
        final LogSegmentData lsd = new LogSegmentData(segment.log().file(), segment.offsetIndex().file(), segment.timeIndex().file());
        final RemoteLogSegmentContext context = remoteStorageManager.copyLogSegment(segmentId, lsd);

        final RemoteLogSegmentMetadata metadata = new RemoteLogSegmentMetadata(segmentId, -1, -1, -1, -1, context.asBytes());
        try (final InputStream remoteInputStream = remoteStorageManager.fetchTimestampIndex(metadata)) {
            assertStreamContentEqualToFile(segment.timeIndex().file(), null, null, remoteInputStream);
        }
    }

    @Test
    public void testDeleteLogSegment() throws RemoteStorageException {
        remoteStorageManager.configure(basicProps(bucket));

        final LogSegment segment1 = createLogSegment(0);
        final LogSegment segment2 = createLogSegment(5);

        final RemoteLogSegmentId segmentId1 = new RemoteLogSegmentId(TP0, UUID.randomUUID());
        final LogSegmentData lsd1 = new LogSegmentData(segment1.log().file(), segment1.offsetIndex().file(), segment1.timeIndex().file());
        final RemoteLogSegmentContext remoteLogSegmentContext1 = remoteStorageManager.copyLogSegment(segmentId1, lsd1);

        final UUID uuid2 = UUID.randomUUID();
        final RemoteLogSegmentId segmentId2 = new RemoteLogSegmentId(TP1, uuid2);
        final LogSegmentData lsd2 = new LogSegmentData(segment2.log().file(), segment2.offsetIndex().file(), segment2.timeIndex().file());
        final RemoteLogSegmentContext remoteLogSegmentContext2 = remoteStorageManager.copyLogSegment(segmentId2, lsd2);

        final RemoteLogSegmentMetadata metadata1 = new RemoteLogSegmentMetadata(segmentId1, -1, -1, -1, -1, remoteLogSegmentContext1.asBytes());
        remoteStorageManager.deleteLogSegment(metadata1);

        final List<String> keys = listS3Keys();
        assertThat(keys, containsInAnyOrder(
            s3Key(TP1, segment2.log().file(), uuid2),
            s3Key(TP1, segment2.offsetIndex().file(), uuid2),
            s3Key(TP1, segment2.timeIndex().file(), uuid2)
        ));
    }

    private void assertStreamContentEqualToFile(final File expectedFile,
                                                final Long skipBeginning, final Long skipEnd,
                                                final InputStream actual) throws IOException {
        int length = (int) expectedFile.length();
        if (skipEnd != null) {
            length -= skipEnd;
        }

        try (final InputStream fileInputStream = new FileInputStream(expectedFile)) {
            if (skipBeginning != null) {
                final long skipped = fileInputStream.skip(skipBeginning);
                assert skipped == skipBeginning;
            }

            final ByteBuffer actualBuffer = ByteBuffer.allocate(length);
            Utils.readFully(actual, actualBuffer);
            assertEquals(-1, actual.read());

            final ByteBuffer expectedBuffer = ByteBuffer.allocate(length);
            Utils.readFully(fileInputStream, expectedBuffer);

            assertArrayEquals(expectedBuffer.array(), actualBuffer.array());
        }
    }

    private Map<String, String> basicProps(final String bucket) {
        final Map<String, String> props = new HashMap<>();
        props.put(S3RemoteStorageManagerConfig.S3_BUCKET_NAME_CONFIG, bucket);
        props.put(S3RemoteStorageManagerConfig.S3_CREDENTIALS_PROVIDER_CLASS_CONFIG,
            AnonymousCredentialsProvider.class.getName());
        return props;
    }

    private LogSegment createLogSegment(final long offset) {
        final LogSegment segment = LogUtils.createSegment(offset, logDir, 2048, Time.SYSTEM);
        createdSegments.add(segment);

        final int batchSize = 100;
        for (long i = offset; i < batchSize * 20; i += batchSize) {
            appendRecordBatch(segment, i, 1024, batchSize);
        }

        segment.onBecomeInactiveSegment();
        return segment;
    }

    private void appendRecordBatch(final LogSegment segment, final long offset, final int recordSize, final int numRecords) {
        final SimpleRecord[] records = new SimpleRecord[numRecords];
        long timestamp = 0;
        for (int i = 0; i < numRecords; i++) {
            timestamp = (offset + i) * 10000000;
            records[i] = new SimpleRecord(timestamp, new byte[recordSize]);
            records[i].value().putLong(0, offset + i);
            records[i].value().rewind();
        }
        final long lastOffset = offset + numRecords - 1;
        final MemoryRecords memoryRecords = MemoryRecords.withRecords(
            RecordBatch.CURRENT_MAGIC_VALUE, offset, CompressionType.NONE, TimestampType.CREATE_TIME, records);
        segment.append(lastOffset, timestamp, offset, memoryRecords);
    }

    private List<String> listS3Keys() {
        final List<S3ObjectSummary> objects = s3Client.listObjectsV2(bucket).getObjectSummaries();
        return objects.stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
    }

    private String s3Key(final TopicPartition topicPartition, final File file, final UUID uuid) {
        return topicPartition.toString() + "/" + file.getName() + "." + uuid;
    }

    static class AnonymousCredentialsProvider implements AWSCredentialsProvider {
        @Override
        public AWSCredentials getCredentials() {
            return new BasicAWSCredentials("foo", "bar");
        }

        @Override
        public void refresh() {

        }
    }
}
