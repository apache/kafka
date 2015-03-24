// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.uber.kafka.tools.OffsetIndexTestUtil.Offset;

/**
 * Tests for {@link com.uber.kafka.tools.Kafka7OffsetFixer}
 */
public class Kafka7OffsetFixerTest {

    private static final String TEST_CONSUMER_GROUP = "test_fixer_consumer";
    private static final String TEST_TOPIC = "test_fixer_topic";

    private Kafka7LatestOffsets latestOffsets;
    private ZkClient zkClient;
    private File mirrorkMakerDir;
    private String mirrorMakerPath;
    private String currentOffsetZkPath;
    private List<Offset> offsets;
    private byte[] offsetsBytes;

    @Before
    public void setUp() throws Exception {
        latestOffsets = mock(Kafka7LatestOffsets.class);
        zkClient = mock(ZkClient.class);
        mirrorkMakerDir = Files.createTempDir();
        mirrorMakerPath = mirrorkMakerDir.getPath();
        currentOffsetZkPath = Kafka7OffsetFixer.getCurrentOffsetZkPath(
            TEST_CONSUMER_GROUP, TEST_TOPIC);

        // Set up offset index file.
        offsets = ImmutableList.of(
            new Offset(3, 30L),
            new Offset(4, 40L),
            new Offset(5, 50L),
            new Offset(1, 10L),
            new Offset(2, 20L)
        );
        offsetsBytes = OffsetIndexTestUtil.toByteArray(offsets);
        File offsetIndexFile = new File(mirrorkMakerDir, TEST_TOPIC + "_0");
        Files.write(offsetsBytes, offsetIndexFile);
    }

    @After
    public void tearDown() throws IOException {
        // Clean up offset index file.
        FileUtils.deleteDirectory(mirrorkMakerDir);
    }

    @Test
    public void testBasic() {
        final long CURRENT_OFFSET = 25L;

        // Mock zkClient
        when(zkClient.exists(currentOffsetZkPath)).thenReturn(true);
        byte[] currentOffsetBytes = Long.toString(CURRENT_OFFSET).getBytes();
        when(zkClient.readData(currentOffsetZkPath)).thenReturn(currentOffsetBytes);

        // Run fixer
        Kafka7OffsetFixer fixer = new Kafka7OffsetFixer(
            latestOffsets, mirrorMakerPath, zkClient);
        fixer.fixOffset(TEST_CONSUMER_GROUP, TEST_TOPIC);

        // Verify that the new offset should be 30 since it's the next
        // biggest offset after the current offset 25 in the offset index.
        byte[] newOffset = Long.toString(30L).getBytes();
        verify(zkClient).writeData(currentOffsetZkPath, newOffset);
    }

    @Test
    public void testFixWithLatestOffset() {
        final long LATEST_OFFSET = 60L;
        final long CURRENT_OFFSET = 55L;

        // Mock zkClient
        when(zkClient.exists(currentOffsetZkPath)).thenReturn(true);
        byte[] currentOffsetBytes = Long.toString(CURRENT_OFFSET).getBytes();
        when(zkClient.readData(currentOffsetZkPath)).thenReturn(currentOffsetBytes);

        // Mock Kafka7LatestOffsets
        when(latestOffsets.get(TEST_TOPIC, 0)).thenReturn(LATEST_OFFSET);

        // Run fixer
        Kafka7OffsetFixer fixer = new Kafka7OffsetFixer(
            latestOffsets, mirrorMakerPath, zkClient);
        fixer.fixOffset(TEST_CONSUMER_GROUP, TEST_TOPIC);

        // Verify that the new offset is latest offset since the current
        // offset 55 is bigger than any other offsets in the offset index.
        byte[] newOffset = Long.toString(LATEST_OFFSET).getBytes();
        verify(zkClient).writeData(currentOffsetZkPath, newOffset);
    }

}
