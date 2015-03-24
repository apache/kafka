// Copyright (c) 2015 Uber Technologies, Inc. All rights reserved.
// @author Seung-Yeoul Yang (syyang@uber.com)

package com.uber.kafka.tools;

import java.io.File;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Fixes corrupt Kafka 0.7 offset using offset index file (containing a list of latest
 * offsets written out periodically by a cron job).
 */
public class Kafka7OffsetFixer {

    private static final Logger LOGGER = Logger.getLogger(Kafka7OffsetFixer.class);

    private static final long OFFSET_INDEX_MAX_AGE_MS = TimeUnit.MINUTES.toMillis(10L);

    private static final int PARTITION_0 = 0;

    private static final String LEAF_KAFKA07_ZK_HOST = "localhost:2182";

    private static final String MIRRORMAKER_PATH = "/var/mirrormaker";
    private static final String CURRENT_OFFSET_PATH = "/consumers/%s/offsets/%s/0-0";

    private final Kafka7LatestOffsets latestOffsets;
    private final ZkClient zkClient;
    private final String mirrorMakerPath;

    public Kafka7OffsetFixer(Kafka7LatestOffsets latestOffsetProvider) {
        this(latestOffsetProvider, MIRRORMAKER_PATH);
    }

    public Kafka7OffsetFixer(Kafka7LatestOffsets latestOffsets, String mirrorMakerPath) {
        this(latestOffsets, mirrorMakerPath,
            MigrationUtils.get().newZkClient(LEAF_KAFKA07_ZK_HOST));
    }

    public Kafka7OffsetFixer(Kafka7LatestOffsets latestOffsets,
                             String mirrorMakerPath, ZkClient zkClient) {
        this.latestOffsets = latestOffsets;
        this.mirrorMakerPath = mirrorMakerPath;
        this.zkClient = zkClient;
    }

    public void close() {
        try {
            latestOffsets.close();
            zkClient.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to clean-up Kafka offset fixer", e);
        }
    }

    public static String getCurrentOffsetZkPath(String consumerGroup, String topic) {
        return String.format(CURRENT_OFFSET_PATH, consumerGroup, topic);
    }

    public void fixOffset(String consumerGroup, String topic) {
        Preconditions.checkNotNull(topic, "Topic can't be null");
        // Load offset index.
        File offsetIndexFile = new File(mirrorMakerPath, topic + "_0");
        String offsetIndexPath = offsetIndexFile.getPath();
        if (!offsetIndexFile.exists()) {
            throw new RuntimeException("Offset index file doesn't exist at path: "  +
                offsetIndexPath);
        }
        // Check whether offset index is stale.
        if (offsetIndexFile.lastModified() < System.currentTimeMillis() - OFFSET_INDEX_MAX_AGE_MS) {
            throw new RuntimeException("Offset index file is stale. Path: " + offsetIndexPath +
                ", last modified time: " + new Date(offsetIndexFile.lastModified()));
        }

        LOGGER.info("Loading offset file for topic: " + topic + ", path: " + offsetIndexPath);
        OffsetIndex index = OffsetIndex.load(offsetIndexPath);

        // Get current offset from kafka leaf zookeeper.
        String currentOffsetPath = getCurrentOffsetZkPath(consumerGroup, topic);
        LOGGER.info("Reading current offset for topic: " + topic + ", zk path: " + currentOffsetPath);
        if (!zkClient.exists(currentOffsetPath)) {
            throw new RuntimeException("Missing current offset for " + topic + "at zk path: " +
                currentOffsetPath);
        }
        byte[] currentOffsetBytes = zkClient.readData(currentOffsetPath);
        long currentOffset = Long.parseLong(new String(currentOffsetBytes));
        LOGGER.info("Current offset: " + currentOffset + ", topic: " + topic);

        // Find the next valid offset.
        long newOffset = index.getNextOffset(currentOffset);
        if (newOffset == OffsetIndex.LATEST_OFFSET) {
            // Resolve latest offset to actual offset.
            newOffset = latestOffsets.get(topic, PARTITION_0);
            LOGGER.info("Resolved latest offset to " + newOffset + ", topic " + topic);
        }

        // Update the current offset in zookeeper.
        byte[] newOffsetBytes = Long.toString(newOffset).getBytes();
        zkClient.writeData(currentOffsetPath, newOffsetBytes);

        LOGGER.info(String.format("Updated offset for %s from %d to %d",
            topic, currentOffset, newOffset));
    }

}
