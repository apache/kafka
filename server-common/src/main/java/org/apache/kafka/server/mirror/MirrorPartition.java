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
package org.apache.kafka.server.mirror;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidMirrorException;
import org.apache.kafka.common.utils.Utils;

import java.util.Objects;

/**
 * Uniquely identify a mirror partition by mirrorName, topicId, partition.
 *
 * <p>Mirror names must be non-empty, at most 249 characters, contain only ASCII
 * alphanumerics, '.', '_' and '-', and cannot be "." or "..".
 *
 * @param mirrorName the mirror name
 * @param topicId the topic ID
 * @param partition the partition index
 */
public record MirrorPartition(String mirrorName, Uuid topicId, int partition) {
    private static final int MAX_NAME_LENGTH = 249;

    public MirrorPartition(String mirrorName, Uuid topicId, int partition) {
        this.mirrorName = Objects.requireNonNull(mirrorName, "Mirror name cannot be null");
        validateMirrorName(mirrorName);
        this.topicId = Objects.requireNonNull(topicId, "topicId cannot be null");
        if (partition < 0) {
            throw new IllegalArgumentException("Partition cannot be negative: " + partition);
        }
        this.partition = partition;
    }

    private static void validateMirrorName(String name) {
        if (name.isEmpty()) {
            throw new InvalidMirrorException("Mirror name is invalid: the empty string is not allowed");
        }
        if (".".equals(name)) {
            throw new InvalidMirrorException("Mirror name is invalid: '.' is not allowed");
        }
        if ("..".equals(name)) {
            throw new InvalidMirrorException("Mirror name is invalid: '..' is not allowed");
        }
        if (name.length() > MAX_NAME_LENGTH) {
            throw new InvalidMirrorException("Mirror name is invalid: the length of '" + name +
                    "' is longer than the max allowed length " + MAX_NAME_LENGTH);
        }
        if (!containsValidPattern(name)) {
            throw new InvalidMirrorException("Mirror name is invalid: '" + name +
                    "' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'");
        }
    }

    private static boolean containsValidPattern(String name) {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean isAlphanumeric = (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z');
            if (!isAlphanumeric && c != '.' && c != '_' && c != '-') return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("%s-%s-%d", mirrorName, topicId, partition);
    }

    /**
     * Returns a stable key for coordinator-partition hashing, decoupled from {@link #toString()}
     * so the display format can evolve without remapping partitions.
     */
    public String asCoordinatorKey() {
        return String.format("%s:%s:%d", mirrorName, topicId, partition);
    }

    /**
     * Returns the {@code __mirror_state} partition index this mirror partition maps to.
     * The mapping is deterministic: the same mirror partition always resolves to the same
     * coordinating partition for a given {@code partitionCount}.
     *
     * @param partitionCount the total number of partitions in the {@code __mirror_state} topic
     * @return a partition index in the range {@code [0, partitionCount)}
     */
    public int coordinatingPartition(int partitionCount) {
        return Utils.abs(asCoordinatorKey().hashCode()) % partitionCount;
    }
    
    /**
     * Returns a {@link MirrorPartition} parsed from a string of format {@code mirrorName:topicId:partition}.
     * Since ':' is not a valid mirror name character, the key always splits into exactly three tokens.
     *
     * @param key the string to parse
     * @return the parsed {@link MirrorPartition}
     * @throws IllegalArgumentException if the key is empty, has invalid format, topic ID or partition
     * @throws InvalidMirrorException if the mirror name is invalid
     */
    public static MirrorPartition parse(String key) {
        Objects.requireNonNull(key, "Key cannot be null");
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Mirror key cannot be empty");
        }

        String[] tokens = key.split(":");
        if (tokens.length != 3) {
            throw new IllegalArgumentException("Invalid key format: expected mirrorName:topicId:partition, found " + key);
        }

        Uuid topicId;
        try {
            topicId = Uuid.fromString(tokens[1]);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid topic ID: " + tokens[1], e);
        }

        int partition;
        try {
            partition = Integer.parseInt(tokens[2]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid partition: " + tokens[2], e);
        }

        return new MirrorPartition(tokens[0], topicId, partition);
    }
}
