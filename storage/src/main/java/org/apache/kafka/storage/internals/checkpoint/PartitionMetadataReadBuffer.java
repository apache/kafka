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

package org.apache.kafka.storage.internals.checkpoint;

import org.apache.kafka.common.Uuid;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class PartitionMetadataReadBuffer {
    private static final Pattern WHITE_SPACES_PATTERN = Pattern.compile(":\\s+");
    private static final String VERSION_KEY = "version";
    private static final String TOPIC_ID_KEY = "topic_id";

    private final String location;
    private final BufferedReader reader;

    public PartitionMetadataReadBuffer(
        String location,
        BufferedReader reader
    ) {
        this.location = location;
        this.reader = reader;
    }

    PartitionMetadata read() throws IOException {
        String line = reader.readLine();
        String[] versionArr = parseLine(line, VERSION_KEY);
        int version = parseVersion(line, versionArr[1]);

        // To ensure downgrade compatibility, check if version is at least 0
        if (version < PartitionMetadataFile.CURRENT_VERSION) {
            throw new IOException("Unrecognized version of partition metadata file (" + location + "): " + version);
        }

        line = reader.readLine();
        String[] topicIdArr = parseLine(line, TOPIC_ID_KEY);
        Uuid metadataTopicId = parseTopicId(line, topicIdArr[1]);

        if (metadataTopicId.equals(Uuid.ZERO_UUID)) {
            throw new IOException("Invalid topic ID in partition metadata file (" + location + ")");
        }

        return new PartitionMetadata(version, metadataTopicId);
    }

    private String[] parseLine(String line, String expectedKey) throws IOException {
        if (line == null) {
            throw malformedLineException(null);
        }

        String[] parts = WHITE_SPACES_PATTERN.split(line, 2);
        if (parts.length != 2 || !expectedKey.equals(parts[0])) {
            throw malformedLineException(line);
        }
        return parts;
    }

    private int parseVersion(String line, String value) throws IOException {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw malformedLineException(line, e);
        }
    }

    private Uuid parseTopicId(String line, String value) throws IOException {
        try {
            return Uuid.fromString(value);
        } catch (IllegalArgumentException e) {
            throw malformedLineException(line, e);
        }
    }

    private IOException malformedLineException(String line) {
        return new IOException(String.format("Malformed line in partition metadata file [%s]: %s", location, line));
    }

    private IOException malformedLineException(String line, Exception e) {
        return new IOException(String.format("Malformed line in partition metadata file [%s]: %s", location, line), e);
    }
}
