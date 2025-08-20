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
        String line = null;
        Uuid metadataTopicId;

        try {
            line = reader.readLine();
            String[] versionArr = WHITE_SPACES_PATTERN.split(line);

            if (versionArr.length == 2) {
                int version = Integer.parseInt(versionArr[1]);
                // To ensure downgrade compatibility, check if version is at least 0
                if (version >= PartitionMetadataFile.CURRENT_VERSION) {
                    line = reader.readLine();
                    String[] topicIdArr = WHITE_SPACES_PATTERN.split(line);

                    if (topicIdArr.length == 2) {
                        metadataTopicId = Uuid.fromString(topicIdArr[1]);

                        if (metadataTopicId.equals(Uuid.ZERO_UUID)) {
                            throw new IOException("Invalid topic ID in partition metadata file (" + location + ")");
                        }

                        return new PartitionMetadata(version, metadataTopicId);
                    } else {
                        throw malformedLineException(line);
                    }
                } else {
                    throw new IOException("Unrecognized version of partition metadata file + (" + location + "): " + version);
                }
            } else {
                throw malformedLineException(line);
            }

        } catch (NumberFormatException e) {
            throw malformedLineException(line, e);
        }
    }

    private IOException malformedLineException(String line) {
        return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line));
    }

    private IOException malformedLineException(String line, Exception e) {
        return new IOException(String.format("Malformed line in checkpoint file [%s]: %s", location, line), e);
    }
}
