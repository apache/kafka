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

package org.apache.kafka.metadata.bootstrap;

import java.io.FileNotFoundException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_PARTITION;

/**
 * Test-only implementation that reads bootstrap metadata from the metadata partition snapshot.
 */
public class TestBootstrapDirectory implements BootstrapDirectory {
    private static final String BINARY_BOOTSTRAP_CHECKPOINT_FILENAME = "00000000000000000000-0000000000.checkpoint";
    
    private final String directoryPath;

    /**
     * Create a test-only {@link BootstrapDirectory} that reads bootstrap metadata from the metadata
     * partition snapshot under the given directory.
     *
     * @param directoryPath the base log directory containing the {@code __cluster_metadata-0} partition
     */
    public TestBootstrapDirectory(String directoryPath) {
        this.directoryPath = Objects.requireNonNull(directoryPath);
    }

    @Override
    public BootstrapMetadata read() {
        Path path = Paths.get(directoryPath);
        if (!Files.isDirectory(path)) {
            if (Files.exists(path)) {
                throw new IllegalStateException("Path " + directoryPath + " exists, but is not " +
                        "a directory.");
            } else {
                throw new IllegalStateException("No such directory as " + directoryPath);
            }
        }
        Path binaryBootstrapPath = Paths.get(directoryPath, String.format("%s-%d",
            CLUSTER_METADATA_TOPIC_PARTITION.topic(),
            CLUSTER_METADATA_TOPIC_PARTITION.partition()),
            BINARY_BOOTSTRAP_CHECKPOINT_FILENAME);
        if (!Files.exists(binaryBootstrapPath)) {
            String binaryPath = binaryBootstrapPath.toString();
            throw new UncheckedIOException(binaryPath, new FileNotFoundException(binaryPath));
        } else {
            return BootstrapDirectory.readFromBinaryFile(binaryBootstrapPath.toString());
        }
    }
}
