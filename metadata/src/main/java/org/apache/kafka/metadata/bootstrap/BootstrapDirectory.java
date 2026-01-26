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

import org.apache.kafka.metadata.util.BatchFileReader;
import org.apache.kafka.metadata.util.BatchFileReader.BatchAndType;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Abstraction for reading controller bootstrap metadata from disk.
 */
public interface BootstrapDirectory {

    /**
     * Read the bootstrap metadata from the configured location.
     * Implementations may read from a binary checkpoint file on disk, or fall back to
     * configuration defaults if no checkpoint is present.
     *
     * @return the loaded {@link BootstrapMetadata}
     * @throws UncheckedIOException if the metadata cannot be read from disk
     * @throws IllegalStateException if the configured location is missing or not a directory
     * @throws RuntimeException if the metadata is invalid
     */
    BootstrapMetadata read();

    /**
     * Read bootstrap metadata from the given binary file path.
     *
     * This is a shared helper used by {@link BootstrapDirectory} implementations; it is not
     * intended as part of the public instance contract of {@link #read()}.
     *
     * @param binaryPath the path to the binary bootstrap file
     * @return the loaded {@link BootstrapMetadata}
     * @throws UncheckedIOException if the metadata cannot be read from disk
     * @throws RuntimeException if the binary file contents are invalid
     */
    static BootstrapMetadata readFromBinaryFile(String binaryPath) {
        List<ApiMessageAndVersion> records = new ArrayList<>();
        try (BatchFileReader reader = new BatchFileReader.Builder().
                setPath(binaryPath).build()) {
            while (reader.hasNext()) {
                BatchAndType batchAndType = reader.next();
                if (!batchAndType.isControl()) {
                    records.addAll(batchAndType.batch().records());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to read bootstrap metadata from " + binaryPath, e);
        }
        return BootstrapMetadata.fromRecords(Collections.unmodifiableList(records),
                "the binary bootstrap metadata file: " + binaryPath);
    }
}
