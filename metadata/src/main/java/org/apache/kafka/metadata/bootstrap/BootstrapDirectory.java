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

import java.io.IOException;

/**
 * Abstraction for reading controller bootstrap metadata from disk.
 */
public interface BootstrapDirectory {
    String BINARY_BOOTSTRAP_FILENAME = "bootstrap.checkpoint";

    String BINARY_BOOTSTRAP_CHECKPOINT_FILENAME = "00000000000000000000-0000000000.checkpoint";

    /**
     * Read the bootstrap metadata from the configured location.
     *
     * @return the loaded {@link BootstrapMetadata}
     * @throws Exception if the metadata cannot be read
     */
    BootstrapMetadata read() throws Exception;

    /**
     * Write bootstrap metadata to the configured location.
     *
     * @param bootstrapMetadata the metadata to write
     * @throws IOException if the metadata cannot be written
     */
    void writeBinaryFile(BootstrapMetadata bootstrapMetadata) throws IOException;
}
