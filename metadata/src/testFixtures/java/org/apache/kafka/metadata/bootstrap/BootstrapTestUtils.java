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

import java.nio.file.Path;

import static org.apache.kafka.common.internals.Topic.CLUSTER_METADATA_TOPIC_PARTITION;
import static org.apache.kafka.snapshot.Snapshots.BOOTSTRAP_SNAPSHOT_ID;
import static org.apache.kafka.snapshot.Snapshots.snapshotPath;

/**
 * Utilities for testing classes that deal with bootstrap metadata.
 */
public class BootstrapTestUtils {
    /**
     * Reads bootstrap metadata from the cluster metadata bootstrap checkpoint file of the given metadata directory.
     *
     * @param directoryPath the metadata log directory
     * @return the bootstrap metadata
     */
    public static BootstrapMetadata readBootstrapMetadata(String directoryPath) {
        Path metadataPartitionDir = Path.of(directoryPath,
            CLUSTER_METADATA_TOPIC_PARTITION.topic() + "-" + CLUSTER_METADATA_TOPIC_PARTITION.partition());
        Path checkpointPath = snapshotPath(metadataPartitionDir, BOOTSTRAP_SNAPSHOT_ID);
        return BootstrapMetadata.fromCheckpointFile(checkpointPath);
    }
}
