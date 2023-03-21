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

package org.apache.kafka.controller;

import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.MetadataVersion;

import java.util.function.Consumer;

public interface ZkMigrationBootstrap {
    /**
     * Initialize the metadata log with the correct ZkMigrationStateRecord. If the log is in an invalid
     * state, this method may throw an exception.
     *
     * @param metadataVersion       The current MetadataVersion of the log
     * @param isMetadataLogEmpty    True if the log is being initialized from empty
     * @param recordConsumer        A consumer for the ZkMigrationStateRecord
     */
    void bootstrapInitialMigrationState(
        MetadataVersion metadataVersion,
        boolean isMetadataLogEmpty,
        Consumer<ApiMessageAndVersion> recordConsumer
    );
}
