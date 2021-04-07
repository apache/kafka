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

import java.io.IOException;
import java.util.List;
import org.apache.kafka.metadata.ApiMessageAndVersion;


/**
 * The no-op snapshot writer which does nothing.
 *
 * TODO: This will be moved to the test/ directory once we have the KRaft
 * implementation of the snapshot writer.
 */
public final class NoOpSnapshotWriter implements SnapshotWriter {
    private final long epoch;

    public NoOpSnapshotWriter(long epoch) {
        this.epoch = epoch;
    }

    @Override
    public long epoch() {
        return epoch;
    }

    @Override
    public boolean writeBatch(List<ApiMessageAndVersion> batch) throws IOException {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public void completeSnapshot() throws IOException {
    }
}
