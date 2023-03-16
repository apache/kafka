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

package org.apache.kafka.image;

import org.apache.kafka.common.metadata.ZkMigrationStateRecord;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.migration.ZkMigrationState;

import java.util.Objects;

public class ZkMigrationImage {
    public static final ZkMigrationImage EMPTY = new ZkMigrationImage(ZkMigrationState.UNINITIALIZED);

    private final ZkMigrationState state;

    ZkMigrationImage(ZkMigrationState state) {
        this.state = state;
    }

    public ZkMigrationState migrationState() {
        return state;
    }

    public boolean isEmpty() {
        return this.equals(ZkMigrationImage.EMPTY);
    }

    public void write(ImageWriter writer, ImageWriterOptions options) {
        if (options.metadataVersion().isMigrationSupported()) {
            writer.write(0, new ZkMigrationStateRecord().setZkMigrationState(state.value()));
        } else {
            options.handleLoss("the ZK Migration state");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ZkMigrationImage that = (ZkMigrationImage) o;
        return state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(state);
    }

    @Override
    public String toString() {
        return "ZkMigrationImage{" +
                "state=" + state +
                '}';
    }
}
