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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.state.internals.metrics.RocksDBMetricsRecorder;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

class KeyValueSegment extends RocksDBStore implements Comparable<KeyValueSegment>, Segment {
    public final long id;

    KeyValueSegment(final String segmentName,
                    final String windowName,
                    final long id,
                    final RocksDBMetricsRecorder metricsRecorder) {
        super(segmentName, windowName, metricsRecorder);
        this.id = id;
    }

    @Override
    public void destroy() throws IOException {
        Utils.delete(dbDir);
    }

    @Override
    public synchronized void deleteRange(final Bytes keyFrom, final Bytes keyTo) {
        super.deleteRange(keyFrom, keyTo);
    }

    @Override
    public int compareTo(final KeyValueSegment segment) {
        return Long.compare(id, segment.id);
    }

    @Override
    public void openDB(final Map<String, Object> configs, final File stateDir) {
        super.openDB(configs, stateDir);
        // skip the registering step
    }

    @Override
    public String toString() {
        return "KeyValueSegment(id=" + id + ", name=" + name() + ")";
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final KeyValueSegment segment = (KeyValueSegment) obj;
        return id == segment.id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
