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

/**
 * A RocksDB-backed segmented bytes store with headers support.
 * <p>
 * This store uses {@link SessionSegmentsWithHeaders} to manage segments,
 * where each segment is a {@link SessionSegmentWithHeaders} that extends
 * {@link RocksDBMigratingSessionStoreWithHeaders}. This provides automatic dual-CF
 * migration support from plain key-value format to headers format.
 */
public class SessionRocksDBSegmentedBytesStoreWithHeaders extends AbstractRocksDBSegmentedBytesStore<SessionSegmentWithHeaders> {

    SessionRocksDBSegmentedBytesStoreWithHeaders(final String name,
                                                 final String metricsScope,
                                                 final long retention,
                                                 final long segmentInterval,
                                                 final KeySchema keySchema) {
        super(name, retention, keySchema, new SessionSegmentsWithHeaders(name, metricsScope, retention, segmentInterval));
    }
}
