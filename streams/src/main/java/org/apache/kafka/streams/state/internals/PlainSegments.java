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

import org.apache.kafka.streams.processor.internals.InternalProcessorContext;

/**
 * Manages the {@link PlainSegment}s that are used by the {@link RocksDBSegmentedBytesStore}
 */
class PlainSegments extends AbstractSegments<PlainSegment> {

    PlainSegments(final String name, final long retentionPeriod, final long segmentInterval) {
        super(name, retentionPeriod, segmentInterval);
    }

    @Override
    PlainSegment getOrCreateSegment(final long segmentId, final InternalProcessorContext context) {
        if (segments.containsKey(segmentId)) {
            return segments.get(segmentId);
        } else {
            final PlainSegment newSegment = new PlainSegment(segmentName(segmentId), name, segmentId);
            final PlainSegment shouldBeNull = segments.put(segmentId, newSegment);

            if (shouldBeNull != null) {
                throw new IllegalStateException("PlainSegment already exists. Possible concurrent access.");
            }

            newSegment.openDB(context);
            return newSegment;
        }
    }
}
