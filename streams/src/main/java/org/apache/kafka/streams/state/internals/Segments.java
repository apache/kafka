/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the {@link Segment}s that are used by the {@link RocksDBSegmentedBytesStore}
 */
class Segments {
    static final long MIN_SEGMENT_INTERVAL = 60 * 1000L;

    private final ConcurrentHashMap<Long, Segment> segments = new ConcurrentHashMap<>();
    private final String name;
    private final int numSegments;
    private final long segmentInterval;
    private final SimpleDateFormat formatter;

    private long currentSegmentId = -1L;

    Segments(final String name, final long retentionPeriod, final int numSegments) {
        this.name = name;
        this.numSegments = numSegments;
        this.segmentInterval = Math.max(retentionPeriod / (numSegments - 1), MIN_SEGMENT_INTERVAL);
        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
    }

    long segmentId(long timestamp) {
        return timestamp / segmentInterval;
    }

    String segmentName(long segmentId) {
        return name + "-" + formatter.format(new Date(segmentId * segmentInterval));
    }

    Segment getSegmentForTimestamp(final long timestamp) {
        return getSegment(segmentId(timestamp));
    }

    Segment getOrCreateSegment(final long segmentId, final ProcessorContext context) {
        if (segmentId > currentSegmentId || segmentId > currentSegmentId - numSegments) {
            final long key = segmentId % numSegments;
            final Segment segment = segments.get(key);
            if (!isSegment(segment, segmentId)) {
                cleanup(segmentId);
            }
            if (!segments.containsKey(key)) {
                Segment newSegment = new Segment(segmentName(segmentId), name, segmentId);
                newSegment.openDB(context);
                segments.put(key, newSegment);
                currentSegmentId = segmentId > currentSegmentId ? segmentId : currentSegmentId;
            }
            return segments.get(key);
        } else {
            return null;
        }
    }

    void openExisting(final ProcessorContext context) {
        try {
            File dir = new File(context.stateDir(), name);
            if (dir.exists()) {
                String[] list = dir.list();
                if (list != null) {
                    long[] segmentIds = new long[list.length];
                    for (int i = 0; i < list.length; i++)
                        segmentIds[i] = segmentIdFromSegmentName(list[i]);

                    // open segments in the id order
                    Arrays.sort(segmentIds);
                    for (long segmentId : segmentIds) {
                        if (segmentId >= 0) {
                            getOrCreateSegment(segmentId, context);
                        }
                    }
                }
            } else {
                if (!dir.mkdir()) {
                    throw new ProcessorStateException(String.format("dir %s doesn't exist and cannot be created for segments %s", dir, name));
                }
            }
        } catch (Exception ex) {
            // ignore
        }
    }

    List<Segment> segments(final long timeFrom, final long timeTo) {
        final long segFrom = segmentId(Math.max(0L, timeFrom));
        final long segTo = segmentId(Math.min(currentSegmentId * segmentInterval, Math.max(0, timeTo)));

        final List<Segment> segments = new ArrayList<>();
        for (long segmentId = segFrom; segmentId <= segTo; segmentId++) {
            Segment segment = getSegment(segmentId);
            if (segment != null && segment.isOpen()) {
                try {
                    segments.add(segment);
                } catch (InvalidStateStoreException ise) {
                    // segment may have been closed by streams thread;
                }
            }
        }
        return segments;
    }

    void flush() {
        for (Segment segment : segments.values()) {
            segment.flush();
        }
    }

    public void close() {
        for (Segment segment : segments.values()) {
            segment.close();
        }
    }

    private Segment getSegment(long segmentId) {
        final Segment segment = segments.get(segmentId % numSegments);
        if (!isSegment(segment, segmentId)) {
            return null;
        }
        return segment;
    }

    private boolean isSegment(final Segment store, long segmentId) {
        return store != null && store.id == segmentId;
    }

    private void cleanup(final long segmentId) {
        final long oldestSegmentId = currentSegmentId < segmentId
                ? segmentId - numSegments
                : currentSegmentId - numSegments;

        for (Map.Entry<Long, Segment> segmentEntry : segments.entrySet()) {
            final Segment segment = segmentEntry.getValue();
            if (segment != null && segment.id <= oldestSegmentId) {
                segments.remove(segmentEntry.getKey());
                segment.close();
                segment.destroy();
            }
        }
    }

    private long segmentIdFromSegmentName(String segmentName) {
        try {
            Date date = formatter.parse(segmentName.substring(name.length() + 1));
            return date.getTime() / segmentInterval;
        } catch (Exception ex) {
            return -1L;
        }
    }
}
