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

import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SimpleTimeZone;
import java.util.TreeMap;

/**
 * Manages the {@link Segment}s that are used by the {@link RocksDBSegmentedBytesStore}
 */
class Segments {
    private static final Logger log = LoggerFactory.getLogger(Segments.class);

    private final TreeMap<Long, Segment> segments = new TreeMap<>();
    private final String name;
    private final long retentionPeriod;
    private final long segmentInterval;
    private final SimpleDateFormat formatter;

    Segments(final String name, final long retentionPeriod, final long segmentInterval) {
        this.name = name;
        this.segmentInterval = segmentInterval;
        this.retentionPeriod = retentionPeriod;
        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
    }

    long segmentId(final long timestamp) {
        return timestamp / segmentInterval;
    }

    String segmentName(final long segmentId) {
        // (1) previous format used - as a separator so if this changes in the future
        // then we should use something different.
        // (2) previous format used : as a separator (which did break KafkaStreams on Windows OS)
        // so if this changes in the future then we should use something different.
        return name + "." + segmentId * segmentInterval;
    }

    Segment getSegmentForTimestamp(final long timestamp) {
        return segments.get(segmentId(timestamp));
    }

    Segment getOrCreateSegmentIfLive(final long segmentId, final InternalProcessorContext context) {
        final long minLiveSegment = segmentId(context.streamTime() - retentionPeriod);

        final Segment toReturn;
        if (segmentId >= minLiveSegment) {
            // The segment is live. get it, ensure it's open, and return it.
            toReturn = getOrCreateSegment(segmentId, context);
        } else {
            toReturn = null;
        }

        cleanupEarlierThan(minLiveSegment);
        return toReturn;
    }

    private Segment getOrCreateSegment(final long segmentId, final InternalProcessorContext context) {
        if (segments.containsKey(segmentId)) {
            return segments.get(segmentId);
        } else {
            final Segment newSegment = new Segment(segmentName(segmentId), name, segmentId);
            final Segment shouldBeNull = segments.put(segmentId, newSegment);

            if (shouldBeNull != null) {
                throw new IllegalStateException("Segment already exists. Possible concurrent access.");
            }

            newSegment.openDB(context);
            return newSegment;
        }
    }

    void openExisting(final InternalProcessorContext context) {
        try {
            final File dir = new File(context.stateDir(), name);
            if (dir.exists()) {
                final String[] list = dir.list();
                if (list != null) {
                    final long[] segmentIds = new long[list.length];
                    for (int i = 0; i < list.length; i++)
                        segmentIds[i] = segmentIdFromSegmentName(list[i], dir);

                    // open segments in the id order
                    Arrays.sort(segmentIds);
                    for (final long segmentId : segmentIds) {
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
        } catch (final Exception ex) {
            // ignore
        }

        final long minLiveSegment = segmentId(context.streamTime() - retentionPeriod);
        cleanupEarlierThan(minLiveSegment);
    }

    List<Segment> segments(final long timeFrom, final long timeTo) {
        final List<Segment> result = new ArrayList<>();
        final NavigableMap<Long, Segment> segmentsInRange = segments.subMap(
            segmentId(timeFrom), true,
            segmentId(timeTo), true
        );
        for (final Segment segment : segmentsInRange.values()) {
            if (segment.isOpen()) {
                result.add(segment);
            }
        }
        return result;
    }

    List<Segment> allSegments() {
        final List<Segment> result = new ArrayList<>();
        for (final Segment segment : segments.values()) {
            if (segment.isOpen()) {
                result.add(segment);
            }
        }
        return result;
    }

    void flush() {
        for (final Segment segment : segments.values()) {
            segment.flush();
        }
    }

    public void close() {
        for (final Segment segment : segments.values()) {
            segment.close();
        }
        segments.clear();
    }

    private void cleanupEarlierThan(final long minLiveSegment) {
        final Iterator<Map.Entry<Long, Segment>> toRemove =
            segments.headMap(minLiveSegment, false).entrySet().iterator();

        while (toRemove.hasNext()) {
            final Map.Entry<Long, Segment> next = toRemove.next();
            toRemove.remove();
            final Segment segment = next.getValue();
            segment.close();
            try {
                segment.destroy();
            } catch (final IOException e) {
                log.error("Error destroying {}", segment, e);
            }
        }
    }

    private long segmentIdFromSegmentName(final String segmentName,
                                          final File parent) {
        final int segmentSeparatorIndex = name.length();
        final char segmentSeparator = segmentName.charAt(segmentSeparatorIndex);
        final String segmentIdString = segmentName.substring(segmentSeparatorIndex + 1);
        final long segmentId;

        // old style segment name with date
        if (segmentSeparator == '-') {
            try {
                segmentId = formatter.parse(segmentIdString).getTime() / segmentInterval;
            } catch (final ParseException e) {
                log.warn("Unable to parse segmentName {} to a date. This segment will be skipped", segmentName);
                return -1L;
            }
            renameSegmentFile(parent, segmentName, segmentId);
        } else {
            // for both new formats (with : or .) parse segment ID identically
            try {
                segmentId = Long.parseLong(segmentIdString) / segmentInterval;
            } catch (final NumberFormatException e) {
                throw new ProcessorStateException("Unable to parse segment id as long from segmentName: " + segmentName);
            }

            // intermediate segment name with : breaks KafkaStreams on Windows OS -> rename segment file to new name with .
            if (segmentSeparator == ':') {
                renameSegmentFile(parent, segmentName, segmentId);
            }
        }

        return segmentId;

    }

    private void renameSegmentFile(final File parent,
                                   final String segmentName,
                                   final long segmentId) {
        final File newName = new File(parent, segmentName(segmentId));
        final File oldName = new File(parent, segmentName);
        if (!oldName.renameTo(newName)) {
            throw new ProcessorStateException("Unable to rename old style segment from: "
                + oldName
                + " to new name: "
                + newName);
        }
    }

}
