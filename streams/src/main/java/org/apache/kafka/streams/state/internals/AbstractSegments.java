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
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.query.Position;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SimpleTimeZone;
import java.util.TreeMap;

abstract class AbstractSegments<S extends Segment> implements Segments<S> {
    private static final Logger log = LoggerFactory.getLogger(AbstractSegments.class);

    final TreeMap<Long, S> segments = new TreeMap<>();
    final String name;
    private final long retentionPeriod;
    private final long segmentInterval;
    private final SimpleDateFormat formatter;
    Position position;

    AbstractSegments(final String name, final long retentionPeriod, final long segmentInterval) {
        this.name = name;
        this.segmentInterval = segmentInterval;
        this.retentionPeriod = retentionPeriod;
        // Create a date formatter. Formatted timestamps are used as segment name suffixes
        this.formatter = new SimpleDateFormat("yyyyMMddHHmm");
        this.formatter.setTimeZone(new SimpleTimeZone(0, "UTC"));
    }

    public void setPosition(final Position position) {
        this.position = position;
    }

    @Override
    public long segmentId(final long timestamp) {
        return timestamp / segmentInterval;
    }

    @Override
    public String segmentName(final long segmentId) {
        // (1) previous format used - as a separator so if this changes in the future
        // then we should use something different.
        // (2) previous format used : as a separator (which did break KafkaStreams on Windows OS)
        // so if this changes in the future then we should use something different.
        return name + "." + segmentId * segmentInterval;
    }

    @Override
    public S segmentForTimestamp(final long timestamp) {
        return segments.get(segmentId(timestamp));
    }

    @Override
    public S getOrCreateSegmentIfLive(final long segmentId,
                                      final ProcessorContext context,
                                      final long streamTime) {
        final long minLiveTimestamp = streamTime - retentionPeriod;
        final long minLiveSegment = segmentId(minLiveTimestamp);

        if (segmentId >= minLiveSegment) {
            // The segment is live. get it, ensure it's open, and return it.
            return getOrCreateSegment(segmentId, context);
        } else {
            return null;
        }
    }

    @Override
    public void openExisting(final ProcessorContext context, final long streamTime) {
        try {
            final File dir = new File(context.stateDir(), name);
            if (dir.exists()) {
                final String[] list = dir.list();
                if (list != null) {
                    Arrays.stream(list)
                            .map(segment -> segmentIdFromSegmentName(segment, dir))
                            .sorted() // open segments in the id order
                            .filter(segmentId -> segmentId >= 0)
                            .forEach(segmentId -> getOrCreateSegment(segmentId, context));
                }
            } else {
                if (!dir.mkdir()) {
                    throw new ProcessorStateException(String.format("dir %s doesn't exist and cannot be created for segments %s", dir, name));
                }
            }
        } catch (final Exception ex) {
            // ignore
        }

        cleanupExpiredSegments(streamTime);
    }

    @Override
    public List<S> segments(final long timeFrom, final long timeTo, final boolean forward) {
        final List<S> result = new ArrayList<>();
        final NavigableMap<Long, S> segmentsInRange;
        if (forward) {
            segmentsInRange = segments.subMap(
                segmentId(timeFrom), true,
                segmentId(timeTo), true
            );
        } else {
            segmentsInRange = segments.subMap(
                segmentId(timeFrom), true,
                segmentId(timeTo), true
            ).descendingMap();
        }
        for (final S segment : segmentsInRange.values()) {
            if (segment.isOpen()) {
                result.add(segment);
            }
        }
        return result;
    }

    @Override
    public List<S> allSegments(final boolean forward) {
        final List<S> result = new ArrayList<>();
        final Collection<S> values;
        if (forward) {
            values = segments.values();
        } else {
            values = segments.descendingMap().values();
        }
        for (final S segment : values) {
            if (segment.isOpen()) {
                result.add(segment);
            }
        }
        return result;
    }

    @Override
    public void flush() {
        for (final S segment : segments.values()) {
            segment.flush();
        }
    }

    @Override
    public void close() {
        for (final S segment : segments.values()) {
            segment.close();
        }
        segments.clear();
    }

    protected void cleanupExpiredSegments(final long streamTime) {
        final long minLiveSegment = segmentId(streamTime - retentionPeriod);
        final Iterator<Map.Entry<Long, S>> toRemove =
            segments.headMap(minLiveSegment, false).entrySet().iterator();

        while (toRemove.hasNext()) {
            final Map.Entry<Long, S> next = toRemove.next();
            toRemove.remove();
            final S segment = next.getValue();
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
                throw new ProcessorStateException("Unable to parse segment id as long from segmentName: " + segmentName, e);
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
