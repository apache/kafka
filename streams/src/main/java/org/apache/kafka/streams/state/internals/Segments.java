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

import java.util.List;

interface Segments<S extends Segment> {

    long segmentId(final long timestamp);

    String segmentName(final long segmentId);

    S getSegmentForTimestamp(final long timestamp);

    S getOrCreateSegmentIfLive(final long segmentId, final InternalProcessorContext context, final long streamTime);

    S getOrCreateSegment(final long segmentId, final InternalProcessorContext context);

    void openExisting(final InternalProcessorContext context, final long streamTime);

    List<S> segments(final long timeFrom, final long timeTo);

    List<S> allSegments();

    void flush();

    void close();
}