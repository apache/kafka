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
package org.apache.kafka.streams.processor;

/**
 * Controls what notion of time is used for punctuation scheduled via {@link ProcessorContext#schedule(long, PunctuationType, Punctuator)} schedule}:
 * <ul>
 *   <li>STREAM_TIME - uses "stream time", which is advanced by the processing of messages
 *   in accordance with the timestamp as extracted by the {@link TimestampExtractor} in use.
 *   <b>NOTE:</b> Only advanced if messages arrive</li>
 *   <li>WALL_CLOCK_TIME - uses system time (the wall-clock time),
 *   which is advanced at the polling interval ({@link org.apache.kafka.streams.StreamsConfig#POLL_MS_CONFIG})
 *   independent of whether new messages arrive. <b>NOTE:</b> This is best effort only as its granularity is limited
 *   by how long an iteration of the processing loop takes to complete</li>
 * </ul>
 */
public enum PunctuationType {
   STREAM_TIME,
   WALL_CLOCK_TIME,
}
