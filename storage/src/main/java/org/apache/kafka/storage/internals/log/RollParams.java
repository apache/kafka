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
package org.apache.kafka.storage.internals.log;

/**
 * A class used to hold params required to decide to rotate a log segment or not.
 */
public class RollParams {

    public final long maxSegmentMs;
    public final int maxSegmentBytes;
    public final long maxTimestampInMessages;
    public final long maxOffsetInMessages;
    public final int messagesSize;
    public final long now;

    public RollParams(long maxSegmentMs,
               int maxSegmentBytes,
               long maxTimestampInMessages,
               long maxOffsetInMessages,
               int messagesSize,
               long now) {

        this.maxSegmentMs = maxSegmentMs;
        this.maxSegmentBytes = maxSegmentBytes;
        this.maxTimestampInMessages = maxTimestampInMessages;
        this.maxOffsetInMessages = maxOffsetInMessages;
        this.messagesSize = messagesSize;
        this.now = now;
    }

    @Override
    public String toString() {
        return "RollParams(" +
                "maxSegmentMs=" + maxSegmentMs +
                ", maxSegmentBytes=" + maxSegmentBytes +
                ", maxTimestampInMessages=" + maxTimestampInMessages +
                ", maxOffsetInMessages=" + maxOffsetInMessages +
                ", messagesSize=" + messagesSize +
                ", now=" + now +
                ')';
    }

}
