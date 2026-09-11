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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.Serial;

/**
 * Thrown when MirrorMaker 2 determines that a source topic-partition has been reset -- typically
 * because the topic was deleted and recreated -- while the connector still holds a committed offset
 * from the previous incarnation of the topic.
 *
 * <p>The tell-tale signal is an out-of-range offset on a partition whose log start offset is
 * {@code 0}: the partition has been rewound to the very beginning, so the previously tracked offset
 * points past the end of the log rather than before its start.
 *
 * <p>Resuming from {@code earliest} in this situation would silently re-replicate the new topic on
 * top of the old data on the target cluster, so the task fails fast and leaves the decision to an
 * operator.
 *
 * <p>Only raised when {@link MirrorSourceConfig#OFFSET_VALIDATION_ENABLED} is set to {@code true}.
 */
public class TopicResetException extends ConnectException {

    @Serial
    private static final long serialVersionUID = 1L;

    public TopicResetException(String message) {
        super(message);
    }

    public TopicResetException(String message, Throwable cause) {
        super(message, cause);
    }
}
