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

/**
 * Thrown by {@link MirrorSourceTask} when it detects that a source topic has
 * been deleted and recreated (i.e. its log end offset has been reset to 0),
 * making the previously tracked offset invalid. This "fail-fast" signal makes
 * the topic reset explicit and stops the connector task immediately, preventing
 * MirrorMaker 2 from silently resuming replication from the earliest offset and
 * producing an inconsistent copy of the topic.
 */
public class TopicResetException extends ConnectException {

    private static final long serialVersionUID = 1L;

    public TopicResetException(String message) {
        super(message);
    }

    public TopicResetException(String message, Throwable cause) {
        super(message, cause);
    }
}

