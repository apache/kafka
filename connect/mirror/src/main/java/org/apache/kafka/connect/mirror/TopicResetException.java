/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
 * Thrown when a source partition's earliest available offset has reset back
 * to zero, indicating the source topic was deleted and recreated rather than
 * simply trimmed by retention. The task stops instead of silently reseeding
 * from offset zero, which could re-copy already-replicated records or
 * desynchronize the primary and DR clusters.
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
